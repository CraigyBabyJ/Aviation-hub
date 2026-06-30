from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from typing import Any

import requests

from util import utc_now_iso

LOGGER = logging.getLogger("aviation_hub.ivao_bookings")
FEED_NAME = "ivao_atc_bookings"

TOKEN_FILE   = os.environ.get(
    "IVAO_TOKEN_FILE",
    os.path.join(os.path.dirname(__file__), "../../../secrets/ivao_tokens.json"),
)
TOKEN_URL    = "https://api.ivao.aero/v2/oauth/token"
BOOKINGS_URL = "https://api.ivao.aero/v2/atc/bookings/daily"
CLIENT_ID    = "c69d816a-4157-4fdc-92bc-3eefed92937f"

IVAO_RATING: dict[int, str] = {
    1: "AS1", 2: "AS2", 3: "AS3", 4: "ADC",
    5: "APC", 6: "ACC", 7: "SEC", 8: "SAI", 9: "CAI",
}

_POSITION_SUFFIXES = {
    "_DEL": "DEL", "_GND": "GND", "_TWR": "TWR",
    "_APP": "APP", "_DEP": "DEP", "_CTR": "CTR",
    "_FSS": "FSS", "_ATIS": "ATIS",
}


def _derive_position_type(callsign: str) -> str | None:
    cs = callsign.upper()
    for suffix, pos in _POSITION_SUFFIXES.items():
        if suffix in cs:
            return pos
    return None


def _read_tokens() -> dict[str, Any]:
    try:
        with open(TOKEN_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _write_tokens(data: dict[str, Any]) -> None:
    try:
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.chmod(TOKEN_FILE, 0o600)
    except Exception as exc:
        LOGGER.warning("Failed to write IVAO token file: %s", exc)


def _get_access_token(session: requests.Session) -> str:
    store = _read_tokens()
    expires_at = float(store.get("expires_at", 0))
    # Use cached access token if valid with 60s buffer
    if store.get("access_token") and expires_at > time.time() * 1000 + 60_000:
        return store["access_token"]

    refresh_token = store.get("refresh_token", "")
    if not refresh_token:
        raise RuntimeError("No IVAO refresh_token in token file — re-authenticate via the web UI")

    resp = session.post(
        TOKEN_URL,
        data={
            "grant_type":    "refresh_token",
            "refresh_token": refresh_token,
            "client_id":     CLIENT_ID,
        },
        timeout=(10, 20),
    )
    resp.raise_for_status()
    data = resp.json()

    new_store = {
        "access_token":  data["access_token"],
        "refresh_token": data.get("refresh_token", refresh_token),
        "expires_at":    time.time() * 1000 + data.get("expires_in", 3600) * 1000,
    }
    _write_tokens(new_store)
    LOGGER.info("%s: access token refreshed", FEED_NAME)
    return new_store["access_token"]


def process_ivao_atc_bookings(conn: sqlite3.Connection, session: requests.Session) -> None:
    fetched_at = utc_now_iso()

    try:
        token = _get_access_token(session)
    except Exception as exc:
        LOGGER.error("%s: cannot get access token: %s", FEED_NAME, exc)
        return

    headers = {"Authorization": f"Bearer {token}"}
    today = datetime.now(tz=timezone.utc)
    dates = [(today.year, today.month, today.day + i) for i in range(3)]
    date_strs = []
    for y, mo, d in dates:
        try:
            date_strs.append(datetime(y, mo, d).strftime("%Y-%m-%d"))
        except ValueError:
            pass

    raw_bookings: list[dict[str, Any]] = []
    for date_str in date_strs:
        try:
            r = session.get(
                BOOKINGS_URL,
                params={"date": date_str},
                headers=headers,
                timeout=(10, 30),
            )
            r.raise_for_status()
            items = r.json()
            if isinstance(items, list):
                raw_bookings.extend(items)
        except Exception as exc:
            LOGGER.warning("%s: failed to fetch %s: %s", FEED_NAME, date_str, exc)

    # Deduplicate by id
    seen: set[int] = set()
    rows: list[tuple[Any, ...]] = []
    for b in raw_bookings:
        bid = b.get("id")
        if bid is None or bid in seen:
            continue
        seen.add(bid)

        callsign = b.get("atcPosition") or ""
        ref = b.get("atcPositionRef") or {}
        airport = ref.get("airportId") or (callsign.split("_")[0] if callsign else "")
        freq     = ref.get("frequency") or 0
        user     = b.get("user") or {}
        name     = f"{user.get('firstName', '')} {user.get('lastName', '')}".strip() or None
        vid      = user.get("id")
        rating_id = (user.get("rating") or {}).get("atcRatingId") or 0
        rating   = IVAO_RATING.get(rating_id, "—")
        division = user.get("divisionId") or ""
        starts   = b.get("startDate") or ""
        ends     = b.get("endDate") or ""
        training = b.get("training")
        pos_type = _derive_position_type(callsign)

        rows.append((
            bid, callsign, airport or None, pos_type,
            name, vid, rating, division, freq,
            starts, ends, training, fetched_at,
            json.dumps(b, separators=(",", ":")),
        ))

    insert_sql = """
        INSERT INTO ivao_atc_bookings_latest (
            booking_id, callsign, airport_icao, position_type,
            controller_name, controller_vid, rating, division, frequency,
            starts_at_utc, ends_at_utc, training, fetched_at_utc, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(booking_id) DO UPDATE SET
            callsign        = excluded.callsign,
            airport_icao    = excluded.airport_icao,
            position_type   = excluded.position_type,
            controller_name = excluded.controller_name,
            controller_vid  = excluded.controller_vid,
            rating          = excluded.rating,
            division        = excluded.division,
            frequency       = excluded.frequency,
            starts_at_utc   = excluded.starts_at_utc,
            ends_at_utc     = excluded.ends_at_utc,
            training        = excluded.training,
            fetched_at_utc  = excluded.fetched_at_utc,
            raw_json        = excluded.raw_json
    """

    with conn:
        conn.execute("DELETE FROM ivao_atc_bookings_latest")
        if rows:
            conn.executemany(insert_sql, rows)

    LOGGER.info("%s: synced %d bookings (snapshot replace)", FEED_NAME, len(rows))
