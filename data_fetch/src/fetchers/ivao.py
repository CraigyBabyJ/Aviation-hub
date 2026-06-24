from __future__ import annotations

import logging
import sqlite3

import requests

from db import update_feed_state
from util import (
    normalize_iso_utc,
    to_float,
    to_int,
    utc_now_iso,
    with_retries,
)

LOGGER = logging.getLogger("aviation_hub.ivao")
# IVAO public "whazzup" v2 snapshot of everyone online (pilots + ATC).
IVAO_URL = "https://api.ivao.aero/v2/tracker/whazzup"
FEED_NAME = "ivao_network"


def _fetch_payload(session: requests.Session) -> dict:
    LOGGER.info("%s fetching network snapshot", FEED_NAME)

    def _request() -> dict:
        response = session.get(IVAO_URL, timeout=(10, 30))
        response.raise_for_status()
        return response.json()

    return with_retries(_request, context=FEED_NAME)


def _normalize_frequency(value: object) -> str | None:
    """IVAO sends frequency as a float MHz (e.g. 118.9). Store as text, drop placeholders."""
    if value in (None, "", 0, "0", "0.000"):
        return None
    return str(value)


def process_ivao_network(conn: sqlite3.Connection, session: requests.Session) -> int:
    """
    Pull the IVAO whazzup snapshot, replace the ivao_atc_latest table with the
    currently-online ATC positions, and record feed state. Returns the count of
    ATC positions stored. Mirrors the full-snapshot approach used for VATSIM.
    """
    update_timestamp = utc_now_iso()
    try:
        payload = _fetch_payload(session)
    except Exception as exc:  # noqa: BLE001
        update_feed_state(
            conn,
            feed_name=FEED_NAME,
            last_fetch=update_timestamp,
            last_error=str(exc),
            last_error_at=update_timestamp,
        )
        raise

    atcs = ((payload or {}).get("clients") or {}).get("atcs") or []
    seen_callsigns: list[str] = []

    for client in atcs:
        if not isinstance(client, dict):
            continue
        callsign = (client.get("callsign") or "").strip().upper()
        if not callsign:
            continue

        atc_session = client.get("atcSession") or {}
        last_track = client.get("lastTrack") or {}
        atis = client.get("atis") or {}

        conn.execute(
            """
            INSERT INTO ivao_atc_latest (
                callsign, user_id, rating, frequency, position,
                latitude, longitude, atis_revision, logon_time, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(callsign)
            DO UPDATE SET
                user_id = excluded.user_id,
                rating = excluded.rating,
                frequency = excluded.frequency,
                position = excluded.position,
                latitude = excluded.latitude,
                longitude = excluded.longitude,
                atis_revision = excluded.atis_revision,
                logon_time = excluded.logon_time,
                last_updated = excluded.last_updated
            """,
            (
                callsign,
                to_int(client.get("userId")),
                to_int(client.get("rating")),
                _normalize_frequency(atc_session.get("frequency")),
                atc_session.get("position"),
                to_float(last_track.get("latitude")),
                to_float(last_track.get("longitude")),
                atis.get("revision"),
                normalize_iso_utc(client.get("createdAt")),
                update_timestamp,
            ),
        )
        seen_callsigns.append(callsign)

    # Drop positions that are no longer online so the table is a true snapshot.
    if seen_callsigns:
        placeholders = ",".join("?" for _ in seen_callsigns)
        conn.execute(
            f"DELETE FROM ivao_atc_latest WHERE callsign NOT IN ({placeholders})",
            seen_callsigns,
        )
    else:
        conn.execute("DELETE FROM ivao_atc_latest")

    conn.commit()
    update_feed_state(
        conn,
        feed_name=FEED_NAME,
        last_fetch=update_timestamp,
        last_update=update_timestamp,
        last_success=update_timestamp,
    )
    LOGGER.info("%s stored %d online ATC positions", FEED_NAME, len(seen_callsigns))
    return len(seen_callsigns)
