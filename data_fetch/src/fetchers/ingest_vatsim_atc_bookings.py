from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any

import requests

from db import update_feed_state
from fetchers.vatsim_schedule_utils import derive_vatsim_booking_fields
from util import json_dumps_compact, normalize_vatsim_api_time, utc_now_iso, with_retries

LOGGER = logging.getLogger("aviation_hub.vatsim_bookings")
FEED_NAME = "vatsim_atc_bookings"

DEFAULT_BOOKINGS_URL = "https://atc-bookings.vatsim.net/api/booking"


def _truthy_env(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def bookings_enabled() -> bool:
    return _truthy_env("VATSIM_BOOKINGS_ENABLED", True)


def _bookings_url() -> str:
    return (os.environ.get("VATSIM_BOOKINGS_URL") or DEFAULT_BOOKINGS_URL).strip().rstrip("/")


def _api_key() -> str | None:
    key = os.environ.get("VATSIM_BOOKINGS_API_KEY")
    if key is None:
        return None
    stripped = key.strip()
    return stripped or None


def _fetch_bookings(session: requests.Session, url: str, api_key: str | None) -> list[dict[str, Any]]:
    if api_key:
        LOGGER.info("%s fetching %s (Bearer token set)", FEED_NAME, url)
    else:
        LOGGER.info("%s fetching %s (no API key; public list)", FEED_NAME, url)

    def _request() -> list[dict[str, Any]]:
        headers: dict[str, str] = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        response = session.get(url, headers=headers, timeout=(10, 60))
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list):
            return [x for x in payload if isinstance(x, dict)]
        if isinstance(payload, dict):
            for key in ("data", "bookings", "items", "results"):
                block = payload.get(key)
                if isinstance(block, list):
                    return [x for x in block if isinstance(x, dict)]
        raise ValueError("Unrecognized bookings payload shape (expected JSON array)")

    return with_retries(_request, context=FEED_NAME, attempts=4, base_delay=1.5)


def _row_from_booking(b: dict[str, Any], fetched_at: str) -> tuple[Any, ...] | None:
    raw_id = b.get("id")
    if raw_id is None:
        LOGGER.warning("%s skipping booking without id: %s", FEED_NAME, b)
        return None
    booking_id = str(raw_id).strip()
    callsign = (b.get("callsign") or "").strip()
    if not booking_id or not callsign:
        LOGGER.warning("%s skipping booking without callsign (id=%s)", FEED_NAME, booking_id)
        return None

    start = normalize_vatsim_api_time(b.get("start") or b.get("starts_at") or b.get("start_time"))
    end = normalize_vatsim_api_time(b.get("end") or b.get("ends_at") or b.get("end_time"))
    if not start or not end:
        LOGGER.warning(
            "%s skipping booking with bad times (id=%s callsign=%s)",
            FEED_NAME,
            booking_id,
            callsign,
        )
        return None

    airport_guess, fir_guess, position_type = derive_vatsim_booking_fields(callsign)
    cid_val = b.get("cid") or b.get("controller_cid") or b.get("vatsim_id")
    controller_cid = str(cid_val).strip() if cid_val is not None and str(cid_val).strip() else None
    controller_name = None
    for key in ("name", "controller_name", "controller", "pilot_name"):
        v = b.get(key)
        if v is not None and str(v).strip():
            controller_name = str(v).strip()
            break

    booking_type = None
    bt = b.get("type") or b.get("booking_type")
    if bt is not None and str(bt).strip():
        booking_type = str(bt).strip()

    raw_json = json_dumps_compact(b)

    return (
        booking_id,
        callsign,
        airport_guess,
        fir_guess,
        position_type,
        controller_cid,
        controller_name,
        start,
        end,
        booking_type,
        fetched_at,
        raw_json,
    )


def process_vatsim_atc_bookings(conn: sqlite3.Connection, session: requests.Session) -> None:
    if not bookings_enabled():
        LOGGER.info("%s skipped: VATSIM_BOOKINGS_ENABLED is off", FEED_NAME)
        return

    api_key = _api_key()
    url = _bookings_url()
    fetched_at = utc_now_iso()

    try:
        bookings = _fetch_bookings(session, url, api_key)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("%s fetch failed: %s", FEED_NAME, exc)
        update_feed_state(
            conn,
            feed_name=FEED_NAME,
            last_fetch=fetched_at,
            last_error=str(exc),
            last_error_at=fetched_at,
        )
        return

    rows: list[tuple[Any, ...]] = []
    for b in bookings:
        row = _row_from_booking(b, fetched_at)
        if row:
            rows.append(row)

    insert_sql = """
        INSERT INTO vatsim_atc_bookings_latest (
            booking_id, callsign, airport_icao, fir_icao, position_type,
            controller_cid, controller_name, starts_at_utc, ends_at_utc,
            booking_type, fetched_at_utc, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(booking_id) DO UPDATE SET
            callsign = excluded.callsign,
            airport_icao = excluded.airport_icao,
            fir_icao = excluded.fir_icao,
            position_type = excluded.position_type,
            controller_cid = excluded.controller_cid,
            controller_name = excluded.controller_name,
            starts_at_utc = excluded.starts_at_utc,
            ends_at_utc = excluded.ends_at_utc,
            booking_type = excluded.booking_type,
            fetched_at_utc = excluded.fetched_at_utc,
            raw_json = excluded.raw_json
    """

    with conn:
        conn.execute("DELETE FROM vatsim_atc_bookings_latest")
        if rows:
            conn.executemany(insert_sql, rows)

        update_feed_state(
            conn,
            feed_name=FEED_NAME,
            last_fetch=fetched_at,
            last_success=fetched_at,
            last_error=None,
            last_error_at=None,
        )

    LOGGER.info("%s synced %s bookings (snapshot replace)", FEED_NAME, len(rows))
