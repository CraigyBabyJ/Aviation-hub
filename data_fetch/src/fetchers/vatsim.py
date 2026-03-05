from __future__ import annotations

import logging
import sqlite3

import requests

from db import get_feed_state, update_feed_state
from util import normalize_iso_utc, to_float, to_int, utc_now_iso, with_retries

LOGGER = logging.getLogger("aviation_hub.vatsim")
VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json"
FEED_NAME = "vatsim_network"


def _fetch_payload(session: requests.Session) -> dict:
    def _request() -> dict:
        response = session.get(VATSIM_URL, timeout=(10, 30))
        response.raise_for_status()
        return response.json()

    return with_retries(_request, context=FEED_NAME)


def process_vatsim_network(
    conn: sqlite3.Connection,
    session: requests.Session,
) -> tuple[bool, int, int]:
    fetched_at = utc_now_iso()
    payload = _fetch_payload(session)

    # VATSIM's update_timestamp is the authoritative feed-change marker.
    update_timestamp = normalize_iso_utc(payload.get("general", {}).get("update_timestamp"))
    if not update_timestamp:
        raise ValueError("VATSIM payload missing general.update_timestamp")

    state = get_feed_state(conn, FEED_NAME)
    state_last_update = normalize_iso_utc(state["last_update"]) if state else None
    # Skip unchanged snapshots to avoid unnecessary DB churn.
    if state_last_update == update_timestamp:
        update_feed_state(
            conn,
            feed_name=FEED_NAME,
            last_fetch=fetched_at,
            last_error=None,
            last_error_at=None,
        )
        LOGGER.info(
            "%s unchanged (timestamp=%s) - skipping update",
            FEED_NAME,
            update_timestamp,
        )
        return False, 0, 0

    controllers = payload.get("controllers", [])
    pilots = payload.get("pilots", [])
    current_callsigns: set[str] = set()
    current_pilot_callsigns: set[str] = set()

    with conn:
        for item in controllers:
            callsign = (item.get("callsign") or "").strip()
            if not callsign:
                continue
            current_callsigns.add(callsign)
            conn.execute(
                """
                INSERT INTO vatsim_controllers_latest (
                    callsign, cid, name, facility, rating, frequency,
                    latitude, longitude, altitude, server, visual_range,
                    logon_time, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(callsign)
                DO UPDATE SET
                    cid = excluded.cid,
                    name = excluded.name,
                    facility = excluded.facility,
                    rating = excluded.rating,
                    frequency = excluded.frequency,
                    latitude = excluded.latitude,
                    longitude = excluded.longitude,
                    altitude = excluded.altitude,
                    server = excluded.server,
                    visual_range = excluded.visual_range,
                    logon_time = excluded.logon_time,
                    last_updated = excluded.last_updated
                """,
                (
                    callsign,
                    to_int(item.get("cid")),
                    item.get("name"),
                    to_int(item.get("facility")),
                    to_int(item.get("rating")),
                    item.get("frequency"),
                    to_float(item.get("latitude")),
                    to_float(item.get("longitude")),
                    to_int(item.get("altitude")),
                    item.get("server"),
                    to_int(item.get("visual_range")),
                    item.get("logon_time"),
                    update_timestamp,
                ),
            )

        for item in pilots:
            callsign = (item.get("callsign") or "").strip()
            if not callsign:
                continue

            flight_plan = item.get("flight_plan") or {}
            current_pilot_callsigns.add(callsign)
            conn.execute(
                """
                INSERT INTO vatsim_pilots_latest (
                    callsign, cid, name, server, pilot_rating,
                    latitude, longitude, altitude, groundspeed,
                    transponder, heading, qnh_i_hg, qnh_mb,
                    flight_plan_aircraft, flight_plan_departure,
                    flight_plan_arrival, flight_plan_altitude,
                    flight_plan_rules, logon_time, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(callsign)
                DO UPDATE SET
                    cid = excluded.cid,
                    name = excluded.name,
                    server = excluded.server,
                    pilot_rating = excluded.pilot_rating,
                    latitude = excluded.latitude,
                    longitude = excluded.longitude,
                    altitude = excluded.altitude,
                    groundspeed = excluded.groundspeed,
                    transponder = excluded.transponder,
                    heading = excluded.heading,
                    qnh_i_hg = excluded.qnh_i_hg,
                    qnh_mb = excluded.qnh_mb,
                    flight_plan_aircraft = excluded.flight_plan_aircraft,
                    flight_plan_departure = excluded.flight_plan_departure,
                    flight_plan_arrival = excluded.flight_plan_arrival,
                    flight_plan_altitude = excluded.flight_plan_altitude,
                    flight_plan_rules = excluded.flight_plan_rules,
                    logon_time = excluded.logon_time,
                    last_updated = excluded.last_updated
                """,
                (
                    callsign,
                    to_int(item.get("cid")),
                    item.get("name"),
                    item.get("server"),
                    to_int(item.get("pilot_rating")),
                    to_float(item.get("latitude")),
                    to_float(item.get("longitude")),
                    to_int(item.get("altitude")),
                    to_int(item.get("groundspeed")),
                    item.get("transponder"),
                    to_int(item.get("heading")),
                    to_float(item.get("qnh_i_hg")),
                    to_int(item.get("qnh_mb")),
                    flight_plan.get("aircraft"),
                    flight_plan.get("departure"),
                    flight_plan.get("arrival"),
                    flight_plan.get("altitude"),
                    flight_plan.get("flight_rules"),
                    item.get("logon_time"),
                    update_timestamp,
                ),
            )

        if current_callsigns:
            placeholders = ",".join("?" for _ in current_callsigns)
            conn.execute(
                f"DELETE FROM vatsim_controllers_latest WHERE callsign NOT IN ({placeholders})",
                tuple(current_callsigns),
            )
        else:
            conn.execute("DELETE FROM vatsim_controllers_latest")

        if current_pilot_callsigns:
            pilot_placeholders = ",".join("?" for _ in current_pilot_callsigns)
            conn.execute(
                f"DELETE FROM vatsim_pilots_latest WHERE callsign NOT IN ({pilot_placeholders})",
                tuple(current_pilot_callsigns),
            )
        else:
            conn.execute("DELETE FROM vatsim_pilots_latest")

        update_feed_state(
            conn,
            feed_name=FEED_NAME,
            last_fetch=fetched_at,
            last_update=update_timestamp,
            last_success=fetched_at,
            last_error=None,
            last_error_at=None,
        )

    reload_seconds = payload.get("general", {}).get("reload", 60)
    try:
        reload_seconds = int(float(reload_seconds))
    except (TypeError, ValueError):
        reload_seconds = 60
    reload_seconds = max(30, min(120, reload_seconds))

    controller_count = len(current_callsigns)
    pilot_count = len(current_pilot_callsigns)
    LOGGER.info(
        "%s updated at %s with %s controllers and %s pilots",
        FEED_NAME,
        update_timestamp,
        controller_count,
        pilot_count,
    )
    return True, controller_count + pilot_count, reload_seconds


def next_poll_seconds(last_reload_hint: int | None) -> int:
    if last_reload_hint is None:
        return 60
    return max(30, min(120, last_reload_hint))
