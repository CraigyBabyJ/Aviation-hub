from __future__ import annotations

import logging
import sqlite3

import requests

from db import get_feed_state, update_feed_state
from fetchers.airport_live_status import refresh_airport_live_status
from util import (
    extract_airport_from_callsign,
    json_dumps_compact,
    normalize_iso_utc,
    to_float,
    to_int,
    utc_now_iso,
    with_retries,
)

LOGGER = logging.getLogger("aviation_hub.vatsim")
VATSIM_URL = "https://data.vatsim.net/v3/vatsim-data.json"
FEED_NAME = "vatsim_network"


def _fetch_payload(session: requests.Session) -> dict:
    LOGGER.info("%s fetching network snapshot", FEED_NAME)

    def _request() -> dict:
        response = session.get(VATSIM_URL, timeout=(10, 30))
        response.raise_for_status()
        return response.json()

    return with_retries(_request, context=FEED_NAME)


def _insert_event(
    conn: sqlite3.Connection,
    *,
    ts: str,
    event_type: str,
    entity: str,
    airport: str | None,
    payload: dict,
    dedupe_key: str,
) -> int:
    cursor = conn.execute(
        """
        INSERT INTO events (ts, type, entity, airport, payload_json, dedupe_key)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(dedupe_key) DO NOTHING
        """,
        (ts, event_type, entity, airport, json_dumps_compact(payload), dedupe_key),
    )
    return cursor.rowcount


def _find_active_session_id(conn: sqlite3.Connection, callsign: str) -> int | None:
    row = conn.execute(
        """
        SELECT id
        FROM atc_sessions
        WHERE callsign = ? AND is_active = 1
        ORDER BY started_at DESC, id DESC
        LIMIT 1
        """,
        (callsign,),
    ).fetchone()
    return to_int(row["id"]) if row else None


def _open_atc_session(conn: sqlite3.Connection, *, ts: str, item: dict) -> bool:
    callsign = (item.get("callsign") or "").strip()
    if not callsign:
        return False
    if _find_active_session_id(conn, callsign) is not None:
        return False

    conn.execute(
        """
        INSERT INTO atc_sessions (
            callsign, airport, facility, frequency, name, cid, logon_time,
            started_at, last_seen, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        """,
        (
            callsign,
            extract_airport_from_callsign(callsign),
            to_int(item.get("facility")),
            item.get("frequency"),
            item.get("name"),
            to_int(item.get("cid")),
            item.get("logon_time"),
            ts,
            ts,
        ),
    )
    return True


def _touch_atc_session(conn: sqlite3.Connection, *, ts: str, item: dict) -> bool:
    callsign = (item.get("callsign") or "").strip()
    if not callsign:
        return False
    active_id = _find_active_session_id(conn, callsign)
    if active_id is None:
        return False

    conn.execute(
        """
        UPDATE atc_sessions
        SET
            last_seen = ?,
            frequency = COALESCE(?, frequency),
            facility = COALESCE(?, facility),
            name = COALESCE(?, name),
            cid = COALESCE(?, cid),
            logon_time = COALESCE(?, logon_time),
            airport = COALESCE(?, airport)
        WHERE id = ?
        """,
        (
            ts,
            item.get("frequency"),
            to_int(item.get("facility")),
            item.get("name"),
            to_int(item.get("cid")),
            item.get("logon_time"),
            extract_airport_from_callsign(callsign),
            active_id,
        ),
    )
    return True


def _close_atc_session(conn: sqlite3.Connection, *, ts: str, callsign: str) -> bool:
    active_id = _find_active_session_id(conn, callsign)
    if active_id is None:
        return False

    conn.execute(
        """
        UPDATE atc_sessions
        SET ended_at = ?, last_seen = ?, is_active = 0
        WHERE id = ?
        """,
        (ts, ts, active_id),
    )
    return True


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
    LOGGER.info(
        "%s timestamp check: remote=%s local=%s",
        FEED_NAME,
        update_timestamp,
        state_last_update,
    )
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
    current_atc: dict[str, dict] = {}
    online_events = 0
    offline_events = 0
    sessions_opened = 0
    sessions_updated = 0
    sessions_closed = 0

    with conn:
        for item in controllers:
            callsign = (item.get("callsign") or "").strip()
            if not callsign:
                continue
            current_callsigns.add(callsign)
            facility = to_int(item.get("facility"))
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
                    facility,
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
            if facility and facility > 0:
                current_atc[callsign] = {
                    "callsign": callsign,
                    "cid": to_int(item.get("cid")),
                    "name": item.get("name"),
                    "facility": facility,
                    "rating": to_int(item.get("rating")),
                    "frequency": item.get("frequency"),
                    "server": item.get("server"),
                    "logon_time": item.get("logon_time"),
                    "last_updated": update_timestamp,
                }

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

        try:
            seen_rows = conn.execute(
                """
                SELECT callsign, last_seen, last_frequency, last_facility, last_updated,
                       cid, name, rating, server, logon_time
                FROM atc_seen
                """
            ).fetchall()
            seen_by_callsign = {row["callsign"]: row for row in seen_rows}
            current_atc_callsigns = set(current_atc.keys())
            seen_callsigns = set(seen_by_callsign.keys())

            for callsign in sorted(current_atc_callsigns - seen_callsigns):
                item = current_atc[callsign]
                online_events += _insert_event(
                    conn,
                    ts=fetched_at,
                    event_type="ATC_ONLINE",
                    entity=callsign,
                    airport=extract_airport_from_callsign(callsign),
                    payload=item,
                    dedupe_key=f"ATC_ONLINE:{callsign}:{item.get('logon_time') or ''}",
                )
                if _open_atc_session(conn, ts=fetched_at, item=item):
                    sessions_opened += 1

            for callsign in sorted(seen_callsigns - current_atc_callsigns):
                row = seen_by_callsign[callsign]
                payload = {
                    "callsign": callsign,
                    "cid": row["cid"],
                    "name": row["name"],
                    "facility": row["last_facility"],
                    "rating": row["rating"],
                    "frequency": row["last_frequency"],
                    "server": row["server"],
                    "logon_time": row["logon_time"],
                    "last_updated": row["last_updated"],
                }
                offline_events += _insert_event(
                    conn,
                    ts=fetched_at,
                    event_type="ATC_OFFLINE",
                    entity=callsign,
                    airport=extract_airport_from_callsign(callsign),
                    payload=payload,
                    dedupe_key=f"ATC_OFFLINE:{callsign}:{row['last_seen']}",
                )
                if _close_atc_session(conn, ts=fetched_at, callsign=callsign):
                    sessions_closed += 1

            for item in current_atc.values():
                if _touch_atc_session(conn, ts=fetched_at, item=item):
                    sessions_updated += 1

            for callsign, item in current_atc.items():
                conn.execute(
                    """
                    INSERT INTO atc_seen (
                        callsign, last_seen, last_status, last_frequency, last_facility,
                        last_updated, cid, name, rating, server, logon_time
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(callsign)
                    DO UPDATE SET
                        last_seen = excluded.last_seen,
                        last_status = excluded.last_status,
                        last_frequency = excluded.last_frequency,
                        last_facility = excluded.last_facility,
                        last_updated = excluded.last_updated,
                        cid = excluded.cid,
                        name = excluded.name,
                        rating = excluded.rating,
                        server = excluded.server,
                        logon_time = excluded.logon_time
                    """,
                    (
                        callsign,
                        fetched_at,
                        "online",
                        item["frequency"],
                        item["facility"],
                        item["last_updated"],
                        item["cid"],
                        item["name"],
                        item["rating"],
                        item["server"],
                        item["logon_time"],
                    ),
                )

            if current_atc_callsigns:
                placeholders = ",".join("?" for _ in current_atc_callsigns)
                conn.execute(
                    f"DELETE FROM atc_seen WHERE callsign NOT IN ({placeholders})",
                    tuple(current_atc_callsigns),
                )
            else:
                conn.execute("DELETE FROM atc_seen")
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("ATC event processing failed: %s", exc)

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
    LOGGER.info(
        "%s events created: online=%s offline=%s",
        FEED_NAME,
        online_events,
        offline_events,
    )
    LOGGER.info(
        "%s sessions: opened=%s updated=%s closed=%s",
        FEED_NAME,
        sessions_opened,
        sessions_updated,
        sessions_closed,
    )
    try:
        refreshed = refresh_airport_live_status(conn)
        LOGGER.info("%s airport_live_status refreshed (%s rows)", FEED_NAME, refreshed)
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("%s airport_live_status refresh failed: %s", FEED_NAME, exc)
    return True, controller_count + pilot_count, reload_seconds


def next_poll_seconds(last_reload_hint: int | None) -> int:
    if last_reload_hint is None:
        return 60
    return max(30, min(120, last_reload_hint))
