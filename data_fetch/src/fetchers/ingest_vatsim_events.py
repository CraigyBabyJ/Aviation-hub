from __future__ import annotations

import logging
import os
import sqlite3
from typing import Any

import requests

from db import update_feed_state
from fetchers.vatsim_schedule_utils import (
    extract_event_id,
    normalize_event_airports_json,
    organisers_sidecar_json,
    pick_str,
)
from util import json_dumps_compact, normalize_vatsim_api_time, utc_now_iso, with_retries

LOGGER = logging.getLogger("aviation_hub.vatsim_events")
FEED_NAME = "vatsim_events"

DEFAULT_EVENTS_URL = "https://events.vatsim.net/v1/latest"


def _truthy_env(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _events_url() -> str:
    return (os.environ.get("VATSIM_EVENTS_URL") or DEFAULT_EVENTS_URL).strip()


def events_enabled() -> bool:
    return _truthy_env("VATSIM_EVENTS_ENABLED", True)


def _coerce_event_list(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if isinstance(payload, dict):
        for key in ("data", "events", "items", "results"):
            block = payload.get(key)
            if isinstance(block, list):
                return [x for x in block if isinstance(x, dict)]
    raise ValueError("Unrecognized events payload shape (expected list or object with data/events array)")


def _fetch_events(session: requests.Session, url: str) -> list[dict[str, Any]]:
    LOGGER.info("%s fetching %s", FEED_NAME, url)

    def _request() -> list[dict[str, Any]]:
        response = session.get(url, timeout=(10, 60))
        response.raise_for_status()
        payload = response.json()
        return _coerce_event_list(payload)

    return with_retries(_request, context=FEED_NAME, attempts=4, base_delay=1.5)


def _row_from_event(ev: dict[str, Any], fetched_at: str) -> tuple[Any, ...] | None:
    event_id = extract_event_id(ev)
    name = pick_str(ev, "name", "title", "event_name")
    start = normalize_vatsim_api_time(
        pick_str(ev, "start_time", "startTime", "starts_at", "start", "begin")
    )
    end = normalize_vatsim_api_time(pick_str(ev, "end_time", "endTime", "ends_at", "end", "finish"))
    if not event_id or not name or not start or not end:
        LOGGER.warning(
            "%s skipping malformed event (id=%r name=%r start=%r end=%r)",
            FEED_NAME,
            event_id,
            name,
            start,
            end,
        )
        return None

    event_type = pick_str(ev, "type", "event_type", "eventType", "category")
    short_description = pick_str(ev, "short_description", "shortDescription", "summary")
    description = pick_str(ev, "description", "details", "body")
    banner_url = pick_str(ev, "banner", "banner_url", "bannerUrl", "image", "image_url")
    link_url = pick_str(ev, "link", "link_url", "linkUrl", "url", "href")

    airports_raw = ev.get("airports")
    organisers_raw = ev.get("organisers")
    if organisers_raw is None:
        organisers_raw = ev.get("organizers")

    airports_json = normalize_event_airports_json(airports_raw)
    organisers_json, divisions_json, regions_json = organisers_sidecar_json(organisers_raw)

    raw_json = json_dumps_compact(ev)

    return (
        event_id,
        name,
        event_type,
        start,
        end,
        short_description,
        description,
        banner_url,
        link_url,
        airports_json,
        organisers_json,
        divisions_json,
        regions_json,
        fetched_at,
        raw_json,
    )


def process_vatsim_events(conn: sqlite3.Connection, session: requests.Session) -> None:
    if not events_enabled():
        LOGGER.info("%s skipped: VATSIM_EVENTS_ENABLED is off", FEED_NAME)
        return

    url = _events_url()
    fetched_at = utc_now_iso()

    try:
        events = _fetch_events(session, url)
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
    for ev in events:
        row = _row_from_event(ev, fetched_at)
        if row:
            rows.append(row)

    insert_sql = """
        INSERT INTO vatsim_events_latest (
            event_id, name, event_type, start_time_utc, end_time_utc,
            short_description, description, banner_url, link_url,
            airports_json, organisers_json, divisions_json, regions_json,
            fetched_at_utc, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            name = excluded.name,
            event_type = excluded.event_type,
            start_time_utc = excluded.start_time_utc,
            end_time_utc = excluded.end_time_utc,
            short_description = excluded.short_description,
            description = excluded.description,
            banner_url = excluded.banner_url,
            link_url = excluded.link_url,
            airports_json = excluded.airports_json,
            organisers_json = excluded.organisers_json,
            divisions_json = excluded.divisions_json,
            regions_json = excluded.regions_json,
            fetched_at_utc = excluded.fetched_at_utc,
            raw_json = excluded.raw_json
    """

    with conn:
        conn.execute("DELETE FROM vatsim_events_latest")
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

    LOGGER.info("%s synced %s events (snapshot replace)", FEED_NAME, len(rows))
