from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any

import requests

from db import update_feed_state
from util import json_dumps_compact, normalize_iso_utc, to_int, utc_now_iso, with_retries

LOGGER = logging.getLogger("aviation_hub.sigmet")
SIGMET_URL = "https://aviationweather.gov/api/data/isigmet?format=json"
FEED_NAME = "aviationweather_sigmet"


def _normalize_epoch_or_iso(value: Any) -> str | None:
    if value in (None, ""):
        return None

    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")

    text = str(value).strip()
    if not text:
        return None

    if text.isdigit():
        return datetime.fromtimestamp(int(text), tz=timezone.utc).isoformat().replace("+00:00", "Z")

    return normalize_iso_utc(text)


def _sigmet_id(item: dict[str, Any]) -> str | None:
    explicit = item.get("id")
    if explicit not in (None, ""):
        return str(explicit).strip()

    fir = str(item.get("firId") or item.get("fir") or "").strip().upper()
    series = str(item.get("seriesId") or item.get("series") or "").strip().upper()
    valid_from = _normalize_epoch_or_iso(item.get("validTimeFrom")) or ""
    if fir and series and valid_from:
        return f"{fir}:{series}:{valid_from}"
    return None


def _fetch_payload(session: requests.Session) -> list[dict[str, Any]]:
    LOGGER.info("%s fetching international SIGMET data", FEED_NAME)

    def _request() -> list[dict[str, Any]]:
        response = session.get(
            SIGMET_URL,
            headers={"User-Agent": "AviationHub/1.0"},
            timeout=(10, 30),
        )
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError("Expected list from SIGMET endpoint")
        return data

    return with_retries(_request, context=FEED_NAME)


def process_sigmet(conn: sqlite3.Connection, session: requests.Session) -> tuple[bool, int]:
    fetched_at = utc_now_iso()
    items = _fetch_payload(session)
    upserted = 0
    latest_valid_from: str | None = None

    with conn:
        for item in items:
            sigmet_id = _sigmet_id(item)
            if not sigmet_id:
                continue

            valid_from = _normalize_epoch_or_iso(item.get("validTimeFrom"))
            valid_to = _normalize_epoch_or_iso(item.get("validTimeTo"))
            raw_text = item.get("rawSigmet") or item.get("raw_text")
            geometry_value = item.get("geom")
            geometry = None if geometry_value in (None, "") else json_dumps_compact(geometry_value)

            conn.execute(
                """
                INSERT INTO sigmets (
                    id, fir, fir_name, hazard, qualifier, base, top,
                    movement_dir, movement_speed, valid_from, valid_to,
                    raw_text, geometry, fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id)
                DO UPDATE SET
                    fir = excluded.fir,
                    fir_name = excluded.fir_name,
                    hazard = excluded.hazard,
                    qualifier = excluded.qualifier,
                    base = excluded.base,
                    top = excluded.top,
                    movement_dir = excluded.movement_dir,
                    movement_speed = excluded.movement_speed,
                    valid_from = excluded.valid_from,
                    valid_to = excluded.valid_to,
                    raw_text = excluded.raw_text,
                    geometry = excluded.geometry,
                    fetched_at = excluded.fetched_at
                """,
                (
                    sigmet_id,
                    (item.get("firId") or item.get("fir") or None),
                    item.get("firName"),
                    item.get("hazard"),
                    item.get("qualifier"),
                    to_int(item.get("base")),
                    to_int(item.get("top")),
                    to_int(item.get("dir")),
                    to_int(item.get("spd")),
                    valid_from,
                    valid_to,
                    raw_text,
                    geometry,
                    fetched_at,
                ),
            )
            upserted += 1

            if valid_from and (latest_valid_from is None or valid_from > latest_valid_from):
                latest_valid_from = valid_from

        update_feed_state(
            conn,
            feed_name=FEED_NAME,
            last_fetch=fetched_at,
            last_update=latest_valid_from,
            last_success=fetched_at,
            last_error=None,
            last_error_at=None,
        )

    LOGGER.info("%s fetched %s SIGMET records", FEED_NAME, len(items))
    if upserted == 0:
        LOGGER.info("%s unchanged (entries=%s) - skipping update", FEED_NAME, len(items))
    else:
        LOGGER.info("%s processed %s entries (%s upserts)", FEED_NAME, len(items), upserted)
    return True, upserted
