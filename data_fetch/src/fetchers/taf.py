from __future__ import annotations

import gzip
import logging
import sqlite3
import xml.etree.ElementTree as ET

import requests

from db import update_feed_state
from fetchers.weather_derivation import recalc_latest_weather
from util import normalize_iso_utc, to_float, utc_now_iso, with_retries

LOGGER = logging.getLogger("aviation_hub.taf")
TAF_URL = "https://aviationweather.gov/data/cache/tafs.cache.xml.gz"
FEED_NAME = "aviationweather_taf"


def _find_text(node: ET.Element, path: str) -> str | None:
    value = node.findtext(path)
    if value is None:
        return None
    value = value.strip()
    return value or None


def _fetch_payload(session: requests.Session) -> list[ET.Element]:
    LOGGER.info("%s fetching TAF cache", FEED_NAME)

    def _request() -> list[ET.Element]:
        response = session.get(TAF_URL, timeout=(10, 30))
        response.raise_for_status()
        xml_bytes = gzip.decompress(response.content)
        root = ET.fromstring(xml_bytes)
        return root.findall(".//TAF")

    return with_retries(_request, context=FEED_NAME)


def process_taf(conn: sqlite3.Connection, session: requests.Session) -> tuple[bool, int]:
    fetched_at = utc_now_iso()
    taf_rows = _fetch_payload(session)
    upserted = 0

    with conn:
        for taf in taf_rows:
            icao = (_find_text(taf, "station_id") or "").upper()
            issue_time = normalize_iso_utc(_find_text(taf, "issue_time"))
            bulletin_time = normalize_iso_utc(_find_text(taf, "bulletin_time"))
            valid_from_time = normalize_iso_utc(_find_text(taf, "valid_time_from"))
            valid_to_time = normalize_iso_utc(_find_text(taf, "valid_time_to"))
            raw_text = _find_text(taf, "raw_text")

            if not icao:
                continue

            existing = conn.execute(
                "SELECT issue_time FROM taf_latest WHERE icao = ?",
                (icao,),
            ).fetchone()
            existing_issue_time = normalize_iso_utc(existing["issue_time"]) if existing else None
            if existing_issue_time and issue_time and existing_issue_time >= issue_time:
                continue

            conn.execute(
                """
                INSERT INTO taf_latest (
                    icao, issue_time, bulletin_time, valid_from_time, valid_to_time,
                    raw_text, latitude, longitude
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(icao)
                DO UPDATE SET
                    issue_time = excluded.issue_time,
                    bulletin_time = excluded.bulletin_time,
                    valid_from_time = excluded.valid_from_time,
                    valid_to_time = excluded.valid_to_time,
                    raw_text = excluded.raw_text,
                    latitude = excluded.latitude,
                    longitude = excluded.longitude
                """,
                (
                    icao,
                    issue_time,
                    bulletin_time,
                    valid_from_time,
                    valid_to_time,
                    raw_text,
                    to_float(_find_text(taf, "latitude")),
                    to_float(_find_text(taf, "longitude")),
                ),
            )
            upserted += 1

        update_feed_state(
            conn,
            feed_name=FEED_NAME,
            last_fetch=fetched_at,
            last_success=fetched_at,
            last_error=None,
            last_error_at=None,
        )

    if upserted == 0:
        LOGGER.info("%s unchanged (rows=%s) - skipping update", FEED_NAME, len(taf_rows))
    else:
        LOGGER.info("%s processed %s rows (%s upserts)", FEED_NAME, len(taf_rows), upserted)

    try:
        flags_upserted, scores_upserted = recalc_latest_weather(conn)
        LOGGER.info(
            "%s derived weather refreshed (flags=%s scores=%s)",
            FEED_NAME,
            flags_upserted,
            scores_upserted,
        )
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("%s weather derivation failed: %s", FEED_NAME, exc)
    return True, upserted
