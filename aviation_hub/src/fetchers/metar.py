from __future__ import annotations

import csv
import gzip
import io
import logging
import sqlite3

import requests

from db import update_feed_state
from util import to_float, to_int, utc_now_iso, with_retries

LOGGER = logging.getLogger("aviation_hub.metar")
METAR_URL = "https://aviationweather.gov/data/cache/metars.cache.csv.gz"
FEED_NAME = "aviationweather_metar"


def _fetch_payload(session: requests.Session) -> list[dict[str, str]]:
    def _request() -> list[dict[str, str]]:
        response = session.get(METAR_URL, timeout=(10, 30))
        response.raise_for_status()
        decompressed = gzip.decompress(response.content)
        text_stream = io.StringIO(decompressed.decode("utf-8", errors="replace"))
        reader = csv.DictReader(text_stream)
        return list(reader)

    return with_retries(_request, context=FEED_NAME)


def process_metar(conn: sqlite3.Connection, session: requests.Session) -> tuple[bool, int]:
    fetched_at = utc_now_iso()
    rows = _fetch_payload(session)
    upserted = 0

    with conn:
        for row in rows:
            icao = (row.get("station_id") or "").strip().upper()
            obs_time = row.get("observation_time")
            if not icao or not obs_time:
                continue

            existing = conn.execute(
                "SELECT observation_time FROM metar_latest WHERE icao = ?",
                (icao,),
            ).fetchone()
            if existing and (existing["observation_time"] or "") >= obs_time:
                continue

            conn.execute(
                """
                INSERT INTO metar_latest (
                    icao, observation_time, raw_text, latitude, longitude,
                    temp_c, dewpoint_c, wind_dir_degrees, wind_speed_kt,
                    wind_gust_kt, visibility_statute_mi, altim_in_hg
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(icao)
                DO UPDATE SET
                    observation_time = excluded.observation_time,
                    raw_text = excluded.raw_text,
                    latitude = excluded.latitude,
                    longitude = excluded.longitude,
                    temp_c = excluded.temp_c,
                    dewpoint_c = excluded.dewpoint_c,
                    wind_dir_degrees = excluded.wind_dir_degrees,
                    wind_speed_kt = excluded.wind_speed_kt,
                    wind_gust_kt = excluded.wind_gust_kt,
                    visibility_statute_mi = excluded.visibility_statute_mi,
                    altim_in_hg = excluded.altim_in_hg
                """,
                (
                    icao,
                    obs_time,
                    row.get("raw_text"),
                    to_float(row.get("latitude")),
                    to_float(row.get("longitude")),
                    to_float(row.get("temp_c")),
                    to_float(row.get("dewpoint_c")),
                    to_int(row.get("wind_dir_degrees")),
                    to_int(row.get("wind_speed_kt")),
                    to_int(row.get("wind_gust_kt")),
                    to_float(row.get("visibility_statute_mi")),
                    to_float(row.get("altim_in_hg")),
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

    LOGGER.info("%s processed %s rows (%s upserts)", FEED_NAME, len(rows), upserted)
    return True, upserted
