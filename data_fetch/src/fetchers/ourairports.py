from __future__ import annotations

import csv
import logging
import sqlite3
from datetime import timedelta
from io import StringIO
from pathlib import Path

import requests

from db import get_feed_state, update_feed_state
from util import parse_iso_utc, to_float, to_int, utc_now_iso, with_retries

LOGGER = logging.getLogger("aviation_hub.ourairports")
FEED_NAME = "ourairports_sync"
MIN_SYNC_INTERVAL = timedelta(days=7)
OUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "ourairports"
SOURCES = {
    "airports.csv": "https://davidmegginson.github.io/ourairports-data/airports.csv",
    "runways.csv": "https://davidmegginson.github.io/ourairports-data/runways.csv",
    "navaids.csv": "https://davidmegginson.github.io/ourairports-data/navaids.csv",
}


def _sync_airport_reference(conn: sqlite3.Connection) -> int:
    airports_path = OUT_DIR / "airports.csv"
    if not airports_path.exists():
        return 0

    text = airports_path.read_text(encoding="utf-8", errors="replace")
    reader = csv.DictReader(StringIO(text))
    upserted = 0
    with conn:
        for row in reader:
            icao = (row.get("ident") or "").strip().upper()
            if len(icao) != 4:
                continue
            conn.execute(
                """
                INSERT INTO airport_reference_latest (
                    icao, iata, name, latitude_deg, longitude_deg, elevation_ft,
                    country, region, municipality, continent, type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(icao)
                DO UPDATE SET
                    iata = excluded.iata,
                    name = excluded.name,
                    latitude_deg = excluded.latitude_deg,
                    longitude_deg = excluded.longitude_deg,
                    elevation_ft = excluded.elevation_ft,
                    country = excluded.country,
                    region = excluded.region,
                    municipality = excluded.municipality,
                    continent = excluded.continent,
                    type = excluded.type
                """,
                (
                    icao,
                    (row.get("iata_code") or "").strip().upper() or None,
                    row.get("name"),
                    to_float(row.get("latitude_deg")),
                    to_float(row.get("longitude_deg")),
                    to_int(row.get("elevation_ft")),
                    (row.get("iso_country") or "").strip().upper() or None,
                    (row.get("iso_region") or "").strip().upper() or None,
                    row.get("municipality"),
                    (row.get("continent") or "").strip().upper() or None,
                    row.get("type"),
                ),
            )
            upserted += 1
    return upserted


def _download(session: requests.Session, url: str) -> bytes:
    def _request() -> bytes:
        response = session.get(url, timeout=(10, 120))
        response.raise_for_status()
        return response.content

    return with_retries(_request, context=FEED_NAME)


def process_ourairports(conn: sqlite3.Connection, session: requests.Session) -> tuple[bool, int]:
    fetched_at = utc_now_iso()
    now = parse_iso_utc(fetched_at)
    if now is None:
        raise ValueError("Unable to parse current UTC timestamp")

    state = get_feed_state(conn, FEED_NAME)
    last_success = parse_iso_utc(state["last_success"]) if state and state["last_success"] else None

    if last_success and (now - last_success) < MIN_SYNC_INTERVAL:
        ref_rows = _sync_airport_reference(conn)
        next_due = last_success + MIN_SYNC_INTERVAL
        update_feed_state(
            conn,
            feed_name=FEED_NAME,
            last_fetch=fetched_at,
            last_error=None,
            last_error_at=None,
        )
        LOGGER.info(
            "%s not due yet (last_success=%s next_due=%s) - skipping download",
            FEED_NAME,
            last_success.isoformat().replace("+00:00", "Z"),
            next_due.isoformat().replace("+00:00", "Z"),
        )
        if ref_rows > 0:
            LOGGER.info("%s refreshed airport reference rows from disk (%s)", FEED_NAME, ref_rows)
        return False, 0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    files_written = 0
    for filename, url in SOURCES.items():
        LOGGER.info("%s downloading %s", FEED_NAME, url)
        content = _download(session, url)
        target = OUT_DIR / filename
        temp = OUT_DIR / f".{filename}.tmp"
        temp.write_bytes(content)
        temp.replace(target)
        files_written += 1
        LOGGER.info("%s saved %s (%s bytes)", FEED_NAME, target, target.stat().st_size)

    ref_rows = _sync_airport_reference(conn)

    update_feed_state(
        conn,
        feed_name=FEED_NAME,
        last_fetch=fetched_at,
        last_success=fetched_at,
        last_error=None,
        last_error_at=None,
    )
    LOGGER.info("%s sync complete (%s files, %s airport reference rows)", FEED_NAME, files_written, ref_rows)
    return True, files_written
