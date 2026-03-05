from __future__ import annotations

import logging
import sqlite3

import requests

from db import update_feed_state
from util import sha256_text, utc_now_iso, with_retries

LOGGER = logging.getLogger("aviation_hub.atis")
ATIS_URL = "https://data.vatsim.net/v3/afv-atis-data.json"
FEED_NAME = "vatsim_atis"


def _fetch_payload(session: requests.Session) -> list[dict]:
    def _request() -> list[dict]:
        response = session.get(ATIS_URL, timeout=(10, 30))
        response.raise_for_status()
        data = response.json()
        if not isinstance(data, list):
            raise ValueError("Expected list from AFV ATIS endpoint")
        return data

    return with_retries(_request, context=FEED_NAME)


def process_atis(conn: sqlite3.Connection, session: requests.Session) -> tuple[bool, int]:
    fetched_at = utc_now_iso()
    items = _fetch_payload(session)
    upserted = 0

    with conn:
        for item in items:
            callsign = (item.get("callsign") or "").strip()
            if not callsign:
                continue

            last_updated = item.get("last_updated")
            if not last_updated:
                continue

            existing = conn.execute(
                "SELECT last_updated FROM vatsim_atis_latest WHERE callsign = ?",
                (callsign,),
            ).fetchone()
            if existing and (existing["last_updated"] or "") >= last_updated:
                continue

            text_lines = item.get("text_atis") or []
            if isinstance(text_lines, str):
                text_lines = [text_lines]
            text_value = "\n".join(str(line) for line in text_lines)

            conn.execute(
                """
                INSERT INTO vatsim_atis_latest (
                    callsign, airport, atis_code, frequency, text, text_hash, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(callsign)
                DO UPDATE SET
                    airport = excluded.airport,
                    atis_code = excluded.atis_code,
                    frequency = excluded.frequency,
                    text = excluded.text,
                    text_hash = excluded.text_hash,
                    last_updated = excluded.last_updated
                """,
                (
                    callsign,
                    callsign[:4],
                    item.get("atis_code"),
                    item.get("frequency"),
                    text_value,
                    sha256_text(text_value),
                    last_updated,
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

    LOGGER.info("%s processed %s entries (%s upserts)", FEED_NAME, len(items), upserted)
    return True, upserted
