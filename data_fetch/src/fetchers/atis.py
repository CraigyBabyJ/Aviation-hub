from __future__ import annotations

import logging
import sqlite3

import requests

from db import update_feed_state
from util import (
    extract_airport_from_callsign,
    json_dumps_compact,
    normalize_iso_utc,
    sha256_text,
    utc_now_iso,
    with_retries,
)

LOGGER = logging.getLogger("aviation_hub.atis")
ATIS_URL = "https://data.vatsim.net/v3/afv-atis-data.json"
FEED_NAME = "vatsim_atis"


def _fetch_payload(session: requests.Session) -> list[dict]:
    LOGGER.info("%s fetching AFV ATIS data", FEED_NAME)

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
    atis_changed_events = 0

    with conn:
        for item in items:
            callsign = (item.get("callsign") or "").strip()
            if not callsign:
                continue

            last_updated = normalize_iso_utc(item.get("last_updated"))
            if not last_updated:
                continue

            existing = conn.execute(
                "SELECT last_updated, text_hash FROM vatsim_atis_latest WHERE callsign = ?",
                (callsign,),
            ).fetchone()
            existing_last_updated = normalize_iso_utc(existing["last_updated"]) if existing else None
            if existing_last_updated and existing_last_updated >= last_updated:
                continue

            text_lines = item.get("text_atis") or []
            if isinstance(text_lines, str):
                text_lines = [text_lines]
            text_value = "\n".join(str(line) for line in text_lines)
            text_hash = sha256_text(text_value)
            airport = extract_airport_from_callsign(callsign) or callsign[:4]

            try:
                if existing and existing["text_hash"] and existing["text_hash"] != text_hash:
                    payload = {
                        "callsign": callsign,
                        "airport": airport,
                        "atis_code": item.get("atis_code"),
                        "frequency": item.get("frequency"),
                        "last_updated": last_updated,
                        "text_hash": text_hash,
                        "text_preview": text_value[:120],
                    }
                    cursor = conn.execute(
                        """
                        INSERT INTO events (ts, type, entity, airport, payload_json, dedupe_key)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(dedupe_key) DO NOTHING
                        """,
                        (
                            fetched_at,
                            "ATIS_CHANGED",
                            callsign,
                            airport,
                            json_dumps_compact(payload),
                            f"ATIS_CHANGED:{callsign}:{text_hash}",
                        ),
                    )
                    atis_changed_events += cursor.rowcount
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("ATIS event processing failed for %s: %s", callsign, exc)

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
                    airport,
                    item.get("atis_code"),
                    item.get("frequency"),
                    text_value,
                    text_hash,
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

    if upserted == 0:
        LOGGER.info("%s unchanged (entries=%s) - skipping update", FEED_NAME, len(items))
    else:
        LOGGER.info("%s processed %s entries (%s upserts)", FEED_NAME, len(items), upserted)
    LOGGER.info("%s events created: atis_changed=%s", FEED_NAME, atis_changed_events)
    return True, upserted
