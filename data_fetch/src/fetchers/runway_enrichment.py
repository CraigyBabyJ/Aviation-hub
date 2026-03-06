from __future__ import annotations

import csv
import logging
import sqlite3
from io import StringIO
from pathlib import Path

from util import sha256_text, to_float, to_int, utc_now_iso

LOGGER = logging.getLogger("aviation_hub.runways")


def classify_surface(surface: str | None) -> str:
    token = (surface or "").strip().upper()
    if not token:
        return "unknown"

    hard_terms = ("ASP", "ASPH", "ASPHALT", "CON", "CONC", "CONCRETE", "BIT", "PAVED")
    soft_terms = ("GRASS", "TURF", "DIRT", "GRVL", "GRAVEL", "SAND", "EARTH", "SOIL", "CLAY")

    if "WATER" in token:
        return "water"
    if any(term in token for term in hard_terms):
        return "hard"
    if any(term in token for term in soft_terms):
        return "soft"
    return "unknown"


def ingest_runways_from_disk(conn: sqlite3.Connection, runways_path: Path) -> int:
    if not runways_path.exists():
        return 0

    text = runways_path.read_text(encoding="utf-8", errors="replace")
    reader = csv.DictReader(StringIO(text))
    ts = utc_now_iso()
    upserted = 0

    with conn:
        conn.execute("DELETE FROM airport_runways_latest")
        for row in reader:
            airport_ident = (row.get("airport_ident") or "").strip().upper()
            if not airport_ident:
                continue

            airport_icao = airport_ident if len(airport_ident) == 4 else None
            payload_for_hash = "|".join(
                str(row.get(k) or "")
                for k in (
                    "airport_ident",
                    "length_ft",
                    "width_ft",
                    "surface",
                    "lighted",
                    "closed",
                    "le_ident",
                    "he_ident",
                    "le_heading_degT",
                    "he_heading_degT",
                )
            )

            conn.execute(
                """
                INSERT INTO airport_runways_latest (
                    airport_ident, airport_icao, length_ft, width_ft, surface, surface_class,
                    lighted, closed, le_ident, he_ident, le_heading_degT, he_heading_degT, source_hash, last_updated
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    airport_ident,
                    airport_icao,
                    to_int(row.get("length_ft")),
                    to_int(row.get("width_ft")),
                    row.get("surface"),
                    classify_surface(row.get("surface")),
                    to_int(row.get("lighted")) or 0,
                    to_int(row.get("closed")) or 0,
                    row.get("le_ident"),
                    row.get("he_ident"),
                    to_float(row.get("le_heading_degT")),
                    to_float(row.get("he_heading_degT")),
                    sha256_text(payload_for_hash),
                    ts,
                ),
            )
            upserted += 1

    LOGGER.info("runway ingest complete: rows=%s", upserted)
    return upserted


def refresh_runway_summary(conn: sqlite3.Connection) -> int:
    ts = utc_now_iso()
    rows = conn.execute(
        """
        SELECT
            COALESCE(airport_icao, airport_ident) AS airport,
            length_ft, width_ft, surface_class, lighted, closed
        FROM airport_runways_latest
        WHERE airport_ident IS NOT NULL AND airport_ident != ''
        """
    ).fetchall()

    summary: dict[str, dict[str, int]] = {}
    for row in rows:
        airport = (row["airport"] or "").strip().upper()
        if not airport:
            continue
        item = summary.setdefault(
            airport,
            {
                "runway_count": 0,
                "active_runway_count": 0,
                "longest_runway_ft": 0,
                "longest_hard_runway_ft": 0,
                "longest_soft_runway_ft": 0,
                "widest_runway_ft": 0,
                "has_hard_surface": 0,
                "has_soft_surface": 0,
                "has_water_runway": 0,
                "has_lighted_runway": 0,
            },
        )
        length_ft = to_int(row["length_ft"]) or 0
        width_ft = to_int(row["width_ft"]) or 0
        surface_class = (row["surface_class"] or "unknown").strip().lower()
        closed = to_int(row["closed"]) or 0
        lighted = to_int(row["lighted"]) or 0

        item["runway_count"] += 1
        if closed == 0:
            item["active_runway_count"] += 1
            item["longest_runway_ft"] = max(item["longest_runway_ft"], length_ft)
            item["widest_runway_ft"] = max(item["widest_runway_ft"], width_ft)
            if lighted == 1:
                item["has_lighted_runway"] = 1
            if surface_class == "hard":
                item["has_hard_surface"] = 1
                item["longest_hard_runway_ft"] = max(item["longest_hard_runway_ft"], length_ft)
            elif surface_class == "soft":
                item["has_soft_surface"] = 1
                item["longest_soft_runway_ft"] = max(item["longest_soft_runway_ft"], length_ft)
            elif surface_class == "water":
                item["has_water_runway"] = 1
        else:
            if surface_class == "water":
                item["has_water_runway"] = 1

    with conn:
        conn.execute("DELETE FROM airport_runway_summary_latest")
        for airport, item in summary.items():
            all_closed = 1 if item["runway_count"] > 0 and item["active_runway_count"] == 0 else 0
            conn.execute(
                """
                INSERT INTO airport_runway_summary_latest (
                    airport, ts, runway_count, active_runway_count, longest_runway_ft, longest_hard_runway_ft,
                    longest_soft_runway_ft, widest_runway_ft, has_hard_surface, has_soft_surface,
                    has_water_runway, has_lighted_runway, all_runways_closed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    airport,
                    ts,
                    item["runway_count"],
                    item["active_runway_count"],
                    item["longest_runway_ft"],
                    item["longest_hard_runway_ft"],
                    item["longest_soft_runway_ft"],
                    item["widest_runway_ft"],
                    item["has_hard_surface"],
                    item["has_soft_surface"],
                    item["has_water_runway"],
                    item["has_lighted_runway"],
                    all_closed,
                ),
            )
    LOGGER.info("runway summary refresh complete: rows=%s", len(summary))
    return len(summary)


def refresh_aircraft_suitability(conn: sqlite3.Connection) -> int:
    ts = utc_now_iso()
    airports = conn.execute(
        """
        SELECT airport FROM airport_runway_summary_latest
        UNION
        SELECT icao AS airport FROM airport_reference_latest
        """
    ).fetchall()

    rows_written = 0
    with conn:
        conn.execute("DELETE FROM airport_aircraft_suitability_latest")
        for row in airports:
            airport = (row["airport"] or "").strip().upper()
            if not airport:
                continue

            ref = conn.execute(
                "SELECT type FROM airport_reference_latest WHERE icao = ?",
                (airport,),
            ).fetchone()
            summary = conn.execute(
                """
                SELECT active_runway_count, longest_runway_ft, longest_hard_runway_ft, longest_soft_runway_ft
                FROM airport_runway_summary_latest
                WHERE airport = ?
                """,
                (airport,),
            ).fetchone()

            airport_type = (ref["type"] if ref and ref["type"] else "").strip().lower() or None
            inactive_type = airport_type in {"closed", "heliport", "seaplane_base"}
            active_runway_count = to_int(summary["active_runway_count"]) if summary else 0
            best_runway = to_int(summary["longest_runway_ft"]) if summary else 0
            best_hard = to_int(summary["longest_hard_runway_ft"]) if summary else 0
            best_soft = to_int(summary["longest_soft_runway_ft"]) if summary else 0

            suitable_airliner = int(
                (not inactive_type)
                and airport_type in {"medium_airport", "large_airport"}
                and best_hard >= 6000
            )
            suitable_regional = int(
                (not inactive_type)
                and airport_type in {"small_airport", "medium_airport", "large_airport"}
                and best_hard >= 4500
            )
            suitable_turboprop = int(
                (not inactive_type)
                and (active_runway_count or 0) > 0
                and (best_runway or 0) >= 3000
            )
            suitable_ga = int(
                (not inactive_type)
                and (active_runway_count or 0) > 0
                and (best_runway or 0) >= 1800
            )
            suitable_bizjet = int(
                (not inactive_type)
                and airport_type in {"small_airport", "medium_airport", "large_airport"}
                and best_hard >= 5000
            )

            notes = (
                f"type={airport_type or 'unknown'};"
                f"best={best_runway or 0};"
                f"hard={best_hard or 0};"
                f"soft={best_soft or 0}"
            )
            conn.execute(
                """
                INSERT INTO airport_aircraft_suitability_latest (
                    airport, ts,
                    suitable_airliner_jet, suitable_regional_jet, suitable_turboprop,
                    suitable_ga_piston, suitable_business_jet,
                    best_runway_ft, best_hard_runway_ft, best_soft_runway_ft,
                    airport_type, suitability_notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    airport,
                    ts,
                    suitable_airliner,
                    suitable_regional,
                    suitable_turboprop,
                    suitable_ga,
                    suitable_bizjet,
                    best_runway or 0,
                    best_hard or 0,
                    best_soft or 0,
                    airport_type,
                    notes,
                ),
            )
            rows_written += 1

    LOGGER.info("aircraft suitability refresh complete: rows=%s", rows_written)
    return rows_written


def refresh_runway_enrichment(conn: sqlite3.Connection, runways_path: Path) -> tuple[int, int, int]:
    ingested = ingest_runways_from_disk(conn, runways_path)
    summary_rows = refresh_runway_summary(conn)
    suitability_rows = refresh_aircraft_suitability(conn)
    return ingested, summary_rows, suitability_rows
