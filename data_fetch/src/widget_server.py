from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread
from urllib.parse import parse_qs, urlparse
from typing import Any

from db import DB_PATH
from util import configure_logging, utc_now_iso

LOGGER = logging.getLogger("aviation_hub.widget")
WIDGET_PATH = "/widgets/current-spicy-airports"
WEATHER_CURRENT_PATH = "/api/weather/current"
METAR_PATH = "/api/metar"
TAF_PATH = "/api/taf"
STATION_PATH = "/api/station"
ATIS_PATH = "/api/atis"
AIRPORT_STATUS_PATH = "/api/airport/status"
AIRPORT_VATSIM_PATH = "/api/airport/vatsim"
AIRPORT_SUMMARY_PATH = "/api/airport/summary"
AIRPORTS_UPCOMING_PATH = "/api/airports/upcoming"
AIRPORTS_RANKED_PATH = "/api/airports/ranked"
VATSIM_AIRPORT_PATH = "/api/vatsim/airport"
VATSIM_EVENTS_PATH = "/api/vatsim/events"
VATSIM_BOOKINGS_PATH = "/api/vatsim/bookings"
VATSIM_INBOUNDS_PATH = "/api/vatsim/inbounds"
VATSIM_LOOKUP_PATH = "/api/vatsim/lookup"
AIRPORT_BRIEF_PATH = "/api/airport/brief"
HTTP_ROUTES = {
    "current_spicy_airports": WIDGET_PATH,
    "weather_current": WEATHER_CURRENT_PATH,
    "metar": METAR_PATH,
    "taf": TAF_PATH,
    "station": STATION_PATH,
    "atis": ATIS_PATH,
    "airport_status": AIRPORT_STATUS_PATH,
    "airport_vatsim": AIRPORT_VATSIM_PATH,
    "airport_summary": AIRPORT_SUMMARY_PATH,
    "airports_upcoming": AIRPORTS_UPCOMING_PATH,
    "airports_ranked": AIRPORTS_RANKED_PATH,
    "vatsim_airport": VATSIM_AIRPORT_PATH,
    "vatsim_events": VATSIM_EVENTS_PATH,
    "vatsim_bookings": VATSIM_BOOKINGS_PATH,
    "vatsim_inbounds": VATSIM_INBOUNDS_PATH,
    "vatsim_lookup": VATSIM_LOOKUP_PATH,
    "airport_brief": AIRPORT_BRIEF_PATH,
}

# VATSIM controller facility codes (see VATSIM API / network documentation).
_FACILITY_LABELS: dict[int, str] = {
    0: "OBS",
    1: "FSS",
    2: "DEL",
    3: "GND",
    4: "TWR",
    5: "APP",
    6: "CTR",
}


def _facility_label(facility: object) -> str:
    if facility is None or facility == "":
        return "ATC"
    try:
        key = int(facility)
    except (TypeError, ValueError):
        return "ATC"
    return _FACILITY_LABELS.get(key, f"Facility {key}")
_METAR_CLOUD_LAYER_RE = re.compile(
    r"(?<!\S)(FEW|SCT|BKN|OVC|VV|///)(\d{3}|///)(CB|TCU|///)?(?!\S)"
)
_ATIS_RUNWAY_RE = re.compile(r"\b(?:RUNWAY(?:S)?(?: IN USE)?|ARRIVAL RUNWAY|DEPARTURE RUNWAY)\s+([0-9]{2}[LRC]?)(?:\s*(?:,|/|AND)\s*([0-9]{2}[LRC]?))*")
_RUNWAY_TOKEN_RE = re.compile(r"\b([0-9]{2}[LRC]?)\b")


def _open_readonly_connection(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000;")
    return conn


def _solar_elevation(lat_deg: float, lon_deg: float, now_utc: datetime) -> float:
    day_of_year = now_utc.timetuple().tm_yday
    hour = now_utc.hour + (now_utc.minute / 60.0) + (now_utc.second / 3600.0)
    gamma = 2.0 * math.pi / 365.0 * (day_of_year - 1 + (hour - 12.0) / 24.0)

    decl = (
        0.006918
        - 0.399912 * math.cos(gamma)
        + 0.070257 * math.sin(gamma)
        - 0.006758 * math.cos(2.0 * gamma)
        + 0.000907 * math.sin(2.0 * gamma)
        - 0.002697 * math.cos(3.0 * gamma)
        + 0.00148 * math.sin(3.0 * gamma)
    )
    eqtime = 229.18 * (
        0.000075
        + 0.001868 * math.cos(gamma)
        - 0.032077 * math.sin(gamma)
        - 0.014615 * math.cos(2.0 * gamma)
        - 0.040849 * math.sin(2.0 * gamma)
    )

    total_minutes = now_utc.hour * 60.0 + now_utc.minute + now_utc.second / 60.0
    true_solar_time = total_minutes + eqtime + (4.0 * lon_deg)
    true_solar_time = true_solar_time % 1440.0
    hour_angle = math.radians((true_solar_time / 4.0) - 180.0)

    lat_rad = math.radians(lat_deg)
    cos_zenith = (
        math.sin(lat_rad) * math.sin(decl)
        + math.cos(lat_rad) * math.cos(decl) * math.cos(hour_angle)
    )
    cos_zenith = max(-1.0, min(1.0, cos_zenith))
    zenith_deg = math.degrees(math.acos(cos_zenith))
    return 90.0 - zenith_deg


def _day_state(lat_deg: float, lon_deg: float, now_utc: datetime) -> tuple[str, int]:
    elevation = _solar_elevation(lat_deg, lon_deg, now_utc)
    if elevation > 0.0:
        return "day", 1
    if elevation > -6.0:
        return "twilight", 0
    return "night", 0


def _condition_flags(row: dict[str, Any]) -> dict[str, int]:
    return {
        "thunderstorm": int(row.get("has_thunderstorm") or 0),
        "snow": int(row.get("has_snow") or 0),
        "gusty": int(row.get("is_gusty") or 0),
        "low_visibility": int(row.get("is_low_visibility") or 0),
        "low_ceiling": int(row.get("is_low_ceiling") or 0),
    }


def _primary_condition(row: dict[str, Any]) -> str:
    flags = _condition_flags(row)
    priority = ("thunderstorm", "snow", "low_visibility", "gusty", "low_ceiling")
    for name in priority:
        if flags[name] == 1:
            return name
    return "mixed"


def _challenge_level_for_widget(overall_score: float) -> str:
    if overall_score < 8.0:
        return "easy"
    if overall_score < 14.0:
        return "moderate"
    if overall_score < 24.0:
        return "spicy"
    if overall_score < 34.0:
        return "extreme"
    return "severe"


def _spicy_rank(row: dict[str, Any], day_state: str, category: str) -> float:
    score = float(row.get("overall_score") or 0.0)
    flags = _condition_flags(row)

    if category == "airliner":
        score += 2.0 * flags["low_visibility"]
        score += 1.6 * flags["low_ceiling"]
        score += 1.4 * flags["snow"]
        score += 1.0 * flags["thunderstorm"]
        score += 0.4 * flags["gusty"]
        flight_category = str(row.get("flight_category") or "")
        if flight_category == "LIFR":
            score += 1.4
        elif flight_category == "IFR":
            score += 1.0
        elif flight_category == "MVFR":
            score += 0.4
    else:
        score += 2.1 * flags["gusty"]
        score += 1.9 * flags["thunderstorm"]
        score += 1.4 * flags["snow"]
        score += 0.8 * flags["low_visibility"]
        score += 0.4 * flags["low_ceiling"]

    if day_state == "day":
        score += 2.0
    elif day_state == "twilight":
        score += 0.5
    else:
        score -= 3.0
    return round(score, 3)


def _load_candidates(
    conn: sqlite3.Connection, suitability_field: str, *, category: str
) -> list[dict[str, Any]]:
    rows = conn.execute(
        f"""
        SELECT
            l.airport, l.name, l.country, l.region,
            l.overall_score, l.challenge_level,
            l.has_snow, l.has_thunderstorm,
            l.is_gusty, l.is_low_visibility, l.is_low_ceiling,
            l.wind_gust_kt, l.visibility_meters, l.flight_category,
            l.latitude_deg, l.longitude_deg
        FROM airport_live_status_latest l
        JOIN airport_aircraft_suitability_latest s ON s.airport = l.airport
        WHERE s.{suitability_field} = 1
          AND l.latitude_deg IS NOT NULL
          AND l.longitude_deg IS NOT NULL
          AND l.overall_score IS NOT NULL
        """
    ).fetchall()

    now_utc = datetime.now(timezone.utc)
    result: list[dict[str, Any]] = []
    for row in rows:
        lat = float(row["latitude_deg"])
        lon = float(row["longitude_deg"])
        day_state, is_daylight = _day_state(lat, lon, now_utc)
        overall_score = float(row["overall_score"] or 0.0)
        base = {
            "airport": row["airport"],
            "name": row["name"],
            "country": row["country"],
            "region": row["region"],
            "overall_score": overall_score,
            "challenge_level": row["challenge_level"],
            "has_snow": int(row["has_snow"] or 0),
            "has_thunderstorm": int(row["has_thunderstorm"] or 0),
            "is_gusty": int(row["is_gusty"] or 0),
            "is_low_visibility": int(row["is_low_visibility"] or 0),
            "is_low_ceiling": int(row["is_low_ceiling"] or 0),
            "wind_gust_kt": row["wind_gust_kt"],
            "visibility_meters": row["visibility_meters"],
            "flight_category": row["flight_category"],
            "day_state": day_state,
            "is_daylight": is_daylight,
        }
        primary_condition = _primary_condition(base)
        result.append(
            {
                **base,
                "challenge_level": _challenge_level_for_widget(overall_score),
                "primary_condition": primary_condition,
                "spicy_rank": _spicy_rank(base, day_state, category),
            }
        )
    return result


def _top_ranked(
    candidates: list[dict[str, Any]],
    *,
    top_band: int = 8,
    avoid_condition: str | None = None,
) -> dict[str, Any] | None:
    if not candidates:
        return None
    ordered = sorted(
        candidates,
        key=lambda c: (float(c["spicy_rank"]), float(c["overall_score"])),
        reverse=True,
    )
    band = ordered[: max(1, min(top_band, len(ordered)))]
    if avoid_condition:
        diversified = [c for c in band if c.get("primary_condition") != avoid_condition]
        if diversified:
            return diversified[0]
    return band[0]


def _pick_featured(
    candidates: list[dict[str, Any]],
    threshold: float,
    *,
    avoid_condition: str | None = None,
) -> tuple[dict[str, Any] | None, str]:
    if not candidates:
        return None, "none"

    thresholded = [c for c in candidates if float(c["overall_score"]) >= threshold]
    stages: list[tuple[str, set[str], list[dict[str, Any]]]] = [
        ("day", {"day"}, thresholded),
        ("day_twilight", {"day", "twilight"}, thresholded),
        ("any", {"day", "twilight", "night"}, thresholded),
        ("day_no_threshold", {"day"}, candidates),
        ("day_twilight_no_threshold", {"day", "twilight"}, candidates),
        ("any_no_threshold", {"day", "twilight", "night"}, candidates),
    ]

    for stage_name, allowed_states, pool in stages:
        filtered = [c for c in pool if c.get("day_state") in allowed_states]
        selected = _top_ranked(filtered, avoid_condition=avoid_condition)
        if selected is not None:
            return selected, stage_name
    return None, "none"


def build_spicy_widget_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    airliner_candidates = _load_candidates(conn, "suitable_airliner_jet", category="airliner")
    ga_candidates = _load_candidates(conn, "suitable_ga_piston", category="ga")
    airliner, airliner_stage = _pick_featured(airliner_candidates, threshold=8.0)
    airliner_primary = airliner.get("primary_condition") if airliner else None
    ga, ga_stage = _pick_featured(
        ga_candidates,
        threshold=6.0,
        avoid_condition=airliner_primary,
    )
    LOGGER.info(
        "spicy widget selected: airliner_stage=%s airliner_primary=%s ga_stage=%s",
        airliner_stage,
        airliner_primary,
        ga_stage,
    )
    return {
        "generated_at": utc_now_iso(),
        "airliner": airliner,
        "ga": ga,
    }


def _format_wind(row: sqlite3.Row) -> dict[str, int | None] | None:
    direction = row["wind_dir_degrees"]
    speed = row["wind_speed_kt"]
    gust = row["wind_gust_kt"]
    if direction is None and speed is None and gust is None:
        return None
    return {
        "dir_degrees": direction,
        "speed_kt": speed,
        "gust_kt": gust,
    }


def _format_visibility(row: sqlite3.Row) -> dict[str, float | int | None] | None:
    meters = row["visibility_meters"]
    statute_mi = row["visibility_statute_mi"]
    if meters is None and statute_mi is None:
        return None
    return {
        "meters": meters,
        "statute_mi": statute_mi,
    }


def _format_pressure(row: sqlite3.Row) -> dict[str, float | int | None] | None:
    altim_in_hg = row["altim_in_hg"]
    altim_hpa = row["altim_hpa"]
    if altim_in_hg is None and altim_hpa is None:
        return None
    return {
        "in_hg": altim_in_hg,
        "hpa": altim_hpa,
    }


def _normalize_precip(row: sqlite3.Row) -> str | None:
    if int(row["has_thunderstorm"] or 0) == 1:
        return "thunderstorm"
    if int(row["has_freezing_precip"] or 0) == 1:
        return "freezing-precip"
    if int(row["has_snow"] or 0) == 1:
        return "snow"
    if int(row["has_rain"] or 0) == 1:
        return "rain"
    if int(row["has_showers"] or 0) == 1:
        return "showers"
    return None


def _extract_report(raw_text: str | None) -> str:
    text = (raw_text or "").upper().strip()
    if " RMK " in text:
        text = text.split(" RMK ", 1)[0]
    return text


def _extract_observed_metar(raw_text: str | None) -> str:
    report = _extract_report(raw_text)
    trend_tokens = (" TEMPO ", " BECMG ", " NOSIG ", " PROB30 ", " PROB40 ")
    cut_index = len(report)
    for token in trend_tokens:
        found = report.find(token)
        if found != -1:
            cut_index = min(cut_index, found)
    return report[:cut_index].strip()


def _parse_cloud_layers(raw_text: str | None) -> list[dict[str, Any]]:
    report = _extract_observed_metar(raw_text)
    layers: list[dict[str, Any]] = []
    for match in _METAR_CLOUD_LAYER_RE.finditer(report):
        coverage_token, base_token, cloud_type_token = match.groups()
        coverage = None if coverage_token == "///" else coverage_token
        cloud_type = None if cloud_type_token in (None, "///") else cloud_type_token
        base_ft_agl = None if base_token == "///" else int(base_token) * 100
        layers.append(
            {
                "coverage": coverage,
                "base_ft_agl": base_ft_agl,
                "cloud_type": cloud_type,
            }
        )
    return layers


def _parse_icao_from_query(query: str) -> tuple[str | None, str | None]:
    params = parse_qs(query)
    icao = (params.get("icao", [""])[0] or "").strip().upper()
    if not icao:
        return None, "icao_required"
    if len(icao) != 4 or not icao.isalnum():
        return None, "invalid_icao"
    return icao, None


def _parse_vatsim_airport_icao_query(query: str) -> tuple[str | None, str | None]:
    """ICAO for VATSIM airport scope: 3–4 alphanumeric (matches station / BML airport codes)."""
    params = parse_qs(query)
    icao = (params.get("icao", [""])[0] or "").strip().upper()
    if not icao:
        return None, "icao_required"
    if len(icao) < 3 or len(icao) > 4 or not icao.isalnum():
        return None, "invalid_icao"
    return icao, None


def _parse_vatsim_lookup_query(query: str) -> tuple[str | None, str | None]:
    """`q` or `callsign`: VATSIM pilot/ATC callsign or 3–4 letter ICAO (letters/digits/underscore)."""
    params = parse_qs(query)
    raw = (params.get("q", [""])[0] or params.get("callsign", [""])[0] or "").strip()
    if not raw:
        return None, "q_required"
    q = raw.upper()
    if len(q) < 2 or len(q) > 20:
        return None, "invalid_query_length"
    if not re.match(r"^[A-Z0-9_]+$", q):
        return None, "invalid_query_chars"
    return q, None


def _parse_optional_icao_from_query(query: str) -> tuple[str | None, str | None]:
    """Optional `icao` for filters; error only if present but invalid."""
    params = parse_qs(query)
    icao = (params.get("icao", [""])[0] or "").strip().upper()
    if not icao:
        return None, None
    if len(icao) != 4 or not icao.isalnum():
        return None, "invalid_icao"
    return icao, None


def _parse_limit_from_query(query: str, *, default: int, max_limit: int) -> int:
    params = parse_qs(query)
    raw = (params.get("limit", [""])[0] or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(1, min(max_limit, value))


def _parse_bookings_limit_from_query(query: str, *, default: int = 15, max_limit: int = 25) -> int:
    params = parse_qs(query)
    raw = (params.get("bookings_limit", [""])[0] or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(1, min(max_limit, value))


def _parse_bool_query(query: str, name: str, *, default: bool = True) -> bool:
    params = parse_qs(query)
    raw = (params.get(name, [""])[0] or "").strip().lower()
    if not raw:
        return default
    if raw in ("1", "true", "yes", "on"):
        return True
    if raw in ("0", "false", "no", "off"):
        return False
    return default


def _parse_hours_from_query(query: str, *, default: int, max_hours: int) -> int:
    params = parse_qs(query)
    raw = (params.get("hours", [""])[0] or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return max(1, min(max_hours, value))


def _utc_window_markers_hours(hours: int) -> tuple[str, str]:
    now = datetime.now(timezone.utc).replace(microsecond=0)
    end = now + timedelta(hours=hours)
    return (
        now.isoformat().replace("+00:00", "Z"),
        end.isoformat().replace("+00:00", "Z"),
    )


def _parse_days_ahead_from_query(
    query: str,
    *,
    default: int = 30,
    max_days: int = 366,
) -> int | None:
    """
    `days` = number of days from now for the start-time upper bound (inclusive window).
    `days=0` or negative means no upper bound (all future-ending events, subject to `limit`).
    """
    params = parse_qs(query)
    raw = (params.get("days", [""])[0] or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    if value <= 0:
        return None
    return max(1, min(max_days, value))


def _normalize_runway_ident(ident: str | None) -> str | None:
    token = (ident or "").strip().upper()
    return token or None


def _extract_runway_tokens(text: str) -> list[str]:
    seen: list[str] = []
    for match in _RUNWAY_TOKEN_RE.finditer(text.upper()):
        token = match.group(1)
        if token not in seen:
            seen.append(token)
    return seen


def _parse_runways_from_atis_text(text: str | None) -> dict[str, list[str]]:
    upper_text = (text or "").upper()
    arrival: list[str] = []
    departure: list[str] = []
    in_use: list[str] = []

    for raw_line in upper_text.splitlines():
        line = " ".join(raw_line.split())
        if not line:
            continue
        if "ARRIVAL RUNWAY" in line:
            for token in _extract_runway_tokens(line):
                if token not in arrival:
                    arrival.append(token)
        elif "DEPARTURE RUNWAY" in line:
            for token in _extract_runway_tokens(line):
                if token not in departure:
                    departure.append(token)
        elif "RUNWAY IN USE" in line:
            for token in _extract_runway_tokens(line):
                if token not in in_use:
                    in_use.append(token)

    if not arrival and in_use:
        arrival = list(in_use)
    if not departure and in_use:
        departure = list(in_use)

    return {
        "arrival": arrival,
        "departure": departure,
        "in_use": in_use,
    }


def build_metar_payload(conn: sqlite3.Connection, icao: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT icao, observation_time, raw_text
        FROM metar_latest
        WHERE icao = ?
        """,
        (icao,),
    ).fetchone()
    if row is None:
        return None
    return {
        "icao": row["icao"],
        "observation_time": row["observation_time"],
        "raw_text": row["raw_text"],
    }


def build_taf_payload(conn: sqlite3.Connection, icao: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT icao, issue_time, valid_from_time, valid_to_time, raw_text
        FROM taf_latest
        WHERE icao = ?
        """,
        (icao,),
    ).fetchone()
    if row is None:
        return None
    return {
        "icao": row["icao"],
        "issue_time": row["issue_time"],
        "valid_from_time": row["valid_from_time"],
        "valid_to_time": row["valid_to_time"],
        "raw_text": row["raw_text"],
    }


def build_station_payload(conn: sqlite3.Connection, icao: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT icao, iata, name, country, region, municipality,
               latitude_deg, longitude_deg, elevation_ft, type
        FROM airport_reference_latest
        WHERE icao = ?
        """,
        (icao,),
    ).fetchone()
    if row is None:
        return None
    return {
        "icao": row["icao"],
        "iata": row["iata"],
        "name": row["name"],
        "country": row["country"],
        "region": row["region"],
        "municipality": row["municipality"],
        "latitude_deg": row["latitude_deg"],
        "longitude_deg": row["longitude_deg"],
        "elevation_ft": row["elevation_ft"],
        "type": row["type"],
    }


def build_atis_payload(conn: sqlite3.Connection, icao: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT callsign, airport, atis_code, frequency, text, last_updated
        FROM vatsim_atis_latest
        WHERE airport = ?
        ORDER BY last_updated DESC
        LIMIT 1
        """,
        (icao,),
    ).fetchone()
    if row is None:
        return None
    return {
        "callsign": row["callsign"],
        "airport": row["airport"],
        "atis_code": row["atis_code"],
        "frequency": row["frequency"],
        "text": row["text"],
        "last_updated": row["last_updated"],
    }


def build_airport_status_payload(conn: sqlite3.Connection, icao: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT airport, controller_count, has_atc, has_atis, atis_callsign, atis_frequency,
               overall_score, challenge_level, flight_category, wx_summary, raw_metar, raw_taf
        FROM airport_live_status_latest
        WHERE airport = ?
        """,
        (icao,),
    ).fetchone()
    if row is None:
        return None

    controller_rows = conn.execute(
        """
        SELECT callsign, name, facility, frequency, rating, server, logon_time, last_updated
        FROM vatsim_controllers_latest
        WHERE facility IS NOT NULL AND facility > 0 AND callsign LIKE ?
        ORDER BY facility ASC, callsign ASC
        """,
        (f"{icao}_%",),
    ).fetchall()
    controllers = [
        {
            "callsign": controller["callsign"],
            "name": controller["name"],
            "facility": controller["facility"],
            "frequency": controller["frequency"],
            "rating": controller["rating"],
            "server": controller["server"],
            "logon_time": controller["logon_time"],
            "last_updated": controller["last_updated"],
        }
        for controller in controller_rows
    ]

    return {
        "airport": row["airport"],
        "controller_count": row["controller_count"],
        "has_atc": bool(row["has_atc"]),
        "has_atis": bool(row["has_atis"]),
        "atis_callsign": row["atis_callsign"],
        "atis_frequency": row["atis_frequency"],
        "overall_score": row["overall_score"],
        "challenge_level": row["challenge_level"],
        "flight_category": row["flight_category"],
        "wx_summary": row["wx_summary"],
        "raw_metar": row["raw_metar"],
        "raw_taf": row["raw_taf"],
        "controllers": controllers,
    }


def build_vatsim_airport_payload(conn: sqlite3.Connection, icao: str) -> dict[str, Any]:
    """
    VATSIM online controllers and ATIS for an airport prefix (e.g. EGCC_TWR).
    Does not require airport_live_status_latest; reads vatsim_*_latest tables only.
    """
    prefix = f"{icao}_%"
    controller_rows = conn.execute(
        """
        SELECT callsign, name, facility, frequency, rating, server, logon_time, last_updated
        FROM vatsim_controllers_latest
        WHERE facility IS NOT NULL AND facility > 0 AND callsign LIKE ?
        ORDER BY facility ASC, callsign ASC
        """,
        (prefix,),
    ).fetchall()
    controllers: list[dict[str, Any]] = []
    for row in controller_rows:
        fac = row["facility"]
        controllers.append(
            {
                "callsign": row["callsign"],
                "name": row["name"],
                "facility": fac,
                "facility_label": _facility_label(fac),
                "frequency": row["frequency"],
                "rating": row["rating"],
                "server": row["server"],
                "logon_time": row["logon_time"],
                "last_updated": row["last_updated"],
            }
        )

    atis_rows = conn.execute(
        """
        SELECT callsign, airport, atis_code, frequency, text, last_updated
        FROM vatsim_atis_latest
        WHERE UPPER(TRIM(airport)) = ?
        ORDER BY last_updated DESC
        """,
        (icao,),
    ).fetchall()
    atis_list = [
        {
            "callsign": row["callsign"],
            "atis_code": row["atis_code"],
            "frequency": row["frequency"],
            "text": row["text"],
            "last_updated": row["last_updated"],
        }
        for row in atis_rows
    ]

    return {
        "icao": icao,
        "source": "vatsim",
        "controller_count": len(controllers),
        "controllers": controllers,
        "atis": atis_list,
        "has_atis": len(atis_list) > 0,
        "fetched_at": utc_now_iso(),
    }


def _vatsim_pilot_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "callsign": row["callsign"],
        "cid": row["cid"],
        "name": row["name"],
        "server": row["server"],
        "pilot_rating": row["pilot_rating"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "altitude": row["altitude"],
        "groundspeed": row["groundspeed"],
        "transponder": row["transponder"],
        "heading": row["heading"],
        "flight_plan_aircraft": row["flight_plan_aircraft"],
        "flight_plan_departure": row["flight_plan_departure"],
        "flight_plan_arrival": row["flight_plan_arrival"],
        "flight_plan_altitude": row["flight_plan_altitude"],
        "flight_plan_rules": row["flight_plan_rules"],
        "logon_time": row["logon_time"],
        "last_updated": row["last_updated"],
    }


def _vatsim_controller_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    fac = row["facility"]
    return {
        "callsign": row["callsign"],
        "cid": row["cid"],
        "name": row["name"],
        "facility": fac,
        "facility_label": _facility_label(fac),
        "frequency": row["frequency"],
        "rating": row["rating"],
        "latitude": row["latitude"],
        "longitude": row["longitude"],
        "altitude": row["altitude"],
        "server": row["server"],
        "visual_range": row["visual_range"],
        "logon_time": row["logon_time"],
        "last_updated": row["last_updated"],
    }


def build_vatsim_lookup_payload(conn: sqlite3.Connection, q: str) -> dict[str, Any]:
    """
    Resolve one VATSIM entity from the hub snapshot: online pilot, online controller, or airport
    (controllers + ATIS for ICAO). Order depends on query shape (underscore → no airport fallback).
    """
    fetched_at = utc_now_iso()

    def row_pilot() -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT
                callsign, cid, name, server, pilot_rating, latitude, longitude, altitude,
                groundspeed, transponder, heading, flight_plan_aircraft, flight_plan_departure,
                flight_plan_arrival, flight_plan_altitude, flight_plan_rules, logon_time, last_updated
            FROM vatsim_pilots_latest
            WHERE UPPER(TRIM(callsign)) = ?
            """,
            (q,),
        ).fetchone()

    def row_controller() -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT
                callsign, cid, name, facility, rating, frequency, latitude, longitude, altitude,
                server, visual_range, logon_time, last_updated
            FROM vatsim_controllers_latest
            WHERE UPPER(TRIM(callsign)) = ?
            """,
            (q,),
        ).fetchone()

    if "_" in q:
        pr = row_pilot()
        if pr is not None:
            return {"kind": "pilot", "pilot": _vatsim_pilot_row_to_dict(pr), "fetched_at": fetched_at}
        cr = row_controller()
        if cr is not None:
            return {"kind": "controller", "controller": _vatsim_controller_row_to_dict(cr), "fetched_at": fetched_at}
        return {"kind": "not_found", "query": q}

    if 3 <= len(q) <= 4 and q.isalnum():
        pr = row_pilot()
        if pr is not None:
            return {"kind": "pilot", "pilot": _vatsim_pilot_row_to_dict(pr), "fetched_at": fetched_at}
        cr = row_controller()
        if cr is not None:
            return {"kind": "controller", "controller": _vatsim_controller_row_to_dict(cr), "fetched_at": fetched_at}
        ap = build_vatsim_airport_payload(conn, q)
        ap["kind"] = "airport"
        return ap

    pr = row_pilot()
    if pr is not None:
        return {"kind": "pilot", "pilot": _vatsim_pilot_row_to_dict(pr), "fetched_at": fetched_at}
    cr = row_controller()
    if cr is not None:
        return {"kind": "controller", "controller": _vatsim_controller_row_to_dict(cr), "fetched_at": fetched_at}
    return {"kind": "not_found", "query": q}


def build_vatsim_inbounds_payload(
    conn: sqlite3.Connection,
    icao: str,
    *,
    limit: int,
) -> dict[str, Any]:
    """
    Pilots currently on VATSIM whose filed flight plan arrival matches the given ICAO.

    Source: `vatsim_pilots_latest` (network snapshot only; disconnecting removes the row).
    Match: case-insensitive trim on `flight_plan_arrival`.
    """
    rows = conn.execute(
        """
        SELECT
            callsign,
            cid,
            name,
            server,
            pilot_rating,
            latitude,
            longitude,
            altitude,
            groundspeed,
            transponder,
            heading,
            flight_plan_aircraft,
            flight_plan_departure,
            flight_plan_arrival,
            flight_plan_altitude,
            flight_plan_rules,
            logon_time,
            last_updated
        FROM vatsim_pilots_latest
        WHERE UPPER(TRIM(COALESCE(flight_plan_arrival, ''))) = ?
        ORDER BY callsign ASC
        LIMIT ?
        """,
        (icao.upper().strip(), limit),
    ).fetchall()

    pilots: list[dict[str, Any]] = []
    for row in rows:
        pilots.append(
            {
                "callsign": row["callsign"],
                "cid": row["cid"],
                "name": row["name"],
                "server": row["server"],
                "pilot_rating": row["pilot_rating"],
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "altitude": row["altitude"],
                "groundspeed": row["groundspeed"],
                "transponder": row["transponder"],
                "heading": row["heading"],
                "flight_plan_aircraft": row["flight_plan_aircraft"],
                "flight_plan_departure": row["flight_plan_departure"],
                "flight_plan_arrival": row["flight_plan_arrival"],
                "flight_plan_altitude": row["flight_plan_altitude"],
                "flight_plan_rules": row["flight_plan_rules"],
                "logon_time": row["logon_time"],
                "last_updated": row["last_updated"],
            }
        )

    return {
        "icao": icao.upper().strip(),
        "source": "vatsim_pilots_latest",
        "generated_at": utc_now_iso(),
        "match_field": "flight_plan_arrival",
        "note": "Online pilots only, from the latest VATSIM network snapshot ingested locally.",
        "count": len(pilots),
        "limit_applied": limit,
        "pilots": pilots,
    }


def build_vatsim_events_list_payload(
    conn: sqlite3.Connection,
    *,
    limit: int,
    days_ahead: int | None = 30,
) -> dict[str, Any]:
    """
    VATSIM published events from `vatsim_events_latest` (ingested snapshot).

    Overlap rule: event is included if it has not ended yet (`end_time_utc >= now`) and, when
    `days_ahead` is set, its start is on or before `now + days_ahead` (events starting later are
    excluded). Use `days_ahead=None` for no upper bound on start time.
    """
    now_marker = utc_now_iso().replace("+00:00", "Z")
    window_end_marker: str | None = None
    if days_ahead is not None:
        window_end = datetime.now(timezone.utc) + timedelta(days=days_ahead)
        window_end_marker = window_end.replace(microsecond=0).isoformat().replace("+00:00", "Z")

    if window_end_marker is None:
        rows = conn.execute(
            """
            SELECT
                event_id,
                name,
                event_type,
                start_time_utc,
                end_time_utc,
                short_description,
                link_url,
                airports_json,
                fetched_at_utc
            FROM vatsim_events_latest
            WHERE end_time_utc >= ?
            ORDER BY start_time_utc ASC
            LIMIT ?
            """,
            (now_marker, limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                event_id,
                name,
                event_type,
                start_time_utc,
                end_time_utc,
                short_description,
                link_url,
                airports_json,
                fetched_at_utc
            FROM vatsim_events_latest
            WHERE end_time_utc >= ?
              AND start_time_utc <= ?
            ORDER BY start_time_utc ASC
            LIMIT ?
            """,
            (now_marker, window_end_marker, limit),
        ).fetchall()
    events: list[dict[str, Any]] = []
    for row in rows:
        events.append(
            {
                "event_id": row["event_id"],
                "name": row["name"],
                "event_type": row["event_type"],
                "start_time_utc": row["start_time_utc"],
                "end_time_utc": row["end_time_utc"],
                "short_description": row["short_description"],
                "link_url": row["link_url"],
                "airports_json": row["airports_json"],
            }
        )
    return {
        "source": "vatsim_events",
        "generated_at": utc_now_iso(),
        "snapshot_fetched_at": rows[0]["fetched_at_utc"] if rows else None,
        "days_ahead": days_ahead,
        "window_start_utc": now_marker,
        "window_end_utc": window_end_marker,
        "count": len(events),
        "events": events,
    }


def build_vatsim_bookings_list_payload(
    conn: sqlite3.Connection,
    *,
    icao: str | None,
    limit: int,
) -> dict[str, Any]:
    """Scheduled ATC bookings from `vatsim_atc_bookings_latest` (advisory; not live coverage)."""
    now_marker = utc_now_iso().replace("+00:00", "Z")
    if icao:
        rows = conn.execute(
            """
            SELECT
                booking_id,
                callsign,
                airport_icao,
                fir_icao,
                position_type,
                starts_at_utc,
                ends_at_utc,
                booking_type,
                controller_cid,
                fetched_at_utc
            FROM vatsim_atc_bookings_latest
            WHERE ends_at_utc >= ?
              AND (
                    UPPER(TRIM(airport_icao)) = ?
                 OR UPPER(callsign) LIKE ?
              )
            ORDER BY starts_at_utc ASC
            LIMIT ?
            """,
            (now_marker, icao, f"{icao}_%", limit),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                booking_id,
                callsign,
                airport_icao,
                fir_icao,
                position_type,
                starts_at_utc,
                ends_at_utc,
                booking_type,
                controller_cid,
                fetched_at_utc
            FROM vatsim_atc_bookings_latest
            WHERE ends_at_utc >= ?
            ORDER BY starts_at_utc ASC
            LIMIT ?
            """,
            (now_marker, limit),
        ).fetchall()

    bookings: list[dict[str, Any]] = []
    for row in rows:
        bookings.append(
            {
                "booking_id": row["booking_id"],
                "callsign": row["callsign"],
                "airport_icao": row["airport_icao"],
                "fir_icao": row["fir_icao"],
                "position_type": row["position_type"],
                "starts_at_utc": row["starts_at_utc"],
                "ends_at_utc": row["ends_at_utc"],
                "booking_type": row["booking_type"],
                "controller_cid": row["controller_cid"],
            }
        )
    return {
        "source": "vatsim_bookings",
        "generated_at": utc_now_iso(),
        "icao_filter": icao,
        "snapshot_fetched_at": rows[0]["fetched_at_utc"] if rows else None,
        "count": len(bookings),
        "bookings": bookings,
    }


def _load_runway_reference(conn: sqlite3.Connection, icao: str, ident: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            airport_icao,
            length_ft,
            width_ft,
            surface,
            surface_class,
            lighted,
            closed,
            le_ident,
            he_ident,
            le_heading_degT,
            he_heading_degT
        FROM airport_runways_latest
        WHERE airport_icao = ?
          AND (? IN (UPPER(COALESCE(le_ident, '')), UPPER(COALESCE(he_ident, ''))))
        LIMIT 1
        """,
        (icao, ident),
    ).fetchone()
    if row is None:
        return None

    heading = None
    if _normalize_runway_ident(row["le_ident"]) == ident:
        heading = row["le_heading_degT"]
    elif _normalize_runway_ident(row["he_ident"]) == ident:
        heading = row["he_heading_degT"]

    return {
        "ident": ident,
        "length_ft": row["length_ft"],
        "width_ft": row["width_ft"],
        "surface": row["surface"],
        "surface_class": row["surface_class"],
        "lighted": bool(row["lighted"]),
        "closed": bool(row["closed"]),
        "heading_degT": heading,
    }


def _build_current_runways_payload(conn: sqlite3.Connection, icao: str) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT callsign, atis_code, frequency, text, last_updated
        FROM vatsim_atis_latest
        WHERE airport = ?
        ORDER BY last_updated DESC
        """,
        (icao,),
    ).fetchall()
    if not rows:
        return None

    arrival: list[str] = []
    departure: list[str] = []
    in_use: list[str] = []
    sources: list[dict[str, Any]] = []

    for row in rows:
        parsed = _parse_runways_from_atis_text(row["text"])
        for ident in parsed["arrival"]:
            if ident not in arrival:
                arrival.append(ident)
        for ident in parsed["departure"]:
            if ident not in departure:
                departure.append(ident)
        for ident in parsed["in_use"]:
            if ident not in in_use:
                in_use.append(ident)
        sources.append(
            {
                "callsign": row["callsign"],
                "atis_code": row["atis_code"],
                "frequency": row["frequency"],
                "last_updated": row["last_updated"],
            }
        )

    return {
        "arrival": [_load_runway_reference(conn, icao, ident) or {"ident": ident} for ident in arrival],
        "departure": [_load_runway_reference(conn, icao, ident) or {"ident": ident} for ident in departure],
        "in_use": [_load_runway_reference(conn, icao, ident) or {"ident": ident} for ident in in_use],
        "sources": sources,
    }


def build_current_weather_payload(conn: sqlite3.Connection, icao: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            m.icao,
            m.raw_text,
            m.observation_time,
            m.temp_c,
            m.wind_dir_degrees,
            m.wind_speed_kt,
            m.wind_gust_kt,
            m.visibility_statute_mi,
            m.altim_in_hg,
            s.visibility_meters,
            s.flight_category,
            f.wx_summary,
            f.has_thunderstorm,
            f.has_snow,
            f.has_rain,
            f.has_fog,
            f.has_mist,
            f.has_showers,
            f.has_freezing_precip,
            CASE
                WHEN m.altim_in_hg IS NOT NULL THEN ROUND(m.altim_in_hg * 33.8639)
                ELSE NULL
            END AS altim_hpa
        FROM metar_latest m
        LEFT JOIN airport_weather_score_latest s ON s.airport = m.icao
        LEFT JOIN airport_weather_flags_latest f ON f.airport = m.icao
        WHERE m.icao = ?
        """,
        (icao,),
    ).fetchone()
    if row is None:
        return None

    return {
        "metar": row["raw_text"],
        "wind": _format_wind(row),
        "temp_c": row["temp_c"],
        "visibility": _format_visibility(row),
        "cloud_layers": _parse_cloud_layers(row["raw_text"]),
        "current_runways": _build_current_runways_payload(conn, icao),
        "flight_category": row["flight_category"],
        "observed_at": row["observation_time"],
        "pressure": _format_pressure(row),
        "precip": _normalize_precip(row),
        "has_thunderstorm": bool(row["has_thunderstorm"]),
        "has_snow": bool(row["has_snow"]),
        "has_rain": bool(row["has_rain"]),
        "has_fog": bool(row["has_fog"]),
        "has_mist": bool(row["has_mist"]),
        "wx_summary": row["wx_summary"],
    }


def build_airport_brief_payload(
    conn: sqlite3.Connection,
    icao: str,
    *,
    bookings_limit: int = 15,
) -> dict[str, Any]:
    """
    One-call snapshot for an airport: current weather (METAR + flags), derived “spicy” live row,
    live VATSIM controllers/ATIS, and upcoming advisory bookings.
    """
    weather = build_current_weather_payload(conn, icao)
    vatsim = build_vatsim_airport_payload(conn, icao)
    status = build_airport_status_payload(conn, icao)
    spicy: dict[str, Any] | None = None
    if status:
        spicy = {
            "overall_score": status["overall_score"],
            "challenge_level": status["challenge_level"],
            "flight_category": status["flight_category"],
            "wx_summary": status["wx_summary"],
            "has_atc": status["has_atc"],
            "controller_count": status["controller_count"],
            "has_atis": status["has_atis"],
        }

    bookings_block: dict[str, Any] = {
        "advisory": "Scheduled bookings only; not guaranteed online coverage.",
        "count": 0,
        "items": [],
        "snapshot_fetched_at": None,
    }
    try:
        bk = build_vatsim_bookings_list_payload(conn, icao=icao, limit=bookings_limit)
        bookings_block["count"] = bk["count"]
        bookings_block["items"] = bk["bookings"]
        bookings_block["snapshot_fetched_at"] = bk.get("snapshot_fetched_at")
    except sqlite3.OperationalError as exc:
        LOGGER.warning("airport brief: bookings unavailable: %s", exc)
        bookings_block["error"] = "bookings_table_unavailable"

    inbounds_block: dict[str, Any] = {}
    try:
        ib = build_vatsim_inbounds_payload(conn, icao, limit=80)
        cap = 35
        plist = ib["pilots"]
        inbounds_block = {
            "count": ib["count"],
            "match_field": ib["match_field"],
            "note": ib["note"],
            "pilots_sample": plist[:cap],
            "truncated": len(plist) > cap,
            "full_list_url_hint": f"/api/vatsim/inbounds?icao={icao}",
        }
    except sqlite3.OperationalError as exc:
        LOGGER.warning("airport brief: inbounds unavailable: %s", exc)
        inbounds_block = {"error": "pilots_table_unavailable"}

    return {
        "icao": icao,
        "generated_at": utc_now_iso(),
        "weather": weather,
        "spicy": spicy,
        "vatsim": vatsim,
        "bookings": bookings_block,
        "inbounds": inbounds_block,
    }


def build_airport_summary_payload(
    conn: sqlite3.Connection,
    icao: str,
    *,
    signal_hours: int = 24,
) -> dict[str, Any]:
    """
    Lightweight row for dashboards: ATC count, weather flags, spicy score, upcoming bookings/events counts.
    `signal_hours` bounds the “upcoming” booking/event overlap window (default 24h).
    """
    icao_u = icao.upper().strip()
    now_m, win_end = _utc_window_markers_hours(signal_hours)
    row = conn.execute(
        "SELECT * FROM airport_live_status_latest WHERE airport = ?",
        (icao_u,),
    ).fetchone()
    has_row = row is not None
    if has_row:
        atc_count = int(row["controller_count"] or 0)
    else:
        r2 = conn.execute(
            """
            SELECT COUNT(*) AS c FROM vatsim_controllers_latest
            WHERE facility IS NOT NULL AND facility > 0 AND callsign LIKE ?
            """,
            (f"{icao_u}_%",),
        ).fetchone()
        atc_count = int(r2["c"] if r2 else 0)

    weather_flags: dict[str, Any] | None = None
    spicy: dict[str, Any] | None = None
    if has_row:
        weather_flags = {
            "has_snow": bool(row["has_snow"]),
            "has_rain": bool(row["has_rain"]),
            "has_thunderstorm": bool(row["has_thunderstorm"]),
            "has_freezing_precip": bool(row["has_freezing_precip"]),
            "has_fog": bool(row["has_fog"]),
            "has_mist": bool(row["has_mist"]),
            "is_low_visibility": bool(row["is_low_visibility"]),
            "is_low_ceiling": bool(row["is_low_ceiling"]),
            "is_gusty": bool(row["is_gusty"]),
        }
        spicy = {
            "overall_score": row["overall_score"],
            "challenge_level": row["challenge_level"],
            "flight_category": row["flight_category"],
        }

    events_count: int | None = 0
    try:
        er = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM vatsim_events_latest AS e
            WHERE e.end_time_utc >= ?
              AND e.start_time_utc <= ?
              AND e.airports_json IS NOT NULL
              AND json_valid(e.airports_json)
              AND json_type(e.airports_json) = 'array'
              AND EXISTS (
                SELECT 1 FROM json_each(e.airports_json) AS je
                WHERE LENGTH(TRIM(je.value)) = 4
                  AND UPPER(TRIM(je.value)) = ?
              )
            """,
            (now_m, win_end, icao_u),
        ).fetchone()
        events_count = int(er["c"] if er else 0)
    except sqlite3.OperationalError:
        events_count = None

    bookings_count: int | None = 0
    try:
        br = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM vatsim_atc_bookings_latest
            WHERE ends_at_utc >= ?
              AND starts_at_utc <= ?
              AND (
                UPPER(TRIM(COALESCE(airport_icao, ''))) = ?
                OR UPPER(callsign) LIKE ?
              )
            """,
            (now_m, win_end, icao_u, f"{icao_u}_%"),
        ).fetchone()
        bookings_count = int(br["c"] if br else 0)
    except sqlite3.OperationalError:
        bookings_count = None

    return {
        "icao": icao_u,
        "generated_at": utc_now_iso(),
        "signal_window_hours": signal_hours,
        "window_start_utc": now_m,
        "window_end_utc": win_end,
        "atc": {
            "controller_count": atc_count,
            "has_atc": atc_count > 0,
            "has_live_status_row": has_row,
        },
        "weather_flags": weather_flags,
        "spicy": spicy,
        "upcoming_signals": {
            "bookings_count": bookings_count,
            "events_count": events_count,
            "has_bookings": (bookings_count > 0) if bookings_count is not None else None,
            "has_events": (events_count > 0) if events_count is not None else None,
        },
    }


def _airport_upcoming_scores(conn: sqlite3.Connection, hours: int) -> tuple[dict[str, dict[str, int]], str, str]:
    """Bookings + events overlap counts per ICAO for [now, now+hours]. Returns (scores, now_m, win_end)."""
    now_m, win_end = _utc_window_markers_hours(hours)
    scores: dict[str, dict[str, int]] = {}

    def ensure(ap: str) -> dict[str, int]:
        ap = ap.strip().upper()
        if ap not in scores:
            scores[ap] = {"bookings": 0, "events": 0}
        return scores[ap]

    try:
        b_rows = conn.execute(
            """
            SELECT airport_icao, callsign
            FROM vatsim_atc_bookings_latest
            WHERE ends_at_utc >= ? AND starts_at_utc <= ?
            """,
            (now_m, win_end),
        ).fetchall()
        for r in b_rows:
            ap_raw = r["airport_icao"]
            if ap_raw and str(ap_raw).strip():
                ap = str(ap_raw).strip().upper()
                if len(ap) == 4 and ap.isalnum():
                    ensure(ap)["bookings"] += 1
                    continue
            cs = (r["callsign"] or "").strip().upper()
            if "_" in cs:
                prefix = cs.split("_", 1)[0]
                if len(prefix) == 4 and prefix.isalpha():
                    ensure(prefix)["bookings"] += 1
    except sqlite3.OperationalError:
        pass

    try:
        e_rows = conn.execute(
            """
            SELECT UPPER(TRIM(je.value)) AS ap, COUNT(DISTINCT e.event_id) AS c
            FROM vatsim_events_latest AS e
            CROSS JOIN json_each(e.airports_json) AS je
            WHERE e.end_time_utc >= ?
              AND e.start_time_utc <= ?
              AND e.airports_json IS NOT NULL
              AND json_valid(e.airports_json)
              AND json_type(e.airports_json) = 'array'
              AND LENGTH(TRIM(je.value)) = 4
            GROUP BY 1
            """,
            (now_m, win_end),
        ).fetchall()
        for er in e_rows:
            ap = (er["ap"] or "").strip().upper()
            if len(ap) == 4 and ap.isalpha():
                ensure(ap)["events"] = int(er["c"] or 0)
    except sqlite3.OperationalError:
        pass

    return scores, now_m, win_end


def build_airports_upcoming_payload(
    conn: sqlite3.Connection,
    *,
    hours: int,
    limit: int,
) -> dict[str, Any]:
    """
    Rank airports likely to be “busy soon” from ingested bookings + events in a forward window.
    `busyness_score` = bookings + distinct_events (heuristic).
    """
    scores, now_m, win_end = _airport_upcoming_scores(conn, hours)
    ranked_list: list[dict[str, Any]] = []
    for ap, parts in scores.items():
        b = parts["bookings"]
        ev = parts["events"]
        likely_staffed = b > 0
        ranked_list.append(
            {
                "airport": ap,
                "bookings": b,
                "events": ev,
                "busyness_score": b + ev,
                "likely_staffed": likely_staffed,
            }
        )
    ranked_list.sort(key=lambda x: (-x["busyness_score"], x["airport"]))
    top = ranked_list[:limit]
    likely_staffed_rows = [r for r in top if bool(r.get("likely_staffed"))]
    event_only_rows = [r for r in top if not bool(r.get("likely_staffed"))]

    return {
        "generated_at": utc_now_iso(),
        "hours": hours,
        "window_start_utc": now_m,
        "window_end_utc": win_end,
        "legend": {
            "bookings": "Scheduled ATC position bookings overlapping the window.",
            "events": "Published VATSIM events that include the airport and overlap the window.",
            "busyness_score": "Simple heuristic: bookings + events.",
            "likely_staffed": "True when bookings > 0 (bookings are advisory, not guaranteed online).",
        },
        "note": "Overlap window is [now, now+hours].",
        "count": len(top),
        "airports": top,
        "groups": {
            "likely_staffed": {"count": len(likely_staffed_rows), "airports": likely_staffed_rows},
            "event_only": {"count": len(event_only_rows), "airports": event_only_rows},
        },
    }


def build_airports_ranked_payload(
    conn: sqlite3.Connection,
    *,
    hours: int,
    limit: int,
    include_unmanned: bool = True,
) -> dict[str, Any]:
    """
    Ordered list: manned (live ATC) vs not, then how “busy” (controllers, filed inbounds, upcoming
    bookings/events, weather challenge score). Heuristic `rank_score` for dashboards.
    """
    upcoming, now_m, win_end = _airport_upcoming_scores(conn, hours)

    live_by_ap: dict[str, sqlite3.Row] = {}
    try:
        for row in conn.execute(
            """
            SELECT airport, controller_count, has_atc, has_atis,
                   overall_score, challenge_level, country
            FROM airport_live_status_latest
            WHERE COALESCE(controller_count, 0) > 0 OR COALESCE(has_atc, 0) != 0
            """
        ):
            live_by_ap[row["airport"]] = row
    except sqlite3.OperationalError:
        pass

    inb: dict[str, int] = {}
    try:
        for row in conn.execute(
            """
            SELECT UPPER(TRIM(flight_plan_arrival)) AS ap, COUNT(*) AS c
            FROM vatsim_pilots_latest
            WHERE COALESCE(TRIM(flight_plan_arrival), '') != ''
            GROUP BY 1
            """
        ):
            ap = (row["ap"] or "").strip().upper()
            if len(ap) == 4 and ap.isalpha():
                inb[ap] = int(row["c"] or 0)
    except sqlite3.OperationalError:
        pass

    all_aps = set(live_by_ap.keys()) | set(upcoming.keys()) | set(inb.keys())
    rows_out: list[dict[str, Any]] = []
    for ap in sorted(all_aps):
        lr = live_by_ap.get(ap)
        cc = int(lr["controller_count"] or 0) if lr is not None else 0
        ha = bool(lr["has_atc"]) if lr is not None else False
        manned = cc > 0 or ha
        if not include_unmanned and not manned:
            continue

        up = upcoming.get(ap, {"bookings": 0, "events": 0})
        b, ev = up["bookings"], up["events"]
        upcoming_score = b + ev
        ib = inb.get(ap, 0)
        os = float(lr["overall_score"] or 0) if lr is not None else 0.0

        rank_score = (
            (100_000.0 if manned else 0.0)
            + 500.0 * min(cc, 20)
            + 50.0 * min(ib, 50)
            + 10.0 * upcoming_score
            + os
        )

        rows_out.append(
            {
                "airport": ap,
                "manned": manned,
                "controller_count": cc,
                "has_atis": bool(lr["has_atis"]) if lr is not None else False,
                "inbounds": ib,
                "upcoming_bookings": b,
                "upcoming_events": ev,
                "upcoming_score": upcoming_score,
                "overall_score": float(lr["overall_score"]) if lr is not None and lr["overall_score"] is not None else None,
                "challenge_level": lr["challenge_level"] if lr is not None else None,
                "country": lr["country"] if lr is not None else None,
                "rank_score": round(rank_score, 2),
            }
        )

    rows_out.sort(key=lambda x: (-x["rank_score"], x["airport"]))
    top = rows_out[:limit]

    return {
        "generated_at": utc_now_iso(),
        "hours": hours,
        "window_start_utc": now_m,
        "window_end_utc": win_end,
        "include_unmanned": include_unmanned,
        "note": (
            "rank_score = 100k if manned + 500*controllers + 50*inbounds + 10*upcoming_score + "
            "weather overall_score; upcoming uses bookings+events in window; bookings advisory."
        ),
        "count": len(top),
        "airports": top,
    }


class WidgetHandler(BaseHTTPRequestHandler):
    db_path: Path = DB_PATH

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            if parsed.path == WIDGET_PATH:
                self._handle_spicy_widget()
                return
            if parsed.path == WEATHER_CURRENT_PATH:
                self._handle_weather_current(parsed.query)
                return
            if parsed.path == METAR_PATH:
                self._handle_metar(parsed.query)
                return
            if parsed.path == TAF_PATH:
                self._handle_taf(parsed.query)
                return
            if parsed.path == STATION_PATH:
                self._handle_station(parsed.query)
                return
            if parsed.path == ATIS_PATH:
                self._handle_atis(parsed.query)
                return
            if parsed.path == AIRPORT_STATUS_PATH:
                self._handle_airport_status(parsed.query)
                return
            if parsed.path == AIRPORT_SUMMARY_PATH:
                self._handle_airport_summary(parsed.query)
                return
            if parsed.path == AIRPORT_VATSIM_PATH:
                self._handle_vatsim_airport(parsed.query)
                return
            if parsed.path == VATSIM_AIRPORT_PATH:
                self._handle_vatsim_airport(parsed.query)
                return
            if parsed.path == AIRPORTS_UPCOMING_PATH:
                self._handle_airports_upcoming(parsed.query)
                return
            if parsed.path == AIRPORTS_RANKED_PATH:
                self._handle_airports_ranked(parsed.query)
                return
            if parsed.path == VATSIM_EVENTS_PATH:
                self._handle_vatsim_events(parsed.query)
                return
            if parsed.path == VATSIM_BOOKINGS_PATH:
                self._handle_vatsim_bookings(parsed.query)
                return
            if parsed.path == VATSIM_INBOUNDS_PATH:
                self._handle_vatsim_inbounds(parsed.query)
                return
            if parsed.path == VATSIM_LOOKUP_PATH:
                self._handle_vatsim_lookup(parsed.query)
                return
            if parsed.path == AIRPORT_BRIEF_PATH:
                self._handle_airport_brief(parsed.query)
                return
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("request failed: %s", exc)
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": "internal_error"})

    def _handle_spicy_widget(self) -> None:
        response = {"generated_at": utc_now_iso(), "airliner": None, "ga": None}
        try:
            with _open_readonly_connection(self.db_path) as conn:
                payload = build_spicy_widget_payload(conn)
                response.update(payload)
            self._send_json(HTTPStatus.OK, response)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("widget request failed: %s", exc)
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, response)

    def _handle_weather_current(self, query: str) -> None:
        icao, error = _parse_icao_from_query(query)
        if error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": error})
            return

        with _open_readonly_connection(self.db_path) as conn:
            payload = build_current_weather_payload(conn, icao)

        if payload is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "metar_not_found", "icao": icao})
            return
        self._send_json(HTTPStatus.OK, payload)

    def _handle_metar(self, query: str) -> None:
        icao, error = _parse_icao_from_query(query)
        if error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": error})
            return
        with _open_readonly_connection(self.db_path) as conn:
            payload = build_metar_payload(conn, icao)
        if payload is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "metar_not_found", "icao": icao})
            return
        self._send_json(HTTPStatus.OK, payload)

    def _handle_taf(self, query: str) -> None:
        icao, error = _parse_icao_from_query(query)
        if error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": error})
            return
        with _open_readonly_connection(self.db_path) as conn:
            payload = build_taf_payload(conn, icao)
        if payload is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "taf_not_found", "icao": icao})
            return
        self._send_json(HTTPStatus.OK, payload)

    def _handle_station(self, query: str) -> None:
        icao, error = _parse_icao_from_query(query)
        if error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": error})
            return
        with _open_readonly_connection(self.db_path) as conn:
            payload = build_station_payload(conn, icao)
        if payload is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "station_not_found", "icao": icao})
            return
        self._send_json(HTTPStatus.OK, payload)

    def _handle_atis(self, query: str) -> None:
        icao, error = _parse_icao_from_query(query)
        if error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": error})
            return
        with _open_readonly_connection(self.db_path) as conn:
            payload = build_atis_payload(conn, icao)
        if payload is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "atis_not_found", "icao": icao})
            return
        self._send_json(HTTPStatus.OK, payload)

    def _handle_airport_status(self, query: str) -> None:
        icao, error = _parse_icao_from_query(query)
        if error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": error})
            return
        with _open_readonly_connection(self.db_path) as conn:
            payload = build_airport_status_payload(conn, icao)
        if payload is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "airport_not_found", "icao": icao})
            return
        self._send_json(HTTPStatus.OK, payload)

    def _handle_airport_summary(self, query: str) -> None:
        icao, error = _parse_icao_from_query(query)
        if error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": error})
            return
        hours = _parse_hours_from_query(query, default=24, max_hours=168)
        try:
            with _open_readonly_connection(self.db_path) as conn:
                payload = build_airport_summary_payload(conn, icao, signal_hours=hours)
            self._send_json(HTTPStatus.OK, payload)
        except sqlite3.OperationalError as exc:
            LOGGER.warning("airport summary API: %s", exc)
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "summary_unavailable", "detail": str(exc)},
            )

    def _handle_airports_upcoming(self, query: str) -> None:
        hours = _parse_hours_from_query(query, default=6, max_hours=168)
        limit = _parse_limit_from_query(query, default=50, max_limit=200)
        try:
            with _open_readonly_connection(self.db_path) as conn:
                payload = build_airports_upcoming_payload(conn, hours=hours, limit=limit)
            self._send_json(HTTPStatus.OK, payload)
        except sqlite3.OperationalError as exc:
            LOGGER.warning("airports upcoming API: %s", exc)
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "upcoming_unavailable", "detail": str(exc)},
            )

    def _handle_airports_ranked(self, query: str) -> None:
        hours = _parse_hours_from_query(query, default=6, max_hours=168)
        limit = _parse_limit_from_query(query, default=50, max_limit=200)
        include_unmanned = _parse_bool_query(query, "include_unmanned", default=True)
        try:
            with _open_readonly_connection(self.db_path) as conn:
                payload = build_airports_ranked_payload(
                    conn,
                    hours=hours,
                    limit=limit,
                    include_unmanned=include_unmanned,
                )
            self._send_json(HTTPStatus.OK, payload)
        except sqlite3.OperationalError as exc:
            LOGGER.warning("airports ranked API: %s", exc)
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "ranked_unavailable", "detail": str(exc)},
            )

    def _handle_vatsim_airport(self, query: str) -> None:
        icao, error = _parse_vatsim_airport_icao_query(query)
        if error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": error})
            return
        with _open_readonly_connection(self.db_path) as conn:
            payload = build_vatsim_airport_payload(conn, icao)
        self._send_json(HTTPStatus.OK, payload)

    def _handle_vatsim_events(self, query: str) -> None:
        days = _parse_days_ahead_from_query(query, default=30, max_days=366)
        limit = _parse_limit_from_query(query, default=100, max_limit=400)
        try:
            with _open_readonly_connection(self.db_path) as conn:
                payload = build_vatsim_events_list_payload(conn, limit=limit, days_ahead=days)
            self._send_json(HTTPStatus.OK, payload)
        except sqlite3.OperationalError as exc:
            LOGGER.warning("vatsim events API: %s", exc)
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "events_table_unavailable", "detail": str(exc)},
            )

    def _handle_vatsim_bookings(self, query: str) -> None:
        icao, err = _parse_optional_icao_from_query(query)
        if err:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": err})
            return
        limit = _parse_limit_from_query(query, default=25, max_limit=100)
        try:
            with _open_readonly_connection(self.db_path) as conn:
                payload = build_vatsim_bookings_list_payload(conn, icao=icao, limit=limit)
            self._send_json(HTTPStatus.OK, payload)
        except sqlite3.OperationalError as exc:
            LOGGER.warning("vatsim bookings API: %s", exc)
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "bookings_table_unavailable", "detail": str(exc)},
            )

    def _handle_vatsim_inbounds(self, query: str) -> None:
        icao, error = _parse_icao_from_query(query)
        if error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": error})
            return
        limit = _parse_limit_from_query(query, default=200, max_limit=500)
        try:
            with _open_readonly_connection(self.db_path) as conn:
                payload = build_vatsim_inbounds_payload(conn, icao, limit=limit)
            self._send_json(HTTPStatus.OK, payload)
        except sqlite3.OperationalError as exc:
            LOGGER.warning("vatsim inbounds API: %s", exc)
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "pilots_table_unavailable", "detail": str(exc)},
            )

    def _handle_vatsim_lookup(self, query: str) -> None:
        q, err = _parse_vatsim_lookup_query(query)
        if err:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": err})
            return
        try:
            with _open_readonly_connection(self.db_path) as conn:
                payload = build_vatsim_lookup_payload(conn, q)
        except sqlite3.OperationalError as exc:
            LOGGER.warning("vatsim lookup API: %s", exc)
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "lookup_unavailable", "detail": str(exc)},
            )
            return
        if payload.get("kind") == "not_found":
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found", "query": q})
            return
        self._send_json(HTTPStatus.OK, payload)

    def _handle_airport_brief(self, query: str) -> None:
        icao, error = _parse_icao_from_query(query)
        if error:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": error})
            return
        bk_limit = _parse_bookings_limit_from_query(query, default=15, max_limit=25)
        try:
            with _open_readonly_connection(self.db_path) as conn:
                payload = build_airport_brief_payload(conn, icao, bookings_limit=bk_limit)
            self._send_json(HTTPStatus.OK, payload)
        except sqlite3.OperationalError as exc:
            LOGGER.warning("airport brief API: %s", exc)
            self._send_json(
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "brief_unavailable", "detail": str(exc)},
            )

    def log_message(self, fmt: str, *args: object) -> None:
        LOGGER.info("widget_http %s", fmt % args)

    def _send_json(self, status: HTTPStatus, body: dict[str, Any]) -> None:
        encoded = json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        self.send_response(status.value)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aviation Hub widget HTTP server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=4010)
    parser.add_argument("--db", type=Path, default=DB_PATH)
    return parser.parse_args()


def start_widget_server(
    *,
    host: str = "0.0.0.0",
    port: int = 4010,
    db_path: Path = DB_PATH,
) -> ThreadingHTTPServer:
    handler = WidgetHandler
    handler.db_path = db_path
    server = ThreadingHTTPServer((host, port), handler)
    thread = Thread(target=server.serve_forever, name="widget-http", daemon=True)
    thread.start()
    LOGGER.info("widget server listening on %s:%s", host, port)
    return server


def main() -> int:
    args = parse_args()
    configure_logging()
    server = start_widget_server(host=args.host, port=args.port, db_path=args.db)
    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        LOGGER.info("widget server shutdown requested")
    finally:
        server.shutdown()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
