from __future__ import annotations

import argparse
import json
import logging
import math
import re
import sqlite3
import time
from datetime import datetime, timezone
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
HTTP_ROUTES = {
    "current_spicy_airports": WIDGET_PATH,
    "weather_current": WEATHER_CURRENT_PATH,
}
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
        params = parse_qs(query)
        icao = (params.get("icao", [""])[0] or "").strip().upper()
        if not icao:
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "icao_required"})
            return
        if len(icao) != 4 or not icao.isalnum():
            self._send_json(HTTPStatus.BAD_REQUEST, {"error": "invalid_icao"})
            return

        with _open_readonly_connection(self.db_path) as conn:
            payload = build_current_weather_payload(conn, icao)

        if payload is None:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "metar_not_found", "icao": icao})
            return
        self._send_json(HTTPStatus.OK, payload)

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
