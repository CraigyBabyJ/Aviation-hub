from __future__ import annotations

import argparse
import json
import logging
import math
import sqlite3
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from db import DB_PATH
from util import configure_logging, utc_now_iso

LOGGER = logging.getLogger("aviation_hub.widget")
WIDGET_PATH = "/widgets/current-spicy-airports"


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


def _spicy_rank(row: sqlite3.Row, day_state: str) -> float:
    score = float(row["overall_score"] or 0.0)
    score += 2.0 if int(row["has_thunderstorm"] or 0) == 1 else 0.0
    score += 1.5 if int(row["has_snow"] or 0) == 1 else 0.0
    score += 1.0 if int(row["is_gusty"] or 0) == 1 else 0.0
    score += 1.0 if int(row["is_low_visibility"] or 0) == 1 else 0.0
    score += 0.5 if int(row["is_low_ceiling"] or 0) == 1 else 0.0
    if day_state == "day":
        score += 2.0
    elif day_state == "twilight":
        score += 0.5
    else:
        score -= 3.0
    return round(score, 3)


def _load_candidates(conn: sqlite3.Connection, suitability_field: str) -> list[dict[str, Any]]:
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
        result.append(
            {
                "airport": row["airport"],
                "name": row["name"],
                "country": row["country"],
                "region": row["region"],
                "overall_score": float(row["overall_score"] or 0.0),
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
                "spicy_rank": _spicy_rank(row, day_state),
            }
        )
    return result


def _pick_featured(candidates: list[dict[str, Any]], threshold: float) -> dict[str, Any] | None:
    if not candidates:
        return None
    qualified = [c for c in candidates if float(c["overall_score"]) >= threshold]
    source = qualified if qualified else candidates
    source.sort(key=lambda c: (float(c["spicy_rank"]), float(c["overall_score"])), reverse=True)
    return source[0]


def build_spicy_widget_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    airliner_candidates = _load_candidates(conn, "suitable_airliner_jet")
    ga_candidates = _load_candidates(conn, "suitable_ga_piston")
    return {
        "generated_at": utc_now_iso(),
        "airliner": _pick_featured(airliner_candidates, threshold=8.0),
        "ga": _pick_featured(ga_candidates, threshold=6.0),
    }


class WidgetHandler(BaseHTTPRequestHandler):
    db_path: Path = DB_PATH

    def do_GET(self) -> None:  # noqa: N802
        if self.path != WIDGET_PATH:
            self._send_json(HTTPStatus.NOT_FOUND, {"error": "not_found"})
            return

        response = {"generated_at": utc_now_iso(), "airliner": None, "ga": None}
        try:
            with _open_readonly_connection(self.db_path) as conn:
                payload = build_spicy_widget_payload(conn)
                response.update(payload)
            self._send_json(HTTPStatus.OK, response)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("widget request failed: %s", exc)
            self._send_json(HTTPStatus.INTERNAL_SERVER_ERROR, response)

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


def main() -> int:
    args = parse_args()
    configure_logging()
    handler = WidgetHandler
    handler.db_path = args.db
    server = ThreadingHTTPServer((args.host, args.port), handler)
    LOGGER.info("widget server listening on %s:%s", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        LOGGER.info("widget server shutdown requested")
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
