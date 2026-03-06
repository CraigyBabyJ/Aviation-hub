from __future__ import annotations

import logging
import re
import sqlite3

from util import sha256_text, to_int, utc_now_iso

LOGGER = logging.getLogger("aviation_hub.weather")

GUSTY_THRESHOLD_KT = 25
LOW_VISIBILITY_THRESHOLD_M = 5000
LOW_CEILING_THRESHOLD_FT = 1000

_VIS_RE = re.compile(r"\b(M?\d+(?:/\d+)?)SM\b")
_CEILING_RE = re.compile(r"\b(?:BKN|OVC|VV)(\d{3})\b")


def _to_int_flag(value: bool) -> int:
    return 1 if value else 0


def _extract_report(raw_text: str | None) -> str:
    text = (raw_text or "").upper().strip()
    if " RMK " in text:
        text = text.split(" RMK ", 1)[0]
    return text


def _parse_visibility_meters(visibility_statute_mi: float | None, raw_text: str | None) -> int | None:
    if visibility_statute_mi is not None:
        return int(round(visibility_statute_mi * 1609.34))

    report = _extract_report(raw_text)
    match = _VIS_RE.search(report)
    if not match:
        return None

    token = match.group(1)
    multiplier = 1.0
    if token.startswith("M"):
        token = token[1:]
        multiplier = 0.5

    if "/" in token:
        num, den = token.split("/", 1)
        try:
            value_sm = float(num) / float(den)
        except (TypeError, ValueError, ZeroDivisionError):
            return None
    else:
        try:
            value_sm = float(token)
        except (TypeError, ValueError):
            return None

    return int(round(value_sm * multiplier * 1609.34))


def _parse_ceiling_ft_agl(raw_text: str | None) -> int | None:
    report = _extract_report(raw_text)
    matches = [int(m.group(1)) * 100 for m in _CEILING_RE.finditer(report)]
    if not matches:
        return None
    return min(matches)


def _flag_patterns(report: str) -> dict[str, int]:
    patterns = {
        "has_snow": r"\b(?:\+|-)?SN\b|\bSHSN\b|\bBLSN\b",
        "has_rain": r"\b(?:\+|-)?RA\b|\bSHRA\b",
        "has_thunderstorm": r"\bTS(?:RA|SN|GR|GS|DZ)?\b|\bVCTS\b",
        "has_freezing_precip": r"\bFZ(?:RA|DZ)\b",
        "has_fog": r"\bFG\b",
        "has_mist": r"\bBR\b",
        "has_haze": r"\bHZ\b",
        "has_dust_sand": r"\bDU\b|\bSA\b",
        "has_showers": r"\bSH(?:RA|SN|GS|GR|DZ)?\b",
        "has_squalls": r"\bSQ\b",
    }
    result: dict[str, int] = {}
    for key, pattern in patterns.items():
        result[key] = _to_int_flag(re.search(pattern, report) is not None)
    return result


def _build_wx_summary(flags: dict[str, int]) -> str:
    parts: list[str] = []
    if flags["has_thunderstorm"]:
        parts.append("thunderstorm")
    if flags["has_snow"]:
        parts.append("snow")
    if flags["has_freezing_precip"]:
        parts.append("freezing-precip")
    if flags["has_rain"]:
        parts.append("rain")
    if flags["has_fog"]:
        parts.append("fog")
    if flags["has_mist"]:
        parts.append("mist")
    if flags["has_haze"]:
        parts.append("haze")
    if flags["has_dust_sand"]:
        parts.append("dust-sand")
    if flags["has_showers"]:
        parts.append("showers")
    if flags["has_squalls"]:
        parts.append("squalls")
    return ",".join(parts)


def _derive_flight_category(visibility_meters: int | None, ceiling_ft_agl: int | None) -> str | None:
    if visibility_meters is None and ceiling_ft_agl is None:
        return None
    vis_sm = (visibility_meters / 1609.34) if visibility_meters is not None else None

    if (ceiling_ft_agl is not None and ceiling_ft_agl < 500) or (vis_sm is not None and vis_sm < 1.0):
        return "LIFR"
    if (ceiling_ft_agl is not None and ceiling_ft_agl < 1000) or (vis_sm is not None and vis_sm < 3.0):
        return "IFR"
    if (ceiling_ft_agl is not None and ceiling_ft_agl <= 3000) or (vis_sm is not None and vis_sm <= 5.0):
        return "MVFR"
    return "VFR"


def recalc_latest_weather(conn: sqlite3.Connection) -> tuple[int, int]:
    rows = conn.execute(
        """
        SELECT
            m.icao AS airport,
            m.observation_time,
            m.raw_text AS raw_metar,
            m.wind_dir_degrees,
            m.wind_speed_kt,
            m.wind_gust_kt,
            m.visibility_statute_mi,
            t.raw_text AS raw_taf
        FROM metar_latest m
        LEFT JOIN taf_latest t ON t.icao = m.icao
        """
    ).fetchall()

    now = utc_now_iso()
    flags_upserted = 0
    scores_upserted = 0

    for row in rows:
        airport = (row["airport"] or "").strip().upper()
        if not airport:
            continue

        raw_metar = row["raw_metar"]
        raw_taf = row["raw_taf"]
        report = _extract_report(raw_metar)
        flags = _flag_patterns(report)

        visibility_m = _parse_visibility_meters(row["visibility_statute_mi"], raw_metar)
        ceiling_ft = _parse_ceiling_ft_agl(raw_metar)
        wind_speed = to_int(row["wind_speed_kt"]) or 0
        wind_gust = to_int(row["wind_gust_kt"]) or 0
        wind_effective = max(wind_speed, wind_gust)

        is_gusty = _to_int_flag(wind_gust >= GUSTY_THRESHOLD_KT)
        is_low_visibility = _to_int_flag(
            visibility_m is not None and visibility_m < LOW_VISIBILITY_THRESHOLD_M
        )
        is_low_ceiling = _to_int_flag(ceiling_ft is not None and ceiling_ft < LOW_CEILING_THRESHOLD_FT)

        wx_summary = _build_wx_summary(flags)
        source_hash = sha256_text(f"{raw_metar or ''}|{raw_taf or ''}")

        conn.execute(
            """
            INSERT INTO airport_weather_flags_latest (
                airport, ts, raw_metar, raw_taf,
                has_snow, has_rain, has_thunderstorm, has_freezing_precip,
                has_fog, has_mist, has_haze, has_dust_sand, has_showers, has_squalls,
                is_low_visibility, is_low_ceiling, is_gusty, wx_summary, source_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(airport)
            DO UPDATE SET
                ts = excluded.ts,
                raw_metar = excluded.raw_metar,
                raw_taf = excluded.raw_taf,
                has_snow = excluded.has_snow,
                has_rain = excluded.has_rain,
                has_thunderstorm = excluded.has_thunderstorm,
                has_freezing_precip = excluded.has_freezing_precip,
                has_fog = excluded.has_fog,
                has_mist = excluded.has_mist,
                has_haze = excluded.has_haze,
                has_dust_sand = excluded.has_dust_sand,
                has_showers = excluded.has_showers,
                has_squalls = excluded.has_squalls,
                is_low_visibility = excluded.is_low_visibility,
                is_low_ceiling = excluded.is_low_ceiling,
                is_gusty = excluded.is_gusty,
                wx_summary = excluded.wx_summary,
                source_hash = excluded.source_hash
            """,
            (
                airport,
                row["observation_time"] or now,
                raw_metar,
                raw_taf,
                flags["has_snow"],
                flags["has_rain"],
                flags["has_thunderstorm"],
                flags["has_freezing_precip"],
                flags["has_fog"],
                flags["has_mist"],
                flags["has_haze"],
                flags["has_dust_sand"],
                flags["has_showers"],
                flags["has_squalls"],
                is_low_visibility,
                is_low_ceiling,
                is_gusty,
                wx_summary,
                source_hash,
            ),
        )
        flags_upserted += 1

        wind_score = (wind_speed * 0.25) + (max(0, wind_gust - wind_speed) * 0.45)
        vis_score = (
            0.0
            if visibility_m is None
            else max(0.0, (LOW_VISIBILITY_THRESHOLD_M - visibility_m) / 500.0)
        )
        ceiling_score = (
            0.0
            if ceiling_ft is None
            else max(0.0, (LOW_CEILING_THRESHOLD_FT - ceiling_ft) / 100.0)
        )
        precip_score = (
            (2.0 if flags["has_rain"] else 0.0)
            + (2.0 if flags["has_showers"] else 0.0)
            + (4.0 if flags["has_snow"] else 0.0)
        )
        convective_score = (8.0 if flags["has_thunderstorm"] else 0.0) + (3.0 if flags["has_squalls"] else 0.0)
        winter_score = (5.0 if flags["has_snow"] else 0.0) + (6.0 if flags["has_freezing_precip"] else 0.0)
        overall = wind_score + vis_score + ceiling_score + precip_score + convective_score + winter_score

        if overall < 6:
            challenge_level = "easy"
        elif overall < 14:
            challenge_level = "moderate"
        elif overall < 24:
            challenge_level = "spicy"
        else:
            challenge_level = "extreme"

        conn.execute(
            """
            INSERT INTO airport_weather_score_latest (
                airport, ts,
                wind_score, vis_score, ceiling_score, precip_score, convective_score, winter_score,
                overall_score, challenge_level,
                wind_dir_degrees, wind_speed_kt, wind_gust_kt, visibility_meters, ceiling_ft_agl, flight_category
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(airport)
            DO UPDATE SET
                ts = excluded.ts,
                wind_score = excluded.wind_score,
                vis_score = excluded.vis_score,
                ceiling_score = excluded.ceiling_score,
                precip_score = excluded.precip_score,
                convective_score = excluded.convective_score,
                winter_score = excluded.winter_score,
                overall_score = excluded.overall_score,
                challenge_level = excluded.challenge_level,
                wind_dir_degrees = excluded.wind_dir_degrees,
                wind_speed_kt = excluded.wind_speed_kt,
                wind_gust_kt = excluded.wind_gust_kt,
                visibility_meters = excluded.visibility_meters,
                ceiling_ft_agl = excluded.ceiling_ft_agl,
                flight_category = excluded.flight_category
            """,
            (
                airport,
                row["observation_time"] or now,
                wind_score,
                vis_score,
                ceiling_score,
                precip_score,
                convective_score,
                winter_score,
                overall,
                challenge_level,
                to_int(row["wind_dir_degrees"]),
                wind_speed,
                to_int(row["wind_gust_kt"]),
                visibility_m,
                ceiling_ft,
                _derive_flight_category(visibility_m, ceiling_ft),
            ),
        )
        scores_upserted += 1

    LOGGER.info(
        "weather derivation complete: flags_upserted=%s score_upserted=%s",
        flags_upserted,
        scores_upserted,
    )
    return flags_upserted, scores_upserted
