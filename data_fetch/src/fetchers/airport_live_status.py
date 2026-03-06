from __future__ import annotations

import logging
import sqlite3

from util import extract_airport_from_callsign, to_int, utc_now_iso

LOGGER = logging.getLogger("aviation_hub.airport_live")


def refresh_airport_live_status(conn: sqlite3.Connection) -> int:
    ts = utc_now_iso()
    LOGGER.info("airport_live_status refresh started")

    atc_rows = conn.execute(
        """
        SELECT callsign
        FROM vatsim_controllers_latest
        WHERE facility IS NOT NULL AND facility > 0
        """
    ).fetchall()
    atc_by_airport: dict[str, int] = {}
    for row in atc_rows:
        airport = extract_airport_from_callsign(row["callsign"])
        if not airport:
            continue
        atc_by_airport[airport] = atc_by_airport.get(airport, 0) + 1

    atis_rows = conn.execute(
        """
        SELECT airport, callsign, frequency, last_updated
        FROM vatsim_atis_latest
        WHERE airport IS NOT NULL AND airport != ''
        ORDER BY airport ASC, last_updated DESC
        """
    ).fetchall()
    atis_by_airport: dict[str, tuple[str | None, str | None]] = {}
    for row in atis_rows:
        airport = (row["airport"] or "").strip().upper()
        if not airport or airport in atis_by_airport:
            continue
        atis_by_airport[airport] = (row["callsign"], row["frequency"])

    airports = conn.execute(
        """
        SELECT airport FROM airport_weather_flags_latest
        UNION
        SELECT airport FROM airport_weather_score_latest
        UNION
        SELECT icao AS airport FROM airport_reference_latest
        UNION
        SELECT airport FROM vatsim_atis_latest WHERE airport IS NOT NULL AND airport != ''
        """
    ).fetchall()

    rows_written = 0
    with conn:
        for row in airports:
            airport = (row["airport"] or "").strip().upper()
            if not airport:
                continue

            ref = conn.execute(
                """
                SELECT name, country, region, continent, municipality, latitude_deg, longitude_deg, type
                FROM airport_reference_latest
                WHERE icao = ?
                """,
                (airport,),
            ).fetchone()
            flags = conn.execute(
                """
                SELECT
                    ts, raw_metar, raw_taf, has_snow, has_rain, has_thunderstorm, has_freezing_precip,
                    has_fog, has_mist, has_haze, has_dust_sand, has_showers, has_squalls,
                    is_gusty, is_low_visibility, is_low_ceiling, wx_summary
                FROM airport_weather_flags_latest
                WHERE airport = ?
                """,
                (airport,),
            ).fetchone()
            score = conn.execute(
                """
                SELECT
                    ts, overall_score, challenge_level, flight_category,
                    wind_dir_degrees, wind_speed_kt, wind_gust_kt, visibility_meters, ceiling_ft_agl
                FROM airport_weather_score_latest
                WHERE airport = ?
                """,
                (airport,),
            ).fetchone()

            atc_count = atc_by_airport.get(airport, 0)
            atis = atis_by_airport.get(airport)
            atis_callsign = atis[0] if atis else None
            atis_frequency = atis[1] if atis else None

            live_ts = (
                (score["ts"] if score and score["ts"] else None)
                or (flags["ts"] if flags and flags["ts"] else None)
                or ts
            )

            conn.execute(
                """
                INSERT INTO airport_live_status_latest (
                    airport, ts, name, country, region, continent, municipality, latitude_deg, longitude_deg, type,
                    has_atc, controller_count, has_atis, atis_callsign, atis_frequency,
                    has_snow, has_rain, has_thunderstorm, has_freezing_precip, has_fog, has_mist, has_haze,
                    has_dust_sand, has_showers, has_squalls, is_gusty, is_low_visibility, is_low_ceiling,
                    overall_score, challenge_level, flight_category, wind_dir_degrees, wind_speed_kt,
                    wind_gust_kt, visibility_meters, ceiling_ft_agl, raw_metar, raw_taf, wx_summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(airport)
                DO UPDATE SET
                    ts = excluded.ts,
                    name = excluded.name,
                    country = excluded.country,
                    region = excluded.region,
                    continent = excluded.continent,
                    municipality = excluded.municipality,
                    latitude_deg = excluded.latitude_deg,
                    longitude_deg = excluded.longitude_deg,
                    type = excluded.type,
                    has_atc = excluded.has_atc,
                    controller_count = excluded.controller_count,
                    has_atis = excluded.has_atis,
                    atis_callsign = excluded.atis_callsign,
                    atis_frequency = excluded.atis_frequency,
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
                    is_gusty = excluded.is_gusty,
                    is_low_visibility = excluded.is_low_visibility,
                    is_low_ceiling = excluded.is_low_ceiling,
                    overall_score = excluded.overall_score,
                    challenge_level = excluded.challenge_level,
                    flight_category = excluded.flight_category,
                    wind_dir_degrees = excluded.wind_dir_degrees,
                    wind_speed_kt = excluded.wind_speed_kt,
                    wind_gust_kt = excluded.wind_gust_kt,
                    visibility_meters = excluded.visibility_meters,
                    ceiling_ft_agl = excluded.ceiling_ft_agl,
                    raw_metar = excluded.raw_metar,
                    raw_taf = excluded.raw_taf,
                    wx_summary = excluded.wx_summary
                """,
                (
                    airport,
                    live_ts,
                    ref["name"] if ref else None,
                    ref["country"] if ref else None,
                    ref["region"] if ref else None,
                    ref["continent"] if ref else None,
                    ref["municipality"] if ref else None,
                    ref["latitude_deg"] if ref else None,
                    ref["longitude_deg"] if ref else None,
                    ref["type"] if ref else None,
                    1 if atc_count > 0 else 0,
                    atc_count,
                    1 if atis else 0,
                    atis_callsign,
                    atis_frequency,
                    to_int(flags["has_snow"]) if flags else 0,
                    to_int(flags["has_rain"]) if flags else 0,
                    to_int(flags["has_thunderstorm"]) if flags else 0,
                    to_int(flags["has_freezing_precip"]) if flags else 0,
                    to_int(flags["has_fog"]) if flags else 0,
                    to_int(flags["has_mist"]) if flags else 0,
                    to_int(flags["has_haze"]) if flags else 0,
                    to_int(flags["has_dust_sand"]) if flags else 0,
                    to_int(flags["has_showers"]) if flags else 0,
                    to_int(flags["has_squalls"]) if flags else 0,
                    to_int(flags["is_gusty"]) if flags else 0,
                    to_int(flags["is_low_visibility"]) if flags else 0,
                    to_int(flags["is_low_ceiling"]) if flags else 0,
                    score["overall_score"] if score else 0.0,
                    score["challenge_level"] if score else None,
                    score["flight_category"] if score else None,
                    to_int(score["wind_dir_degrees"]) if score else None,
                    to_int(score["wind_speed_kt"]) if score else None,
                    to_int(score["wind_gust_kt"]) if score else None,
                    to_int(score["visibility_meters"]) if score else None,
                    to_int(score["ceiling_ft_agl"]) if score else None,
                    flags["raw_metar"] if flags else None,
                    flags["raw_taf"] if flags else None,
                    flags["wx_summary"] if flags else None,
                ),
            )
            rows_written += 1

    LOGGER.info("airport_live_status refresh complete: rows=%s", rows_written)
    return rows_written
