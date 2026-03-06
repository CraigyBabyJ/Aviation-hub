from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "aviation_hub.db"


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = FULL;")
    conn.execute("PRAGMA wal_autocheckpoint = 1000;")
    conn.execute("PRAGMA busy_timeout = 5000;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS feed_state (
            feed_name TEXT PRIMARY KEY,
            last_update TEXT,
            last_fetch TEXT,
            last_success TEXT,
            last_error TEXT,
            last_error_at TEXT
        );

        CREATE TABLE IF NOT EXISTS vatsim_controllers_latest (
            callsign TEXT PRIMARY KEY,
            cid INTEGER,
            name TEXT,
            facility INTEGER,
            rating INTEGER,
            frequency TEXT,
            latitude REAL,
            longitude REAL,
            altitude INTEGER,
            server TEXT,
            visual_range INTEGER,
            logon_time TEXT,
            last_updated TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_vatsim_controllers_facility
            ON vatsim_controllers_latest (facility);
        CREATE INDEX IF NOT EXISTS idx_vatsim_controllers_last_updated
            ON vatsim_controllers_latest (last_updated);
        CREATE INDEX IF NOT EXISTS idx_vatsim_controllers_lat_lon
            ON vatsim_controllers_latest (latitude, longitude);

        CREATE TABLE IF NOT EXISTS vatsim_pilots_latest (
            callsign TEXT PRIMARY KEY,
            cid INTEGER,
            name TEXT,
            server TEXT,
            pilot_rating INTEGER,
            latitude REAL,
            longitude REAL,
            altitude INTEGER,
            groundspeed INTEGER,
            transponder TEXT,
            heading INTEGER,
            qnh_i_hg REAL,
            qnh_mb INTEGER,
            flight_plan_aircraft TEXT,
            flight_plan_departure TEXT,
            flight_plan_arrival TEXT,
            flight_plan_altitude TEXT,
            flight_plan_rules TEXT,
            logon_time TEXT,
            last_updated TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_vatsim_pilots_rating
            ON vatsim_pilots_latest (pilot_rating);
        CREATE INDEX IF NOT EXISTS idx_vatsim_pilots_last_updated
            ON vatsim_pilots_latest (last_updated);
        CREATE INDEX IF NOT EXISTS idx_vatsim_pilots_lat_lon
            ON vatsim_pilots_latest (latitude, longitude);

        CREATE TABLE IF NOT EXISTS vatsim_atis_latest (
            callsign TEXT PRIMARY KEY,
            airport TEXT,
            atis_code TEXT,
            frequency TEXT,
            text TEXT,
            text_hash TEXT,
            last_updated TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_vatsim_atis_airport
            ON vatsim_atis_latest (airport);
        CREATE INDEX IF NOT EXISTS idx_vatsim_atis_last_updated
            ON vatsim_atis_latest (last_updated);

        CREATE TABLE IF NOT EXISTS metar_latest (
            icao TEXT PRIMARY KEY,
            observation_time TEXT,
            raw_text TEXT,
            latitude REAL,
            longitude REAL,
            temp_c REAL,
            dewpoint_c REAL,
            wind_dir_degrees INTEGER,
            wind_speed_kt INTEGER,
            wind_gust_kt INTEGER,
            visibility_statute_mi REAL,
            altim_in_hg REAL
        );
        CREATE INDEX IF NOT EXISTS idx_metar_wind_speed
            ON metar_latest (wind_speed_kt);
        CREATE INDEX IF NOT EXISTS idx_metar_wind_gust
            ON metar_latest (wind_gust_kt);
        CREATE INDEX IF NOT EXISTS idx_metar_temp_c
            ON metar_latest (temp_c);
        CREATE INDEX IF NOT EXISTS idx_metar_observation_time
            ON metar_latest (observation_time);

        CREATE TABLE IF NOT EXISTS taf_latest (
            icao TEXT PRIMARY KEY,
            issue_time TEXT,
            bulletin_time TEXT,
            valid_from_time TEXT,
            valid_to_time TEXT,
            raw_text TEXT,
            latitude REAL,
            longitude REAL
        );
        CREATE INDEX IF NOT EXISTS idx_taf_issue_time
            ON taf_latest (issue_time);
        CREATE INDEX IF NOT EXISTS idx_taf_valid_to_time
            ON taf_latest (valid_to_time);

        CREATE TABLE IF NOT EXISTS airport_reference_latest (
            icao TEXT PRIMARY KEY,
            iata TEXT,
            name TEXT,
            latitude_deg REAL,
            longitude_deg REAL,
            elevation_ft INTEGER,
            country TEXT,
            region TEXT,
            municipality TEXT,
            continent TEXT,
            type TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_airport_ref_country
            ON airport_reference_latest (country);
        CREATE INDEX IF NOT EXISTS idx_airport_ref_region
            ON airport_reference_latest (region);
        CREATE INDEX IF NOT EXISTS idx_airport_ref_continent
            ON airport_reference_latest (continent);
        CREATE INDEX IF NOT EXISTS idx_airport_ref_lat_lon
            ON airport_reference_latest (latitude_deg, longitude_deg);

        CREATE TABLE IF NOT EXISTS airport_weather_flags_latest (
            airport TEXT PRIMARY KEY,
            ts TEXT NOT NULL,
            raw_metar TEXT,
            raw_taf TEXT,
            has_snow INTEGER NOT NULL DEFAULT 0,
            has_rain INTEGER NOT NULL DEFAULT 0,
            has_thunderstorm INTEGER NOT NULL DEFAULT 0,
            has_freezing_precip INTEGER NOT NULL DEFAULT 0,
            has_fog INTEGER NOT NULL DEFAULT 0,
            has_mist INTEGER NOT NULL DEFAULT 0,
            has_haze INTEGER NOT NULL DEFAULT 0,
            has_dust_sand INTEGER NOT NULL DEFAULT 0,
            has_showers INTEGER NOT NULL DEFAULT 0,
            has_squalls INTEGER NOT NULL DEFAULT 0,
            is_low_visibility INTEGER NOT NULL DEFAULT 0,
            is_low_ceiling INTEGER NOT NULL DEFAULT 0,
            is_gusty INTEGER NOT NULL DEFAULT 0,
            wx_summary TEXT,
            source_hash TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_weather_flags_snow
            ON airport_weather_flags_latest (has_snow);
        CREATE INDEX IF NOT EXISTS idx_weather_flags_thunderstorm
            ON airport_weather_flags_latest (has_thunderstorm);
        CREATE INDEX IF NOT EXISTS idx_weather_flags_airport_ts
            ON airport_weather_flags_latest (airport, ts);

        CREATE TABLE IF NOT EXISTS airport_weather_score_latest (
            airport TEXT PRIMARY KEY,
            ts TEXT NOT NULL,
            wind_score REAL NOT NULL DEFAULT 0,
            vis_score REAL NOT NULL DEFAULT 0,
            ceiling_score REAL NOT NULL DEFAULT 0,
            precip_score REAL NOT NULL DEFAULT 0,
            convective_score REAL NOT NULL DEFAULT 0,
            winter_score REAL NOT NULL DEFAULT 0,
            overall_score REAL NOT NULL DEFAULT 0,
            challenge_level TEXT,
            wind_dir_degrees INTEGER,
            wind_speed_kt INTEGER,
            wind_gust_kt INTEGER,
            visibility_meters INTEGER,
            ceiling_ft_agl INTEGER,
            flight_category TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_weather_score_overall
            ON airport_weather_score_latest (overall_score);
        CREATE INDEX IF NOT EXISTS idx_weather_score_level
            ON airport_weather_score_latest (challenge_level);
        CREATE INDEX IF NOT EXISTS idx_weather_score_airport_ts
            ON airport_weather_score_latest (airport, ts);

        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            type TEXT NOT NULL,
            entity TEXT NOT NULL,
            airport TEXT,
            payload_json TEXT,
            dedupe_key TEXT UNIQUE
        );
        CREATE INDEX IF NOT EXISTS idx_events_ts
            ON events (ts);
        CREATE INDEX IF NOT EXISTS idx_events_type
            ON events (type);
        CREATE INDEX IF NOT EXISTS idx_events_airport
            ON events (airport);

        CREATE TABLE IF NOT EXISTS atc_seen (
            callsign TEXT PRIMARY KEY,
            last_seen TEXT NOT NULL,
            last_status TEXT NOT NULL,
            last_frequency TEXT,
            last_facility INTEGER,
            last_updated TEXT,
            cid INTEGER,
            name TEXT,
            rating INTEGER,
            server TEXT,
            logon_time TEXT
        );
        CREATE TABLE IF NOT EXISTS atc_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            callsign TEXT NOT NULL,
            airport TEXT,
            facility INTEGER,
            frequency TEXT,
            name TEXT,
            cid INTEGER,
            logon_time TEXT,
            started_at TEXT NOT NULL,
            last_seen TEXT NOT NULL,
            ended_at TEXT,
            is_active INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS idx_atc_sessions_callsign
            ON atc_sessions (callsign);
        CREATE INDEX IF NOT EXISTS idx_atc_sessions_airport
            ON atc_sessions (airport);
        CREATE INDEX IF NOT EXISTS idx_atc_sessions_is_active
            ON atc_sessions (is_active);
        CREATE INDEX IF NOT EXISTS idx_atc_sessions_started_at
            ON atc_sessions (started_at);
        """
    )
    conn.commit()


def get_feed_state(conn: sqlite3.Connection, feed_name: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM feed_state WHERE feed_name = ?", (feed_name,)
    ).fetchone()


def update_feed_state(
    conn: sqlite3.Connection,
    *,
    feed_name: str,
    last_fetch: str,
    last_update: str | None = None,
    last_success: str | None = None,
    last_error: str | None = None,
    last_error_at: str | None = None,
) -> None:
    conn.execute(
        """
        INSERT INTO feed_state (
            feed_name, last_update, last_fetch, last_success, last_error, last_error_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(feed_name)
        DO UPDATE SET
            last_update = COALESCE(excluded.last_update, feed_state.last_update),
            last_fetch = excluded.last_fetch,
            last_success = COALESCE(excluded.last_success, feed_state.last_success),
            last_error = excluded.last_error,
            last_error_at = excluded.last_error_at
        """,
        (feed_name, last_update, last_fetch, last_success, last_error, last_error_at),
    )
    conn.commit()
