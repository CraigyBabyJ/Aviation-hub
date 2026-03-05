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
