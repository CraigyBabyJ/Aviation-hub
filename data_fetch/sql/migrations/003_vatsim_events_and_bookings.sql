-- VATSIM scheduled events (public API) and ATC bookings (API key).
-- Apply to an existing DB: sqlite3 data/aviation_hub.db < sql/migrations/003_vatsim_events_and_bookings.sql
-- New installs get the same objects via init_db() in db.py.

CREATE TABLE IF NOT EXISTS vatsim_events_latest (
    event_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    event_type TEXT,
    start_time_utc TEXT NOT NULL,
    end_time_utc TEXT NOT NULL,
    short_description TEXT,
    description TEXT,
    banner_url TEXT,
    link_url TEXT,
    airports_json TEXT,
    organisers_json TEXT,
    divisions_json TEXT,
    regions_json TEXT,
    fetched_at_utc TEXT NOT NULL,
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_vatsim_events_start
    ON vatsim_events_latest (start_time_utc);
CREATE INDEX IF NOT EXISTS idx_vatsim_events_end
    ON vatsim_events_latest (end_time_utc);

CREATE TABLE IF NOT EXISTS vatsim_atc_bookings_latest (
    booking_id TEXT PRIMARY KEY,
    callsign TEXT NOT NULL,
    airport_icao TEXT,
    fir_icao TEXT,
    position_type TEXT,
    controller_cid TEXT,
    controller_name TEXT,
    starts_at_utc TEXT NOT NULL,
    ends_at_utc TEXT NOT NULL,
    booking_type TEXT,
    fetched_at_utc TEXT NOT NULL,
    raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_vatsim_bookings_callsign
    ON vatsim_atc_bookings_latest (callsign);
CREATE INDEX IF NOT EXISTS idx_vatsim_bookings_airport
    ON vatsim_atc_bookings_latest (airport_icao);
CREATE INDEX IF NOT EXISTS idx_vatsim_bookings_starts
    ON vatsim_atc_bookings_latest (starts_at_utc);

DROP VIEW IF EXISTS airports_with_upcoming_events;
CREATE VIEW airports_with_upcoming_events AS
SELECT DISTINCT
    json_each.value AS airport_icao,
    e.event_id,
    e.name AS event_name,
    e.event_type,
    e.start_time_utc,
    e.end_time_utc,
    e.link_url
FROM vatsim_events_latest AS e
CROSS JOIN json_each(e.airports_json)
WHERE e.airports_json IS NOT NULL
  AND json_valid(e.airports_json)
  AND json_type(e.airports_json) = 'array'
  AND length(json_each.value) = 4
  AND e.end_time_utc >= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', 'utc');

DROP VIEW IF EXISTS airports_with_booked_atc_next_6h;
CREATE VIEW airports_with_booked_atc_next_6h AS
SELECT DISTINCT
    b.airport_icao,
    b.booking_id,
    b.callsign,
    b.position_type,
    b.fir_icao,
    b.starts_at_utc,
    b.ends_at_utc,
    b.booking_type,
    b.controller_cid
FROM vatsim_atc_bookings_latest AS b
WHERE b.airport_icao IS NOT NULL
  AND b.starts_at_utc <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '+6 hours')
  AND b.ends_at_utc >= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', 'utc');
