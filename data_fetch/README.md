# Aviation Hub Ingestor

Reliable Python ingestion service that runs continuously on Ubuntu 24 headless and stores latest VATSIM controllers, VATSIM pilots, VATSIM ATIS, METAR, and TAF data into SQLite.
It also performs a built-in weekly OurAirports dataset sync to `data/ourairports/` (no separate service required).

## Project layout

```text
data_fetch/
├── data/
├── sql/
│   └── migrations/
│       └── 003_vatsim_events_and_bookings.sql
├── scripts/
│   └── backfill_atc_sessions.py
├── requirements.txt
├── src/
│   ├── db.py
│   ├── main.py
│   ├── util.py
│   ├── widget_server.py
│   └── fetchers/
│       ├── airport_live_status.py
│       ├── atis.py
│       ├── ingest_vatsim_atc_bookings.py
│       ├── ingest_vatsim_events.py
│       ├── metar.py
│       ├── ourairports.py
│       ├── sigmet.py
│       ├── runway_enrichment.py
│       ├── taf.py
│       ├── vatsim_schedule_utils.py
│       └── vatsim.py
└── systemd/
    └── aviation-hub.service
```

## Setup

```bash
cd /workspace/Aviation-hub/data_fetch
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Run once (validation)

```bash
. .venv/bin/activate
python src/main.py --once
```

## Run continuously

```bash
. .venv/bin/activate
python src/main.py
```

## Widget endpoint

The widget HTTP endpoint is started automatically by `src/main.py` as part of the same service process.

Default bind:
- host: `0.0.0.0`
- port: `4010`

Optional startup overrides:

```bash
python src/main.py --widget-host 0.0.0.0 --widget-port 4010
```

Route:

```text
GET /widgets/current-spicy-airports
GET /api/weather/current?icao=EGMC
```

Example:

```bash
curl -sS http://localhost:4010/widgets/current-spicy-airports
curl -sS "http://localhost:4010/api/weather/current?icao=EGMC"
```

Current API route registry:
- `GET /widgets/current-spicy-airports`
- `GET /api/weather/current?icao=EGMC`

Weather endpoint response shape:
- `metar`
- `wind` (`dir_degrees`, `speed_kt`, `gust_kt`)
- `temp_c`
- `visibility` (`meters`, `statute_mi`)
- `cloud_layers` (`coverage`, `base_ft_agl`, `cloud_type`)
- `current_runways` (`arrival`, `departure`, `in_use`, `sources`)
- `flight_category`
- `observed_at`
- `pressure` (`hpa`, `in_hg`)
- `precip`
- `has_thunderstorm`
- `has_snow`
- `has_rain`
- `has_fog`
- `has_mist`
- `wx_summary`

Selection behavior summary:
- picks one `airliner` and one `ga` airport from derived latest tables
- day-state staged fallback per category: `day` -> `day/twilight` -> `any`
- category-specific ranking (airliner vs ga)
- GA applies a diversity preference against airliner primary condition

Graceful shutdown:
- Press `Ctrl+C` once to request a clean stop. The process exits after the current feed work finishes.

## Data sources and cadence

- VATSIM network JSON: every 60s (uses `general.reload` hint clamped to 30-120 seconds).
- VATSIM AFV ATIS JSON: every 60s.
- AviationWeather METAR cache CSV.GZ: every 10 minutes.
- AviationWeather TAF cache XML.GZ: every 30 minutes.
- AviationWeather international SIGMET JSON: every 20 minutes.
- OurAirports CSV sync: checked hourly, downloads only when 7 days have elapsed since last successful sync.
- VATSIM public events JSON: default every 15 minutes (`VATSIM_EVENTS_POLL_SECONDS`, default `900`).
- VATSIM ATC bookings JSON: default every 5 minutes (`VATSIM_BOOKINGS_POLL_SECONDS`, default `300`).

### VATSIM events vs ATC bookings (scheduled / advisory)

| Feed | Table | Meaning |
|------|--------|--------|
| `vatsim_events` | `vatsim_events_latest` | **Future / current published events** from the public VATSIM Events API (not live network positions). |
| `vatsim_atc_bookings` | `vatsim_atc_bookings_latest` | **Scheduled controller bookings** from the VATSIM ATC Bookings API. **Advisory only**—voluntary sign-ups, not a guarantee anyone will connect. |

**Configuration (environment variables)**

| Variable | Default | Notes |
|----------|---------|--------|
| `VATSIM_EVENTS_ENABLED` | `true` | Set to `false` to skip the events feed entirely. |
| `VATSIM_BOOKINGS_ENABLED` | `true` | Set to `false` to skip bookings entirely. |
| `VATSIM_EVENTS_URL` | `https://events.vatsim.net/v1/latest` | Override if VATSIM changes host or API version; confirm with [VATSIM Events API docs](https://vatsim.dev/api/events-api). |
| `VATSIM_BOOKINGS_URL` | `https://atc-bookings.vatsim.net/api/booking` | List endpoint; trailing slashes are trimmed. |
| `VATSIM_BOOKINGS_API_KEY` | _(empty)_ | Optional **Bearer** token. The public `GET /api/booking` list works **without** a key; set this if you need authenticated access (e.g. filtered or `key_only` queries per [API docs](https://atc-bookings.vatsim.net/api-doc)). |
| `VATSIM_EVENTS_POLL_SECONDS` | `900` | Minimum clamp `30`. |
| `VATSIM_BOOKINGS_POLL_SECONDS` | `300` | Minimum clamp `30`. |

**Assumptions**

- Events API responses are either a top-level JSON array or an object containing a `data` / `events`-style array; unknown fields remain recoverable from `raw_json`.
- Booking payloads match the published bookings API (array of objects with `id`, `callsign`, `start`, `end`, etc.); timestamp strings without a timezone are interpreted as **UTC**.
- The default events hostname must resolve in your environment; if not, set `VATSIM_EVENTS_URL` to the current official URL from VATSIM.

**SQLite helpers (views)**

- `airports_with_upcoming_events` — ICAO codes from `airports_json` for events that have not yet ended (UTC `strftime` window).
- `airports_with_booked_atc_next_6h` — booked **airport** positions (rows with `airport_icao` set) overlapping the next six hours from “now” in SQLite UTC.

Existing databases can apply the same DDL with:

```bash
sqlite3 data/aviation_hub.db < sql/migrations/003_vatsim_events_and_bookings.sql
```

Skip logic:
- VATSIM network updates only when `general.update_timestamp` changes.
- ATIS row upsert only when incoming `last_updated` is newer than stored.
- METAR row upsert only when incoming `observation_time` is newer than stored.
- TAF row upsert only when incoming `issue_time` is newer than stored.

## Database

SQLite DB path: `data/aviation_hub.db`

The app enables:
- `PRAGMA journal_mode=WAL`
- `PRAGMA synchronous=FULL`
- `PRAGMA busy_timeout=5000`
- single-instance file lock (`data/ingestor.lock`) to prevent multiple writers

## Feed to table map

- `vatsim_network`:
  - `vatsim_controllers_latest`
  - `vatsim_pilots_latest`
  - `events` (`ATC_ONLINE`/`ATC_OFFLINE`)
  - `atc_seen` (internal state for online/offline diffing)
  - `atc_sessions` (historical controller sessions)
  - `feed_state`
- `vatsim_atis`:
  - `vatsim_atis_latest`
  - `events` (`ATIS_CHANGED`)
  - `feed_state`
- `aviationweather_metar`:
  - `metar_latest`
  - `metar_history`
  - `feed_state`
- `aviationweather_taf`:
  - `taf_latest`
  - `airport_weather_flags_latest` (refresh)
  - `airport_weather_score_latest` (refresh)
  - `airport_live_status_latest` (refresh)
  - `feed_state`
- `aviationweather_sigmet`:
  - `sigmets`
  - `feed_state`
- `ourairports_sync`:
  - `airport_reference_latest`
  - `airport_runways_latest`
  - `airport_runway_summary_latest`
  - `airport_aircraft_suitability_latest`
  - `airport_live_status_latest` (refresh)
  - local files under `data/ourairports/`
  - `feed_state`
- `vatsim_events`:
  - `vatsim_events_latest`
  - views `airports_with_upcoming_events`
  - `feed_state`
- `vatsim_atc_bookings`:
  - `vatsim_atc_bookings_latest`
  - view `airports_with_booked_atc_next_6h`
  - `feed_state`

## Table reference

- `feed_state`: per-feed health and freshness (`last_fetch`, `last_update`, `last_success`, `last_error`).
- `vatsim_controllers_latest`: current controller snapshot (latest only).
- `vatsim_pilots_latest`: current pilot snapshot (latest only).
- `vatsim_atis_latest`: current ATIS records.
- `metar_latest`: latest METAR by ICAO.
- `metar_history`: append-only METAR observations by ICAO and observation time.
- `taf_latest`: latest TAF by ICAO.
- `sigmets`: latest worldwide SIGMET advisories keyed by identifier.
- `airport_reference_latest`: normalized airport metadata (ICAO, country/region/continent, lat/lon, type).
- `airport_weather_flags_latest`: latest derived weather phenomenon/severity flags.
- `airport_weather_score_latest`: latest derived weather challenge scores.
- `airport_live_status_latest`: one-row-per-airport live snapshot (reference + ATC/ATIS + weather).
- `airport_runways_latest`: normalized runway records from OurAirports.
- `airport_runway_summary_latest`: per-airport runway capability summary.
- `airport_aircraft_suitability_latest`: first-pass suitability flags by aircraft category.
- `events`: append-only event stream (`ATC_ONLINE`, `ATC_OFFLINE`, `ATIS_CHANGED`).
- `atc_seen`: internal state used to detect online/offline transitions.
- `atc_sessions`: historical ATC sessions for analytics.
- `vatsim_events_latest`: latest snapshot of published VATSIM events (`raw_json` retains the full event object for re-parsing).
- `vatsim_atc_bookings_latest`: latest snapshot of ATC bookings (`raw_json` per row).
- `airports_with_upcoming_events` / `airports_with_booked_atc_next_6h`: read-only views for airport-centric widgets (see section above).

`atc_sessions` fields:
- `callsign`, `airport`, `facility`, `frequency`, `name`, `cid`, `logon_time`
- `started_at`, `last_seen`, `ended_at`
- `is_active` (`1` active, `0` closed)

## Event and session flow

- Controllers with `facility <= 0` are observers and do not generate ATC events/sessions.
- When ATC appears in current snapshot but was not seen before:
  - emit `ATC_ONLINE` event
  - open `atc_sessions` row (`is_active=1`, `started_at=last_seen=event ts`)
- While ATC remains online:
  - update active session `last_seen` each successful VATSIM cycle
- When ATC disappears from current snapshot:
  - emit `ATC_OFFLINE` event
  - close active session (`ended_at=last_seen=event ts`, `is_active=0`)

## DB sanity checks

```bash
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS controllers FROM vatsim_controllers_latest;"
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS pilots FROM vatsim_pilots_latest;"
sqlite3 data/aviation_hub.db "SELECT icao, wind_gust_kt, observation_time FROM metar_latest WHERE wind_gust_kt IS NOT NULL ORDER BY wind_gust_kt DESC LIMIT 10;"
sqlite3 data/aviation_hub.db "SELECT icao, observation_time, wind_speed_kt, wind_gust_kt, altim_in_hg FROM metar_history WHERE icao='EGCC' ORDER BY observation_time DESC LIMIT 24;"
sqlite3 data/aviation_hub.db "SELECT icao, issue_time, valid_from_time, valid_to_time FROM taf_latest ORDER BY issue_time DESC LIMIT 10;"
sqlite3 data/aviation_hub.db "SELECT id, fir, hazard, valid_from, valid_to FROM sigmets ORDER BY valid_to DESC LIMIT 10;"
sqlite3 data/aviation_hub.db "SELECT callsign, airport, atis_code, last_updated FROM vatsim_atis_latest ORDER BY last_updated DESC LIMIT 10;"
sqlite3 data/aviation_hub.db "SELECT id, ts, type, entity, airport FROM events ORDER BY id DESC LIMIT 50;"
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS total_sessions, SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END) AS active_sessions FROM atc_sessions;"
sqlite3 data/aviation_hub.db "SELECT callsign, airport, started_at, last_seen FROM atc_sessions WHERE is_active=1 ORDER BY last_seen DESC LIMIT 20;"
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS live_rows, SUM(has_atc) AS airports_with_atc, SUM(has_atis) AS airports_with_atis FROM airport_live_status_latest;"
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS runways FROM airport_runways_latest; SELECT COUNT(*) AS runway_summary FROM airport_runway_summary_latest; SELECT COUNT(*) AS suitability FROM airport_aircraft_suitability_latest;"
```

Events are generated by the ingestor:
- `ATC_ONLINE` / `ATC_OFFLINE` from VATSIM controller snapshot diffs (facility > 0 only).
- `ATIS_CHANGED` when AFV ATIS `text_hash` changes for an existing callsign.

## Analytics queries

Total staffed minutes by airport:

```sql
SELECT
  airport,
  ROUND(SUM((julianday(COALESCE(ended_at, last_seen)) - julianday(started_at)) * 24 * 60), 1) AS staffed_minutes
FROM atc_sessions
WHERE airport IS NOT NULL
GROUP BY airport
ORDER BY staffed_minutes DESC
LIMIT 50;
```

Sessions today for one airport:

```sql
SELECT COUNT(*) AS sessions_today
FROM atc_sessions
WHERE airport = 'EGCC'
  AND date(started_at) = date('now');
```

Most frequently staffed airports:

```sql
SELECT airport, COUNT(*) AS session_count
FROM atc_sessions
WHERE airport IS NOT NULL
GROUP BY airport
ORDER BY session_count DESC
LIMIT 50;
```

Currently active controller sessions:

```sql
SELECT callsign, airport, facility, frequency, started_at, last_seen
FROM atc_sessions
WHERE is_active = 1
ORDER BY last_seen DESC;
```

Top challenging airports right now:

```sql
SELECT airport, country, overall_score, challenge_level, flight_category
FROM airport_live_status_latest
ORDER BY overall_score DESC
LIMIT 20;
```

Recent METAR trend for one airport:

```sql
SELECT
  observation_time,
  wind_dir_degrees,
  wind_speed_kt,
  wind_gust_kt,
  temp_c,
  visibility_statute_mi,
  altim_in_hg
FROM metar_history
WHERE icao = 'EGCC'
ORDER BY observation_time DESC
LIMIT 72;
```

Snow + ATC airports:

```sql
SELECT airport, country, controller_count, overall_score
FROM airport_live_status_latest
WHERE has_snow = 1 AND has_atc = 1
ORDER BY overall_score DESC;
```

Airliner-suitable airports in bad weather:

```sql
SELECT l.airport, l.country, l.overall_score, s.best_hard_runway_ft
FROM airport_live_status_latest l
JOIN airport_aircraft_suitability_latest s ON s.airport = l.airport
WHERE s.suitable_airliner_jet = 1
ORDER BY l.overall_score DESC
LIMIT 50;
```

## Widget response shape

`GET /widgets/current-spicy-airports` returns:

- `generated_at`
- `airliner` (or `null`)
- `ga` (or `null`)

Each category object includes:
- airport identity (`airport`, `name`, `country`, `region`)
- weather severity (`overall_score`, `challenge_level`, `flight_category`)
- weather flags (`has_snow`, `has_thunderstorm`, `is_gusty`, `is_low_visibility`, `is_low_ceiling`)
- key metrics (`wind_gust_kt`, `visibility_meters`)
- daylight preference fields (`day_state`, `is_daylight`)
- computed rank (`spicy_rank`)
- dominant condition hint (`primary_condition`)

## Available HTTP endpoints

Current read-only HTTP routes served by `src/main.py`:

- `GET /widgets/current-spicy-airports`

## Backfill

Use this when `atc_sessions` is missing or needs rebuilding from existing `events`:

```bash
cd data_fetch
. .venv/bin/activate
python scripts/backfill_atc_sessions.py --replace
```

Notes:
- Backfill reads `ATC_ONLINE`/`ATC_OFFLINE` ordered by timestamp and pairs by `callsign`.
- If a controller has no offline event yet, session remains active (`is_active=1`).
- Without `--replace`, script refuses to run if `atc_sessions` already has rows.

## Troubleshooting

Check service status:

```bash
systemctl status aviation-hub.service --no-pager -l
```

Tail logs:

```bash
journalctl -u aviation-hub.service -f
```

Check per-feed health:

```bash
sqlite3 data/aviation_hub.db "SELECT feed_name, last_fetch, last_update, last_success, last_error, last_error_at FROM feed_state ORDER BY feed_name;"
```

Check event throughput:

```bash
sqlite3 data/aviation_hub.db "SELECT type, COUNT(*) FROM events GROUP BY type ORDER BY 2 DESC;"
```

## systemd service

1. Edit `systemd/aviation-hub.service`:
   - Set `User=` to your account (default should be your current user).
   - Replace `/path/to/data_fetch` in `WorkingDirectory` and `ExecStart`.
2. Install and start:

```bash
sudo cp systemd/aviation-hub.service /etc/systemd/system/aviation-hub.service
sudo systemctl daemon-reload
sudo systemctl enable --now aviation-hub
```

3. Logs:

```bash
sudo journalctl -u aviation-hub -f
```

Ensure the configured user can write to `./data`.
