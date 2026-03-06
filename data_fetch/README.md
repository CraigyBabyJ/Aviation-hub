# Aviation Hub Ingestor

Reliable Python ingestion service that runs continuously on Ubuntu 24 headless and stores latest VATSIM controllers, VATSIM pilots, VATSIM ATIS, METAR, and TAF data into SQLite.
It also performs a built-in weekly OurAirports dataset sync to `data/ourairports/` (no separate service required).

## Project layout

```text
data_fetch/
├── data/
├── scripts/
│   └── backfill_atc_sessions.py
├── requirements.txt
├── src/
│   ├── db.py
│   ├── main.py
│   ├── util.py
│   └── fetchers/
│       ├── airport_live_status.py
│       ├── atis.py
│       ├── metar.py
│       ├── ourairports.py
│       ├── runway_enrichment.py
│       ├── taf.py
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

Graceful shutdown:
- Press `Ctrl+C` once to request a clean stop. The process exits after the current feed work finishes.

## Data sources and cadence

- VATSIM network JSON: every 60s (uses `general.reload` hint clamped to 30-120 seconds).
- VATSIM AFV ATIS JSON: every 60s.
- AviationWeather METAR cache CSV.GZ: every 10 minutes.
- AviationWeather TAF cache XML.GZ: every 30 minutes.
- OurAirports CSV sync: checked hourly, downloads only when 7 days have elapsed since last successful sync.

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
  - `feed_state`
- `aviationweather_taf`:
  - `taf_latest`
  - `airport_weather_flags_latest` (refresh)
  - `airport_weather_score_latest` (refresh)
  - `airport_live_status_latest` (refresh)
  - `feed_state`
- `ourairports_sync`:
  - `airport_reference_latest`
  - `airport_runways_latest`
  - `airport_runway_summary_latest`
  - `airport_aircraft_suitability_latest`
  - `airport_live_status_latest` (refresh)
  - local files under `data/ourairports/`
  - `feed_state`

## Table reference

- `feed_state`: per-feed health and freshness (`last_fetch`, `last_update`, `last_success`, `last_error`).
- `vatsim_controllers_latest`: current controller snapshot (latest only).
- `vatsim_pilots_latest`: current pilot snapshot (latest only).
- `vatsim_atis_latest`: current ATIS records.
- `metar_latest`: latest METAR by ICAO.
- `taf_latest`: latest TAF by ICAO.
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
sqlite3 data/aviation_hub.db "SELECT icao, issue_time, valid_from_time, valid_to_time FROM taf_latest ORDER BY issue_time DESC LIMIT 10;"
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
