# Aviation Hub Ingestor

Reliable Python ingestion service that runs continuously on Ubuntu 24 headless and stores latest VATSIM + METAR data into SQLite.

## Project layout

```text
data_fetch/
├── data/
├── requirements.txt
├── src/
│   ├── db.py
│   ├── main.py
│   ├── util.py
│   └── fetchers/
│       ├── atis.py
│       ├── metar.py
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

## Data sources and cadence

- VATSIM network JSON: every 60s (uses `general.reload` hint clamped to 30-120 seconds).
- VATSIM AFV ATIS JSON: every 60s.
- AviationWeather METAR cache CSV.GZ: every 10 minutes.

Skip logic:
- VATSIM network updates only when `general.update_timestamp` changes.
- ATIS row upsert only when incoming `last_updated` is newer than stored.
- METAR row upsert only when incoming `observation_time` is newer than stored.

## Database

SQLite DB path: `data/aviation_hub.db`

The app enables:
- `PRAGMA journal_mode=WAL`
- `PRAGMA busy_timeout=5000`

## DB sanity checks

```bash
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS controllers FROM vatsim_controllers_latest;"
sqlite3 data/aviation_hub.db "SELECT icao, wind_gust_kt, observation_time FROM metar_latest WHERE wind_gust_kt IS NOT NULL ORDER BY wind_gust_kt DESC LIMIT 10;"
sqlite3 data/aviation_hub.db "SELECT callsign, airport, atis_code, last_updated FROM vatsim_atis_latest ORDER BY last_updated DESC LIMIT 10;"
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
