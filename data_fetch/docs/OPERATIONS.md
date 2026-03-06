# Aviation Hub Operations Runbook

This is the quick day-to-day guide for running and checking the ingestor.

## Service control

Check status:

```bash
systemctl status aviation-hub.service --no-pager -l
```

Restart:

```bash
sudo systemctl restart aviation-hub.service
systemctl is-active aviation-hub.service
```

Follow logs:

```bash
journalctl -u aviation-hub.service -f
```

Recent errors only:

```bash
journalctl -u aviation-hub.service -n 400 --no-pager | rg -i "error|exception|traceback|failed"
```

## Feed cadence (current)

- `vatsim_network`: dynamic (30-120s, usually ~60s)
- `vatsim_atis`: 60s
- `aviationweather_metar`: 600s (10m)
- `aviationweather_taf`: 1800s (30m)
- `ourairports_sync`: 3600s check, download every 7 days

## Database quick checks

DB path: `data/aviation_hub.db`

```bash
cd /home/craig/projects/Aviation-hub/data_fetch
sqlite3 data/aviation_hub.db "SELECT feed_name, last_fetch, last_update, last_success, last_error, last_error_at FROM feed_state ORDER BY feed_name;"
```

Current snapshot counts:

```bash
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS controllers FROM vatsim_controllers_latest;"
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS pilots FROM vatsim_pilots_latest;"
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS atis FROM vatsim_atis_latest;"
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS metar FROM metar_latest;"
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS taf FROM taf_latest;"
```

Event throughput:

```bash
sqlite3 data/aviation_hub.db "SELECT type, COUNT(*) FROM events GROUP BY type ORDER BY 2 DESC;"
sqlite3 data/aviation_hub.db "SELECT id, ts, type, entity, airport FROM events ORDER BY id DESC LIMIT 30;"
```

## ATC session checks

Session totals:

```bash
sqlite3 data/aviation_hub.db "SELECT COUNT(*) AS total_sessions, SUM(CASE WHEN is_active=1 THEN 1 ELSE 0 END) AS active_sessions, SUM(CASE WHEN is_active=0 THEN 1 ELSE 0 END) AS closed_sessions FROM atc_sessions;"
```

Currently active ATC sessions:

```bash
sqlite3 -header -column data/aviation_hub.db "SELECT callsign, airport, facility, frequency, started_at, last_seen FROM atc_sessions WHERE is_active=1 ORDER BY last_seen DESC LIMIT 50;"
```

Staffed minutes by airport:

```bash
sqlite3 -header -column data/aviation_hub.db "SELECT airport, ROUND(SUM((julianday(COALESCE(ended_at,last_seen)) - julianday(started_at))*24*60),1) AS staffed_minutes FROM atc_sessions WHERE airport IS NOT NULL GROUP BY airport ORDER BY staffed_minutes DESC LIMIT 30;"
```

Sessions today for one airport (example `EGCC`):

```bash
sqlite3 data/aviation_hub.db "SELECT COUNT(*) FROM atc_sessions WHERE airport='EGCC' AND date(started_at)=date('now');"
```

## Backfill sessions from events

Use when `atc_sessions` is empty or needs rebuild:

```bash
cd /home/craig/projects/Aviation-hub/data_fetch
. .venv/bin/activate
python scripts/backfill_atc_sessions.py --replace
```

Notes:
- Reads `ATC_ONLINE`/`ATC_OFFLINE` events ordered by timestamp.
- Pairs by `callsign`.
- Leaves unmatched online controllers as active sessions.

## Where data comes from

- `vatsim_network` -> controllers, pilots, ATC events, ATC sessions
- `vatsim_atis` -> `vatsim_atis_latest`, `ATIS_CHANGED` events
- `aviationweather_metar` -> `metar_latest`
- `aviationweather_taf` -> `taf_latest`
- `ourairports_sync` -> local CSV files in `data/ourairports/`
