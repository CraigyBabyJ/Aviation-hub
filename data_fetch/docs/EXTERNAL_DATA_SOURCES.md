# External data sources and APIs

This document lists **every outbound HTTP(S) feed** used by the Aviation Hub **data_fetch** ingestor, plus **related third-party sites** that do **not** have a supported integration. It is the reference for Discord bots, other services, and operators deciding what to trust and where data originates.

**No existing fetcher behavior is implied by this file**—it is documentation only. Code defaults live in the Python modules named below.

---

## Ingestor feeds (implemented)

| Feed module | Environment / config | URL (default) | Data |
|-------------|----------------------|---------------|------|
| `fetchers/vatsim.py` | — | `https://data.vatsim.net/v3/vatsim-data.json` | Live VATSIM network: controllers, pilots (authoritative for “who is online now”). |
| `fetchers/atis.py` | — | `https://data.vatsim.net/v3/afv-atis-data.json` | AFV ATIS text per callsign. |
| `fetchers/ingest_vatsim_events.py` | `VATSIM_EVENTS_URL` | `https://events.vatsim.net/v1/latest` | Published upcoming/current VATSIM **events** (JSON). |
| `fetchers/ingest_vatsim_atc_bookings.py` | `VATSIM_BOOKINGS_URL`, optional `VATSIM_BOOKINGS_API_KEY` | `https://atc-bookings.vatsim.net/api/booking` | Scheduled ATC **bookings** (advisory JSON list). |
| `fetchers/metar.py` | — | `https://aviationweather.gov/data/cache/metars.cache.csv.gz` | AviationWeather **METAR** cache (gzip CSV). |
| `fetchers/taf.py` | — | `https://aviationweather.gov/data/cache/tafs.cache.xml.gz` | AviationWeather **TAF** cache (gzip XML). |
| `fetchers/sigmet.py` | — | `https://aviationweather.gov/api/data/isigmet?format=json` | International **SIGMET** JSON. |
| `fetchers/ourairports.py` | — | `https://davidmegginson.github.io/ourairports-data/airports.csv` (and `runways.csv`, `navaids.csv`) | OurAirports mirror for airport/runway reference. |

Official references (for operators, not hard-coded unless noted):

- VATSIM data & ATIS: [VATSIM data services](https://vatsim.net/docs/services/data) (see also [vatsim.dev](https://vatsim.dev/) for API docs).
- VATSIM Events API: [Events API](https://vatsim.dev/api/events-api).
- ATC Bookings: [atc-bookings.vatsim.net/api-doc](https://atc-bookings.vatsim.net/api-doc).
- AviationWeather: [Aviation Weather Center](https://aviationweather.gov/) data products.

---

## Internal HTTP API (no external fetch)

The widget server (`src/widget_server.py`), started from `src/main.py`, serves **read-only JSON** from the local SQLite database. It does **not** call StatSim or other third-party analytics sites.

Discord bot (`../discord_bot/bot.py`) and other clients should treat **`AVIATION_HUB_BASE_URL`** (default `http://127.0.0.1:4010`) as the integration surface. Route list: see **Available HTTP endpoints** in `../README.md`.

---

## StatSim ([statsim.net](https://statsim.net/))

**What it is:** A community **VATSIM statistics** site (overview, ATC/flight leaderboards, airport movements, replay-style features, etc.). Example content described on their overview: network totals, top airports by movements, top ATC positions by time—see [StatSim](https://statsim.net/).

**API status (as of documentation update):**

- The site is delivered as a **Blazor** application (server-rendered / SignalR-style stack), not a documented REST API.
- Probes such as `https://statsim.net/api`, `/swagger`, `/openapi.json` return **404**; there is **no official, stable JSON base URL** documented for third-party ingestion comparable to VATSIM or AviationWeather.

**Overlap with Aviation Hub:**

- **Live traffic / staffing:** Already covered by **`vatsim-data.json`** ingestion into `vatsim_controllers_latest`, `vatsim_pilots_latest`, and derived tables.
- **Events / bookings:** Covered by **`ingest_vatsim_events`** and **`ingest_vatsim_atc_bookings`** from official VATSIM endpoints.
- **Historical rankings / 24h aggregates / replay:** StatSim may add value as a **human-facing** analytics layer; reproducing that inside Aviation Hub would require either a **published API** from StatSim or a separate, explicit scraping project—with **copyright/ToS**, **stability**, and **maintenance** risks. That is **out of scope** for the default ingestor unless StatSim documents a supported integration.

**Recommendation:** Link users to StatSim for rich stats UI; keep Aviation Hub on **official feeds** above for automation, Discord, and your own SQLite/API.

---

## Change policy

- Updates to this document should **add** URLs and notes as new sources appear.
- Changing **which** URL a fetcher uses or **how** often it runs belongs in code and release notes, and should be reflected here in the same change.
