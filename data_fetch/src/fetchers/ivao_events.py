from __future__ import annotations

import json
import logging
import re
import sqlite3
import time

import requests

from util import utc_now_iso

LOGGER = logging.getLogger("aviation_hub.ivao_events")

LISTING_URL = "https://ivao.events"
UA = "Mozilla/5.0 (compatible; AviationHub/1.0)"

MONTH_MAP = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

ICAO_SKIP = {
    "IVAO","HTML","HTTP","HTTPS","META","LINK","BODY","HEAD","TYPE","TRUE",
    "JPEG","WEBP","WEBM","WITH","YOUR","FROM","HAVE","WILL","THAT","THIS",
    "ALSO","MAKE","SURE","READ","FULL","NOTE","BOOK","SLOT","INTO","FIND",
    "MORE","BEEN","BOTH","SUCH","THAN","THEN","THEM","THEY","WHEN","WHAT",
    "WERE","SAID","EACH","MUCH","SOME","INTO","OVER","ONLY","COME","LIKE",
}


def _strip_html(html: str) -> str:
    html = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL)
    html = re.sub(r"<style[^>]*>.*?</style>",  "", html, flags=re.DOTALL)
    html = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s+", " ", html).strip()


def _parse_starts_ends(date_label: str, time_label: str) -> tuple[str | None, str | None]:
    """Parse '2nd July' + '13:00z - 15:00z' → (starts_at_iso, ends_at_iso)."""
    m = re.match(r"(\d+)(?:st|nd|rd|th)?\s+(\w+)", date_label, re.I)
    if not m:
        return None, None
    day = int(m.group(1))
    mon = MONTH_MAP.get(m.group(2).lower())
    if not mon:
        return None, None

    import datetime
    year = datetime.datetime.utcnow().year

    times = re.findall(r"(\d{2}):(\d{2})z", time_label, re.I)
    starts_at = ends_at = None
    if times:
        hh, mm = int(times[0][0]), int(times[0][1])
        starts_at = datetime.datetime(year, mon, day, hh, mm, tzinfo=datetime.timezone.utc).isoformat()
    if len(times) >= 2:
        hh, mm = int(times[1][0]), int(times[1][1])
        ends_at  = datetime.datetime(year, mon, day, hh, mm, tzinfo=datetime.timezone.utc).isoformat()
    return starts_at, ends_at


def _extract_icaos(text: str) -> list[str]:
    raw = re.findall(r"\b([A-Z]{4})\b", text)
    return list(dict.fromkeys(c for c in raw if c not in ICAO_SKIP))


def _scrape_listing(session: requests.Session) -> list[dict]:
    """Return list of basic event dicts from the listing page."""
    resp = session.get(LISTING_URL, headers={"User-Agent": UA}, timeout=(10, 20))
    resp.raise_for_status()
    html = resp.text

    parts = html.split(r'<a href="/')
    events: list[dict] = []
    for part in parts[1:]:
        id_m = re.match(r"(\d+)\">", part)
        if not id_m:
            continue
        eid = id_m.group(1)
        content = part.split("</a>")[0]
        text = _strip_html(content)

        title_m  = re.search(r"(?:In \d+ Days?|Today|Tomorrow)\s+(.+?)(?:\s+\d+(?:st|nd|rd|th)?\s+\w+,)", text, re.I)
        date_m   = re.search(r"(\d+(?:st|nd|rd|th)?\s+\w+),\s*starting\s+(\d{2}:\d{2}z)", text, re.I)
        date_m2  = re.search(r"(\d+(?:st|nd|rd|th)?\s+\w+)", text, re.I)
        time_m   = re.search(r"starting\s+(\d{2}:\d{2}z)", text, re.I)

        title      = title_m.group(1).strip() if title_m else re.sub(r"(?:In \d+ Days?|Today|Tomorrow)", "", text).strip().split(",")[0]
        date_label = date_m.group(1) if date_m else (date_m2.group(1) if date_m2 else "")
        time_label = time_m.group(1) if time_m else ""

        img_m = re.search(r'src="(https?://[^"]+\.(?:jpg|jpeg|png|webp)[^"]*)"', content, re.I)
        image = img_m.group(1) if img_m else ""

        title = re.sub(r"\s+HQ Event$", "", title).strip()
        if not title:
            continue

        events.append({
            "event_id":   eid,
            "title":      title,
            "date_label": date_label,
            "time_label": time_label,
            "image":      image,
            "url":        f"https://ivao.events/{eid}",
        })
    return events


def _scrape_detail(session: requests.Session, event_id: str, base: dict, known_icaos: set[str]) -> dict:
    """Fetch the detail page and enrich the event dict."""
    url = f"{LISTING_URL}/{event_id}"
    try:
        resp = session.get(url, headers={"User-Agent": UA}, timeout=(10, 20))
        resp.raise_for_status()
        text = _strip_html(resp.text)
    except Exception as exc:
        LOGGER.warning("ivao_events: failed to fetch detail for %s: %s", event_id, exc)
        return base

    # Extract airports from the "Location(s)" section only — it sits between
    # "Location(s)" and "Event Link" on the detail page.
    airports: list[str] = []
    loc_m = re.search(
        r"Location\(s\)\s+(?:[\d\w]+\s+[\w]+\s+[\d:z\s\-]+)?([\w\s,/]+?)(?:\s+Event Link|\s+Event Details)",
        text, re.I
    )
    if loc_m:
        candidates = re.findall(r"\b([A-Z]{3,4})\b", loc_m.group(1).upper())
        airports = [c for c in dict.fromkeys(candidates) if c in known_icaos]

    # If location section failed, scan the full text but validate against known airports
    if not airports:
        all_upper = text.upper()
        candidates = re.findall(r"\b([A-Z]{4})\b", all_upper)
        airports = list(dict.fromkeys(c for c in candidates if c in known_icaos))[:6]

    # Description: text between "Event Details" and "Other upcoming events"
    desc_m = re.search(
        r"Event Details\s+(.+?)(?:Other upcoming events|Briefing Information|For pilots|For air traffic)",
        text, re.I | re.DOTALL
    )
    description = re.sub(r"\s+", " ", desc_m.group(1)).strip() if desc_m else ""
    if len(description) > 1500:
        description = description[:1497] + "…"

    starts_at, ends_at = _parse_starts_ends(base["date_label"], base.get("time_label", ""))

    return {
        **base,
        "airports":    airports,
        "description": description,
        "starts_at":   starts_at,
        "ends_at":     ends_at,
    }


def process_ivao_events(conn: sqlite3.Connection, session: requests.Session) -> int:
    """
    Scrape ivao.events listing → for each event not already detailed in DB,
    fetch the detail page and store. Returns count of events seen.
    """
    now = utc_now_iso()

    # Load known ICAO codes from the airports table for ICAO validation
    known_icaos: set[str] = set()
    try:
        rows = conn.execute("SELECT icao FROM airport_reference_latest WHERE length(icao) = 4").fetchall()
        known_icaos = {r["icao"].upper() for r in rows}
    except Exception:
        pass

    try:
        listing = _scrape_listing(session)
    except Exception as exc:
        LOGGER.error("ivao_events: listing scrape failed: %s", exc)
        return 0

    seen_ids = [e["event_id"] for e in listing]
    detail_fetched: set[str] = set()

    if seen_ids:
        placeholders = ",".join("?" * len(seen_ids))
        rows = conn.execute(
            f"SELECT event_id FROM ivao_events WHERE event_id IN ({placeholders}) AND detail_fetched_at IS NOT NULL",
            seen_ids,
        ).fetchall()
        detail_fetched = {r["event_id"] for r in rows}

    for ev in listing:
        eid = ev["event_id"]
        if eid not in detail_fetched:
            LOGGER.info("ivao_events: fetching detail for event %s", eid)
            ev = _scrape_detail(session, eid, ev, known_icaos)
            time.sleep(0.5)  # be polite

        airports_json = json.dumps(ev.get("airports", []))
        starts_at, ends_at = _parse_starts_ends(ev.get("date_label", ""), ev.get("time_label", ""))

        conn.execute(
            """
            INSERT INTO ivao_events (
                event_id, title, date_label, time_label, starts_at, ends_at,
                airports, description, image, url, detail_fetched_at, first_seen_at, last_seen_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(event_id) DO UPDATE SET
                title            = excluded.title,
                date_label       = excluded.date_label,
                time_label       = excluded.time_label,
                starts_at        = excluded.starts_at,
                ends_at          = excluded.ends_at,
                airports         = CASE WHEN excluded.detail_fetched_at IS NOT NULL THEN excluded.airports ELSE ivao_events.airports END,
                description      = CASE WHEN excluded.detail_fetched_at IS NOT NULL THEN excluded.description ELSE ivao_events.description END,
                image            = excluded.image,
                url              = excluded.url,
                detail_fetched_at = COALESCE(excluded.detail_fetched_at, ivao_events.detail_fetched_at),
                last_seen_at     = excluded.last_seen_at
            """,
            (
                eid,
                ev["title"],
                ev.get("date_label", ""),
                ev.get("time_label", ""),
                starts_at,
                ends_at,
                airports_json,
                ev.get("description", ""),
                ev.get("image", ""),
                ev["url"],
                now if eid not in detail_fetched else None,
                now,
                now,
            ),
        )

    conn.commit()
    LOGGER.info("ivao_events: stored/updated %d events", len(listing))
    return len(listing)
