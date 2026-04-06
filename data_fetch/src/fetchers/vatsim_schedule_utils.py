from __future__ import annotations

import json
from typing import Any

from util import extract_airport_from_callsign

_POSITION_SUFFIXES = frozenset(
    {"DEL", "GND", "TWR", "APP", "DEP", "CTR", "FSS", "OBS", "SUP", "INFO"}
)


def normalize_event_airports_json(airports_raw: Any) -> str | None:
    """Return compact JSON array of unique 4-letter ICAO codes from API `airports` list."""
    if airports_raw is None:
        return None
    if not isinstance(airports_raw, list):
        return None
    codes: set[str] = set()
    for item in airports_raw:
        code: str | None = None
        if isinstance(item, str):
            code = item.strip().upper()
        elif isinstance(item, dict):
            for key in ("icao", "ICAO", "airport", "ident", "id", "code"):
                v = item.get(key)
                if v is not None and str(v).strip():
                    code = str(v).strip().upper()
                    break
        if code and len(code) == 4 and code.isalpha():
            codes.add(code)
    return json.dumps(sorted(codes), separators=(",", ":"), ensure_ascii=True)


def organisers_sidecar_json(organisers_raw: Any) -> tuple[str | None, str | None, str | None]:
    """
    Build organisers_json, divisions_json, regions_json (all compact JSON text).
    Unknown shapes are preserved in organisers_json via default=str where needed.
    """
    if organisers_raw is None:
        return None, None, None
    if not isinstance(organisers_raw, list):
        return json.dumps(organisers_raw, separators=(",", ":"), default=str, ensure_ascii=True), None, None

    divisions: set[str] = set()
    regions: set[str] = set()
    for org in organisers_raw:
        if not isinstance(org, dict):
            continue
        div = org.get("division") or org.get("division_id") or org.get("Division")
        reg = org.get("region") or org.get("region_id") or org.get("Region")
        sub = org.get("subdivision") or org.get("subdivision_id")
        if div is not None and str(div).strip():
            divisions.add(str(div).strip())
        if reg is not None and str(reg).strip():
            regions.add(str(reg).strip())
        if sub is not None and str(sub).strip():
            divisions.add(str(sub).strip())

    org_json = json.dumps(organisers_raw, separators=(",", ":"), default=str, ensure_ascii=True)
    div_json = json.dumps(sorted(divisions), separators=(",", ":"), ensure_ascii=True) if divisions else None
    reg_json = json.dumps(sorted(regions), separators=(",", ":"), ensure_ascii=True) if regions else None
    return org_json, div_json, reg_json


def derive_vatsim_booking_fields(callsign: str) -> tuple[str | None, str | None, str | None]:
    """
    Derive airport ICAO, FIR-style ICAO, and position suffix from a booking callsign.

    Assumptions:
    - Tower/ground/approach-style positions use a 4-letter ICAO prefix (e.g. EGLL_TWR).
    - CTR/FSS/OBS-style positions use the first underscore segment as FIR or sector id (e.g. LON_C_CTR).
    """
    raw = (callsign or "").strip().upper()
    parts = [p for p in raw.split("_") if p]
    if not parts:
        return None, None, None

    position_type = parts[-1] if parts[-1] in _POSITION_SUFFIXES else None
    first = parts[0]

    if position_type in {"CTR", "FSS", "OBS"}:
        fir = first if first.isalpha() and 2 <= len(first) <= 4 else None
        return None, fir, position_type

    airport = extract_airport_from_callsign(raw)
    if airport is None and first.isalpha() and len(first) == 4:
        airport = first
    return airport, None, position_type


def extract_event_id(ev: dict[str, Any]) -> str | None:
    for key in ("id", "event_id", "eventId", "uuid", "slug"):
        v = ev.get(key)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None


def pick_str(ev: dict[str, Any], *keys: str) -> str | None:
    for k in keys:
        v = ev.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return None
