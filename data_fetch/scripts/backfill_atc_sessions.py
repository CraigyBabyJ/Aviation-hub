from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from db import DB_PATH, get_connection, init_db  # noqa: E402
from util import extract_airport_from_callsign, to_int  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill atc_sessions from ATC_ONLINE/ATC_OFFLINE events"
    )
    parser.add_argument(
        "--db",
        default=str(DB_PATH),
        help=f"SQLite DB path (default: {DB_PATH})",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing atc_sessions before backfill (recommended for idempotency)",
    )
    return parser.parse_args()


def _payload_dict(raw: str | None) -> dict:
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _insert_session(
    conn: sqlite3.Connection,
    *,
    callsign: str,
    airport: str | None,
    facility: int | None,
    frequency: str | None,
    name: str | None,
    cid: int | None,
    logon_time: str | None,
    started_at: str,
    last_seen: str,
    ended_at: str | None,
    is_active: int,
) -> None:
    conn.execute(
        """
        INSERT INTO atc_sessions (
            callsign, airport, facility, frequency, name, cid, logon_time,
            started_at, last_seen, ended_at, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            callsign,
            airport,
            facility,
            frequency,
            name,
            cid,
            logon_time,
            started_at,
            last_seen,
            ended_at,
            is_active,
        ),
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db)

    with get_connection(db_path) as conn:
        init_db(conn)
        with conn:
            existing = conn.execute("SELECT COUNT(*) AS count FROM atc_sessions").fetchone()
            existing_count = to_int(existing["count"]) or 0
            if existing_count > 0 and not args.replace:
                print(
                    f"Refusing to backfill: atc_sessions already has {existing_count} rows. "
                    "Re-run with --replace to rebuild."
                )
                return 1

            if args.replace:
                conn.execute("DELETE FROM atc_sessions")

            open_sessions: dict[str, dict] = {}
            opened = 0
            closed = 0

            rows = conn.execute(
                """
                SELECT ts, type, entity, airport, payload_json
                FROM events
                WHERE type IN ('ATC_ONLINE', 'ATC_OFFLINE')
                ORDER BY ts ASC, id ASC
                """
            ).fetchall()

            for row in rows:
                ts = row["ts"]
                event_type = row["type"]
                callsign = (row["entity"] or "").strip()
                if not ts or not callsign:
                    continue

                payload = _payload_dict(row["payload_json"])
                airport = row["airport"] or extract_airport_from_callsign(callsign)
                facility = to_int(payload.get("facility"))
                frequency = payload.get("frequency")
                name = payload.get("name")
                cid = to_int(payload.get("cid"))
                logon_time = payload.get("logon_time")

                if event_type == "ATC_ONLINE":
                    if callsign in open_sessions:
                        prev = open_sessions.pop(callsign)
                        _insert_session(
                            conn,
                            callsign=callsign,
                            airport=prev["airport"],
                            facility=prev["facility"],
                            frequency=prev["frequency"],
                            name=prev["name"],
                            cid=prev["cid"],
                            logon_time=prev["logon_time"],
                            started_at=prev["started_at"],
                            last_seen=ts,
                            ended_at=ts,
                            is_active=0,
                        )
                        closed += 1

                    open_sessions[callsign] = {
                        "airport": airport,
                        "facility": facility,
                        "frequency": frequency,
                        "name": name,
                        "cid": cid,
                        "logon_time": logon_time,
                        "started_at": ts,
                        "last_seen": ts,
                    }
                    opened += 1
                    continue

                active = open_sessions.pop(callsign, None)
                if active is None:
                    continue
                _insert_session(
                    conn,
                    callsign=callsign,
                    airport=active["airport"],
                    facility=active["facility"],
                    frequency=active["frequency"],
                    name=active["name"],
                    cid=active["cid"],
                    logon_time=active["logon_time"],
                    started_at=active["started_at"],
                    last_seen=ts,
                    ended_at=ts,
                    is_active=0,
                )
                closed += 1

            for callsign, active in open_sessions.items():
                _insert_session(
                    conn,
                    callsign=callsign,
                    airport=active["airport"],
                    facility=active["facility"],
                    frequency=active["frequency"],
                    name=active["name"],
                    cid=active["cid"],
                    logon_time=active["logon_time"],
                    started_at=active["started_at"],
                    last_seen=active["last_seen"],
                    ended_at=None,
                    is_active=1,
                )

            active_count = len(open_sessions)

    print(
        f"Backfill complete: online_events={opened} closed_sessions={closed} active_sessions={active_count}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
