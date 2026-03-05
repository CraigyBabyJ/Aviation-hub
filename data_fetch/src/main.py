from __future__ import annotations

import argparse
import fcntl
import logging
import signal
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from threading import Event

import requests

from db import get_connection, init_db, update_feed_state
from fetchers.atis import FEED_NAME as ATIS_FEED, process_atis
from fetchers.metar import FEED_NAME as METAR_FEED, process_metar
from fetchers.vatsim import FEED_NAME as VATSIM_FEED, next_poll_seconds, process_vatsim_network
from util import configure_logging, utc_now_iso

LOGGER = logging.getLogger("aviation_hub.main")
STOP_EVENT = Event()
LOCK_PATH = Path(__file__).resolve().parent.parent / "data" / "ingestor.lock"


def _request_shutdown(signum: int, _frame: object) -> None:
    signal_name = signal.Signals(signum).name
    if not STOP_EVENT.is_set():
        LOGGER.info("%s received; shutting down after current cycle", signal_name)
    STOP_EVENT.set()


@dataclass
class PollState:
    interval: int
    next_run: float = 0.0


def run_cycle(conn: sqlite3.Connection, session: requests.Session, *, once: bool = False) -> int:
    # In once mode, bypass polling cadence and execute each feed one time.
    if once:
        LOGGER.info("Running once mode: checking all feeds immediately")
        try:
            LOGGER.info("Checking %s", VATSIM_FEED)
            process_vatsim_network(conn, session)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("VATSIM processing failed: %s", exc)
            update_feed_state(
                conn,
                feed_name=VATSIM_FEED,
                last_fetch=utc_now_iso(),
                last_error=str(exc),
                last_error_at=utc_now_iso(),
            )

        try:
            LOGGER.info("Checking %s", ATIS_FEED)
            process_atis(conn, session)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("ATIS processing failed: %s", exc)
            update_feed_state(
                conn,
                feed_name=ATIS_FEED,
                last_fetch=utc_now_iso(),
                last_error=str(exc),
                last_error_at=utc_now_iso(),
            )

        try:
            LOGGER.info("Checking %s", METAR_FEED)
            process_metar(conn, session)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("METAR processing failed: %s", exc)
            update_feed_state(
                conn,
                feed_name=METAR_FEED,
                last_fetch=utc_now_iso(),
                last_error=str(exc),
                last_error_at=utc_now_iso(),
            )
        return 0

    now = time.time()
    polls = {
        VATSIM_FEED: PollState(interval=60, next_run=0.0),
        ATIS_FEED: PollState(interval=60, next_run=0.0),
        METAR_FEED: PollState(interval=600, next_run=0.0),
    }

    while not STOP_EVENT.is_set():
        now = time.time()

        if now >= polls[VATSIM_FEED].next_run:
            try:
                LOGGER.info("Checking %s", VATSIM_FEED)
                _, _, reload_hint = process_vatsim_network(conn, session)
                polls[VATSIM_FEED].interval = next_poll_seconds(reload_hint)
                LOGGER.info(
                    "%s check complete; next check in %ss",
                    VATSIM_FEED,
                    polls[VATSIM_FEED].interval,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("VATSIM processing failed: %s", exc)
                update_feed_state(
                    conn,
                    feed_name=VATSIM_FEED,
                    last_fetch=utc_now_iso(),
                    last_error=str(exc),
                    last_error_at=utc_now_iso(),
                )
            polls[VATSIM_FEED].next_run = now + polls[VATSIM_FEED].interval

        if now >= polls[ATIS_FEED].next_run:
            try:
                LOGGER.info("Checking %s", ATIS_FEED)
                process_atis(conn, session)
                LOGGER.info(
                    "%s check complete; next check in %ss",
                    ATIS_FEED,
                    polls[ATIS_FEED].interval,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("ATIS processing failed: %s", exc)
                update_feed_state(
                    conn,
                    feed_name=ATIS_FEED,
                    last_fetch=utc_now_iso(),
                    last_error=str(exc),
                    last_error_at=utc_now_iso(),
                )
            polls[ATIS_FEED].next_run = now + polls[ATIS_FEED].interval

        if now >= polls[METAR_FEED].next_run:
            try:
                LOGGER.info("Checking %s", METAR_FEED)
                process_metar(conn, session)
                LOGGER.info(
                    "%s check complete; next check in %ss",
                    METAR_FEED,
                    polls[METAR_FEED].interval,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("METAR processing failed: %s", exc)
                update_feed_state(
                    conn,
                    feed_name=METAR_FEED,
                    last_fetch=utc_now_iso(),
                    last_error=str(exc),
                    last_error_at=utc_now_iso(),
                )
            polls[METAR_FEED].next_run = now + polls[METAR_FEED].interval

        sleep_for = max(1.0, min(state.next_run for state in polls.values()) - time.time())
        STOP_EVENT.wait(timeout=sleep_for)

    LOGGER.info("Shutdown complete")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aviation Hub data ingestor")
    parser.add_argument("--once", action="store_true", help="Run each feed once and exit")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    configure_logging()
    STOP_EVENT.clear()
    signal.signal(signal.SIGINT, _request_shutdown)
    signal.signal(signal.SIGTERM, _request_shutdown)

    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open("w", encoding="utf-8") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            LOGGER.error("Another ingestor instance is already running; exiting")
            return 1

        with get_connection() as conn:
            init_db(conn)
            with requests.Session() as session:
                session.headers.update({"User-Agent": "aviation-hub/1.0"})
                exit_code = run_cycle(conn, session, once=args.once)
            conn.execute("PRAGMA wal_checkpoint(FULL);")
            return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
