from __future__ import annotations

import argparse
import fcntl
import logging
import os
import signal
import sqlite3
import time
from dataclasses import dataclass
from http.server import ThreadingHTTPServer
from pathlib import Path
from threading import Event

import requests

from db import get_connection, init_db, update_feed_state
from fetchers.atis import FEED_NAME as ATIS_FEED, process_atis
from fetchers.metar import FEED_NAME as METAR_FEED, process_metar
from fetchers.ourairports import FEED_NAME as OURAIRPORTS_FEED, process_ourairports
from fetchers.sigmet import FEED_NAME as SIGMET_FEED, process_sigmet
from fetchers.taf import FEED_NAME as TAF_FEED, process_taf
from fetchers.ingest_vatsim_atc_bookings import (
    FEED_NAME as VATSIM_BOOKINGS_FEED,
    process_vatsim_atc_bookings,
)
from fetchers.ingest_vatsim_events import FEED_NAME as VATSIM_EVENTS_FEED, process_vatsim_events
from fetchers.vatsim import FEED_NAME as VATSIM_FEED, next_poll_seconds, process_vatsim_network
from util import configure_logging, utc_now_iso
from widget_server import start_widget_server

LOGGER = logging.getLogger("aviation_hub.main")
STOP_EVENT = Event()
LOCK_PATH = Path(__file__).resolve().parent.parent / "data" / "ingestor.lock"


def _env_poll_seconds(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or not str(raw).strip():
        return default
    try:
        return max(30, int(str(raw).strip()))
    except ValueError:
        LOGGER.warning("Invalid integer for %s=%r; using default %s", name, raw, default)
        return default


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

        try:
            LOGGER.info("Checking %s", TAF_FEED)
            process_taf(conn, session)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("TAF processing failed: %s", exc)
            update_feed_state(
                conn,
                feed_name=TAF_FEED,
                last_fetch=utc_now_iso(),
                last_error=str(exc),
                last_error_at=utc_now_iso(),
            )

        try:
            LOGGER.info("Checking %s", SIGMET_FEED)
            process_sigmet(conn, session)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("SIGMET processing failed: %s", exc)
            update_feed_state(
                conn,
                feed_name=SIGMET_FEED,
                last_fetch=utc_now_iso(),
                last_error=str(exc),
                last_error_at=utc_now_iso(),
            )

        try:
            LOGGER.info("Checking %s", VATSIM_EVENTS_FEED)
            process_vatsim_events(conn, session)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("VATSIM events processing failed: %s", exc)
            update_feed_state(
                conn,
                feed_name=VATSIM_EVENTS_FEED,
                last_fetch=utc_now_iso(),
                last_error=str(exc),
                last_error_at=utc_now_iso(),
            )

        try:
            LOGGER.info("Checking %s", VATSIM_BOOKINGS_FEED)
            process_vatsim_atc_bookings(conn, session)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("VATSIM ATC bookings processing failed: %s", exc)
            update_feed_state(
                conn,
                feed_name=VATSIM_BOOKINGS_FEED,
                last_fetch=utc_now_iso(),
                last_error=str(exc),
                last_error_at=utc_now_iso(),
            )

        try:
            LOGGER.info("Checking %s", OURAIRPORTS_FEED)
            process_ourairports(conn, session)
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("OurAirports processing failed: %s", exc)
            update_feed_state(
                conn,
                feed_name=OURAIRPORTS_FEED,
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
        TAF_FEED: PollState(interval=1800, next_run=0.0),
        SIGMET_FEED: PollState(interval=1200, next_run=0.0),
        VATSIM_EVENTS_FEED: PollState(
            interval=_env_poll_seconds("VATSIM_EVENTS_POLL_SECONDS", 900),
            next_run=0.0,
        ),
        VATSIM_BOOKINGS_FEED: PollState(
            interval=_env_poll_seconds("VATSIM_BOOKINGS_POLL_SECONDS", 300),
            next_run=0.0,
        ),
        OURAIRPORTS_FEED: PollState(interval=3600, next_run=0.0),
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

        if now >= polls[TAF_FEED].next_run:
            try:
                LOGGER.info("Checking %s", TAF_FEED)
                process_taf(conn, session)
                LOGGER.info(
                    "%s check complete; next check in %ss",
                    TAF_FEED,
                    polls[TAF_FEED].interval,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("TAF processing failed: %s", exc)
                update_feed_state(
                    conn,
                    feed_name=TAF_FEED,
                    last_fetch=utc_now_iso(),
                    last_error=str(exc),
                    last_error_at=utc_now_iso(),
                )
            polls[TAF_FEED].next_run = now + polls[TAF_FEED].interval

        if now >= polls[SIGMET_FEED].next_run:
            try:
                LOGGER.info("Checking %s", SIGMET_FEED)
                process_sigmet(conn, session)
                LOGGER.info(
                    "%s check complete; next check in %ss",
                    SIGMET_FEED,
                    polls[SIGMET_FEED].interval,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("SIGMET processing failed: %s", exc)
                update_feed_state(
                    conn,
                    feed_name=SIGMET_FEED,
                    last_fetch=utc_now_iso(),
                    last_error=str(exc),
                    last_error_at=utc_now_iso(),
                )
            polls[SIGMET_FEED].next_run = now + polls[SIGMET_FEED].interval

        if now >= polls[VATSIM_EVENTS_FEED].next_run:
            try:
                LOGGER.info("Checking %s", VATSIM_EVENTS_FEED)
                process_vatsim_events(conn, session)
                LOGGER.info(
                    "%s check complete; next check in %ss",
                    VATSIM_EVENTS_FEED,
                    polls[VATSIM_EVENTS_FEED].interval,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("VATSIM events processing failed: %s", exc)
                update_feed_state(
                    conn,
                    feed_name=VATSIM_EVENTS_FEED,
                    last_fetch=utc_now_iso(),
                    last_error=str(exc),
                    last_error_at=utc_now_iso(),
                )
            polls[VATSIM_EVENTS_FEED].next_run = now + polls[VATSIM_EVENTS_FEED].interval

        if now >= polls[VATSIM_BOOKINGS_FEED].next_run:
            try:
                LOGGER.info("Checking %s", VATSIM_BOOKINGS_FEED)
                process_vatsim_atc_bookings(conn, session)
                LOGGER.info(
                    "%s check complete; next check in %ss",
                    VATSIM_BOOKINGS_FEED,
                    polls[VATSIM_BOOKINGS_FEED].interval,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("VATSIM ATC bookings processing failed: %s", exc)
                update_feed_state(
                    conn,
                    feed_name=VATSIM_BOOKINGS_FEED,
                    last_fetch=utc_now_iso(),
                    last_error=str(exc),
                    last_error_at=utc_now_iso(),
                )
            polls[VATSIM_BOOKINGS_FEED].next_run = now + polls[VATSIM_BOOKINGS_FEED].interval

        if now >= polls[OURAIRPORTS_FEED].next_run:
            try:
                LOGGER.info("Checking %s", OURAIRPORTS_FEED)
                process_ourairports(conn, session)
                LOGGER.info(
                    "%s check complete; next check in %ss",
                    OURAIRPORTS_FEED,
                    polls[OURAIRPORTS_FEED].interval,
                )
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("OurAirports processing failed: %s", exc)
                update_feed_state(
                    conn,
                    feed_name=OURAIRPORTS_FEED,
                    last_fetch=utc_now_iso(),
                    last_error=str(exc),
                    last_error_at=utc_now_iso(),
                )
            polls[OURAIRPORTS_FEED].next_run = now + polls[OURAIRPORTS_FEED].interval

        sleep_for = max(1.0, min(state.next_run for state in polls.values()) - time.time())
        STOP_EVENT.wait(timeout=sleep_for)

    LOGGER.info("Shutdown complete")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aviation Hub data ingestor")
    parser.add_argument("--once", action="store_true", help="Run each feed once and exit")
    parser.add_argument("--widget-host", default="0.0.0.0", help="Widget HTTP bind host")
    parser.add_argument("--widget-port", type=int, default=4010, help="Widget HTTP bind port")
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

        widget_server: ThreadingHTTPServer | None = None
        if not args.once:
            try:
                widget_server = start_widget_server(host=args.widget_host, port=args.widget_port)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Widget HTTP server failed to start: %s", exc)

        with get_connection() as conn:
            init_db(conn)
            with requests.Session() as session:
                session.headers.update({"User-Agent": "aviation-hub/1.0"})
                exit_code = run_cycle(conn, session, once=args.once)
            conn.execute("PRAGMA wal_checkpoint(FULL);")
            if widget_server is not None:
                widget_server.shutdown()
                widget_server.server_close()
            return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
