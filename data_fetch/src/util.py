from __future__ import annotations

import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, TypeVar

T = TypeVar("T")


LOGGER = logging.getLogger("aviation_hub")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None

    if "." in text:
        head, tail = text.split(".", 1)
        frac_digits = []
        rest_start = 0
        for idx, ch in enumerate(tail):
            if ch.isdigit():
                frac_digits.append(ch)
                continue
            rest_start = idx
            break
        else:
            rest_start = len(tail)

        if frac_digits:
            frac = "".join(frac_digits)[:6]
            text = f"{head}.{frac}{tail[rest_start:]}"

    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    else:
        parsed = parsed.astimezone(timezone.utc)
    return parsed


def normalize_iso_utc(value: str | None) -> str | None:
    parsed = parse_iso_utc(value)
    if parsed is None:
        return None
    return parsed.isoformat().replace("+00:00", "Z")


def normalize_vatsim_api_time(value: Any) -> str | None:
    """Parse ISO-8601 or 'YYYY-MM-DD HH:MM:SS' (assumed UTC) from VATSIM-style APIs."""
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        try:
            parsed = datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OSError, OverflowError, ValueError):
            return None
        return parsed.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    text = str(value).strip()
    if not text:
        return None
    iso = normalize_iso_utc(text)
    if iso:
        return iso
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return parsed.isoformat().replace("+00:00", "Z")


def json_dumps_compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True, sort_keys=True)


def extract_airport_from_callsign(callsign: str | None) -> str | None:
    if not callsign:
        return None
    token = callsign.strip().upper().split("_", 1)[0]
    if len(token) == 4 and token.isalnum():
        return token
    return None


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def with_retries(
    func: Callable[[], T],
    *,
    attempts: int = 3,
    base_delay: float = 1.0,
    context: str = "operation",
) -> T:
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:  # noqa: BLE001 - resilience around external I/O
            last_error = exc
            if attempt >= attempts:
                break
            sleep_for = base_delay * (2 ** (attempt - 1))
            LOGGER.warning(
                "%s failed on attempt %s/%s (%s). Retrying in %.1fs",
                context,
                attempt,
                attempts,
                exc,
                sleep_for,
            )
            time.sleep(sleep_for)

    assert last_error is not None
    raise last_error
