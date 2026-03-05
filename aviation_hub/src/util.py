from __future__ import annotations

import hashlib
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
