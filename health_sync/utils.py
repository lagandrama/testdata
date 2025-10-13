from __future__ import annotations

import datetime as dt
from decimal import Decimal, ROUND_HALF_UP
from typing import Any, Callable, Iterable, Optional

import pytz
import structlog
from tenacity import RetryCallState, retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .config import get_settings

logger = structlog.get_logger()


def get_tz() -> pytz.BaseTzInfo:
    return pytz.timezone(get_settings().TZ)


def iso_date(d: dt.date | dt.datetime) -> str:
    if isinstance(d, dt.datetime):
        d = d.date()
    return d.isoformat()


def round_2dp(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    q = Decimal(str(value)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return float(q)


def seconds_to_minutes(value_seconds: Optional[float]) -> Optional[int]:
    if value_seconds is None:
        return None
    return int(round(value_seconds / 60))


def meters_to_km(value_meters: Optional[float]) -> Optional[float]:
    if value_meters is None:
        return None
    return round_2dp(value_meters / 1000.0)


def speed_kmh_to_pace_min_per_km(kmh: Optional[float]) -> Optional[float]:
    if kmh is None or kmh == 0:
        return None
    minutes_per_km = 60.0 / kmh
    return round_2dp(minutes_per_km)


def mps_to_speed_and_pace(mps: Optional[float]) -> tuple[Optional[float], Optional[float]]:
    if mps is None or mps == 0:
        return None, None
    kmh = mps * 3.6
    pace = speed_kmh_to_pace_min_per_km(kmh)
    return round_2dp(kmh), pace


def redact(value: Optional[str]) -> str:
    if not value:
        return ""
    return value[:4] + "…" if len(value) > 8 else "***"


def retry_backoff(max_attempts: int = 5, base: float = 1.0) -> Callable[[Callable[..., Any]], Any]:
    def _before_log(retry_state: RetryCallState) -> None:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        logger.warning(
            "retrying",
            attempt=retry_state.attempt_number,
            error=str(exc) if exc else None,
        )

    def decorator(fn: Callable[..., Any]) -> Any:
        return retry(
            reraise=True,
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=base, min=base, max=base * 8),
            retry=retry_if_exception_type((Exception,)),
            before=_before_log,
        )(fn)

    return decorator


NORMALIZED_WORKOUT_TYPES = {
    "run": "run",
    "running": "run",
    "ride": "ride",
    "cycling": "ride",
    "bike": "ride",
    "swim": "swim",
    "strength": "strength",
    "weight_training": "strength",
    "walk": "walk",
    "hike": "hike",
    "yoga": "yoga",
}


def normalize_workout_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    key = value.replace("-", "_").replace(" ", "_").lower()
    return NORMALIZED_WORKOUT_TYPES.get(key, "other")


