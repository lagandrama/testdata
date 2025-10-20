from __future__ import annotations

import datetime as dt
import os
from typing import Optional, List
import json
import pathlib
import time
import random

import structlog

from ..models import UnifiedRow
from ..utils import iso_date, seconds_to_minutes

logger = structlog.get_logger()


def _coalesce(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


# --- capture/cache mode config ---
GARMIN_MODE = (os.getenv("GARMIN_MODE") or "online").lower()  # online | capture
GARMIN_CACHE_DIR = pathlib.Path(os.getenv("GARMIN_CACHE_DIR") or "./data/garmin_cache")


def _cache_path(kind: str, day: dt.date) -> pathlib.Path:
    GARMIN_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return GARMIN_CACHE_DIR / f"{kind}-{day.isoformat()}.json"


def _read_cache(kind: str, day: dt.date) -> dict:
    p = _cache_path(kind, day)
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _min_to_hhmm(m: Optional[int]) -> Optional[str]:
    if m is None:
        return None
    m = int(m)
    h, mm = divmod(m, 60)
    return f"{h:02d}:{mm:02d}"


def _only_hms(ts: Optional[str]) -> Optional[str]:
    if not ts:
        return None
    try:
        # accept formats like HH:MM:SS or ISO and return HH:MM:SS
        if "T" in ts:
            from datetime import datetime
            t = datetime.fromisoformat(ts)
            return t.strftime("%H:%M:%S")
        parts = ts.split(":")
        if len(parts) >= 2:
            hh = int(parts[0]); mm = int(parts[1]); ss = int(parts[2]) if len(parts) > 2 else 0
            return f"{hh:02d}:{mm:02d}:{ss:02d}"
    except Exception:
        return None
    return None


def _extract_number(val) -> Optional[int]:
    """Return an int if possible. Handles dict forms like {value: 79, qualifierKey: "FAIR"}."""
    try:
        if val is None:
            return None
        # dict with common numeric carrier keys
        if isinstance(val, dict):
            for k in ("value", "score", "overall", "overallScore", "numeric", "number"):
                if k in val:
                    return _extract_number(val[k])
            return None
        # list/tuple – pick first numeric
        if isinstance(val, (list, tuple)):
            for x in val:
                n = _extract_number(x)
                if n is not None:
                    return n
            return None
        # numbers
        if isinstance(val, (int, float)):
            try:
                return int(val)
            except Exception:
                return None
        # strings
        if isinstance(val, str):
            s = val.strip()
            # try int
            if s.isdigit():
                return int(s)
            # try float
            try:
                f = float(s)
                return int(f)
            except Exception:
                return None
    except Exception:
        return None
    return None


def _login_client():
    """
    Try garminconnect (new) with either garth session or username/password.
    We keep it simple and rely on env GARMIN_USERNAME/GARMIN_PASSWORD if present.
    """
    try:
        from garminconnect import Garmin
    except Exception as e:
        raise RuntimeError(f"garminconnect is not installed or failed to import: {e}")

    user = os.getenv("GARMIN_USERNAME")
    pwd = os.getenv("GARMIN_PASSWORD")
    if not user or not pwd:
        raise RuntimeError("GARMIN_USERNAME/GARMIN_PASSWORD not set in environment")

    client = Garmin(user, pwd)

    def _do_login():
        client.login()

    _retry_with_backoff(_do_login, label="garmin_login")
    return client


def _fetch_daily(client, day: dt.date) -> dict:
    def _call():
        try:
            return client.get_user_summary(day.isoformat())  # type: ignore[attr-defined]
        except Exception:
            return client.get_stats(day.isoformat())  # fallback

    try:
        return _retry_with_backoff(_call, label="garmin_daily") or {}
    except Exception:
        return {}


def _fetch_sleep(client, day: dt.date) -> dict:
    def _call():
        try:
            return client.get_sleep_data(day.isoformat())  # type: ignore[attr-defined]
        except Exception:
            return client.get_sleep(day.isoformat())

    try:
        return _retry_with_backoff(_call, label="garmin_sleep") or {}
    except Exception:
        return {}


def _is_rate_limited_error(err: Exception) -> bool:
    msg = str(err).lower()
    if "429" in msg or "rate limit" in msg or "1015" in msg:
        return True
    # Some http-like exceptions expose status
    code = getattr(err, "status_code", None) or getattr(err, "code", None)
    try:
        if code and int(code) in (429, 1015):
            return True
    except Exception:
        pass
    return False


def _retry_with_backoff(fn, label: str, *, max_attempts: int = 6, base_delay: float = 1.0, max_delay: float = 60.0):
    """
    Exponential backoff with jitter. Retries on any exception, but logs when rate limited.
    Delays: ~1s,2s,4s,8s,16s,32s (capped), with +/- jitter.
    """
    attempt = 0
    last_err: Exception | None = None
    while attempt < max_attempts:
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last_err = e
            attempt += 1
            is_rl = _is_rate_limited_error(e)
            delay = min(max_delay, base_delay * (2 ** (attempt - 1)))
            jitter = random.uniform(0.25, 1.25)
            sleep_s = delay * jitter
            logger.warning(
                "garmin_retry",
                extra={
                    "label": label,
                    "attempt": attempt,
                    "max": max_attempts,
                    "rate_limited": is_rl,
                    "sleep_sec": round(sleep_s, 2),
                    "error": str(e),
                },
            )
            time.sleep(sleep_s)
    if last_err is not None:
        raise last_err
    return None


def fetch_day(day: dt.date) -> List[List[Optional[str | float | int]]]:
    """
    Fetch Garmin daily metrics for a given calendar day and map to UnifiedRow.
    We aim for: bedtime, wake_time, sleep_duration_min (HH:MM), sleep_score, rhr_bpm (lowest),
    hrv_ms (not available -> None), readiness_or_body_battery_score (body battery),
    steps, active_calories.
    """
    if GARMIN_MODE == "capture":
        daily = _read_cache("daily", day)
        sleep = _read_cache("sleep", day)
        if not daily and not sleep:
            raise RuntimeError(
                "Capture files not found. Run: python -m health_sync.playwright.garmin_capture"
            )
    else:
        client = _login_client()
        try:
            daily = _fetch_daily(client, day) or {}
            sleep = _fetch_sleep(client, day) or {}
        finally:
            try:
                client.logout()
            except Exception:
                pass

    # Parse sleep
    # garminconnect sleep structure varies; try common fields
    bedtime = None
    waketime = None
    duration_min = None
    sleep_score = None
    lowest_hr = None

    try:
        if isinstance(sleep, dict):
            sw = sleep.get("sleepWindow") or sleep.get("dailySleepDTO") or sleep.get("sleepSummary") or {}
            # common variants
            bedtime = _coalesce(sw.get("sleepStartTimestampLocal"), sw.get("startTimeGMT"), sw.get("sleepStartTime"))
            waketime = _coalesce(sw.get("sleepEndTimestampLocal"), sw.get("endTimeGMT"), sw.get("sleepEndTime"))
            # duration sec or min depending on field
            dur_sec = _coalesce(sw.get("sleepTimeSeconds"), sw.get("sleepDuration"), sw.get("duration"))
            if dur_sec is not None:
                try:
                    dur_sec = int(dur_sec)
                except Exception:
                    dur_sec = None
            if dur_sec is not None:
                duration_min = seconds_to_minutes(dur_sec)
            raw_score = _coalesce(sw.get("sleepScores", {}).get("overall"), sw.get("overallScore"), sw.get("sleepScore"))
            sleep_score = _extract_number(raw_score)
            lowest_hr = _coalesce(sw.get("lowestHeartRate"), sw.get("minHeartRate"))
    except Exception:
        pass

    # Daily activity
    steps = None
    active_cal = None
    readiness_or_bb = None
    try:
        if isinstance(daily, dict):
            steps = _coalesce(daily.get("steps"), daily.get("totalSteps"))
            active_cal = _coalesce(daily.get("activeKilocalories"), daily.get("activeCalories"))
            # Body Battery score may be present in a separate object; attempt common keys
            raw_bb = _coalesce(
                daily.get("bodyBatteryOverallValue"),
                daily.get("bodyBatteryMax"),
                daily.get("bodyBatteryOverall"),
            )
            readiness_or_bb = _extract_number(raw_bb)
    except Exception:
        pass

    # Normalize types
    try: steps = int(steps) if steps is not None else None
    except Exception: steps = None
    try: active_cal = int(active_cal) if active_cal is not None else None
    except Exception: active_cal = None
    try: lowest_hr = int(lowest_hr) if lowest_hr is not None else None
    except Exception: lowest_hr = None
    # sleep_score and body battery already normalized via _extract_number

    hhmm = _min_to_hhmm(duration_min)

    has_any = any(v is not None for v in (bedtime, waketime, hhmm, sleep_score, lowest_hr, steps, active_cal, readiness_or_bb))
    if not has_any:
        return []

    row = UnifiedRow(
        date=iso_date(day),
        source="garmin",
        bedtime=_only_hms(bedtime),
        wake_time=_only_hms(waketime),
        sleep_duration_min=hhmm,
        sleep_score=sleep_score,  # type: ignore[arg-type]
        rhr_bpm=lowest_hr,
        hrv_ms=None,
        readiness_or_body_battery_score=readiness_or_bb,  # type: ignore[arg-type]
        steps=steps,
        active_calories=active_cal,
        activity_score=None,
    ).as_row()

    return [row]


