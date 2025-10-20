from __future__ import annotations

import datetime as dt
import os
from typing import Optional, List, Any
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


def _min_to_hhmm(m: Optional[int | float]) -> Optional[str]:
    if m is None:
        return None
    try:
        m = int(round(float(m)))
        h, mm = divmod(m, 60)
        return f"{h:02d}:{mm:02d}"
    except Exception:
        return None


def _to_hm(ts: Optional[Any], *, shift_hours: int = -1) -> Optional[str]:
    """
    Vrati 'HH:MM' iz:
      - ISO stringa (…T…)
      - 'HH:MM(:SS)' stringa
      - epoch sekundi ili milisekundi (int/float)
    i pomakni rezultat za shift_hours (default -1h).
    """
    if ts is None:
        return None
    try:
        # numeric epoch
        if isinstance(ts, (int, float)):
            val = float(ts)
            # heuristika: ms ako je > 1e12
            if val > 1e12:
                val /= 1000.0
            dtobj = dt.datetime.fromtimestamp(val) + dt.timedelta(hours=shift_hours)
            return dtobj.strftime("%H:%M")
        # string
        s = str(ts)
        if "T" in s:
            dtobj = dt.datetime.fromisoformat(s.replace("Z", "+00:00")) + dt.timedelta(hours=shift_hours)
            return dtobj.strftime("%H:%M")
        parts = s.split(":")
        if len(parts) >= 2:
            hh = int(parts[0]); mm = int(parts[1])
            base = dt.datetime(2000, 1, 1, hh, mm) + dt.timedelta(hours=shift_hours)
            return base.strftime("%H:%M")
    except Exception:
        return None
    return None


def _extract_number(val) -> Optional[int]:
    try:
        if val is None:
            return None
        if isinstance(val, dict):
            for k in ("value", "score", "overall", "overallScore", "numeric", "number", "avg"):
                if k in val:
                    return _extract_number(val[k])
            return None
        if isinstance(val, (list, tuple)):
            for x in val:
                n = _extract_number(x)
                if n is not None:
                    return n
            return None
        if isinstance(val, (int, float)):
            return int(val)
        if isinstance(val, str):
            s = val.strip()
            if s.isdigit():
                return int(s)
            try:
                return int(float(s))
            except Exception:
                return None
    except Exception:
        return None
    return None


def _login_client():
    try:
        from garminconnect import Garmin
    except Exception as e:
        raise RuntimeError(f"garminconnect nije instaliran ili se ne može importati: {e}")

    user = os.getenv("GARMIN_USERNAME")
    pwd = os.getenv("GARMIN_PASSWORD")
    if not user or not pwd:
        raise RuntimeError("GARMIN_USERNAME/GARMIN_PASSWORD nisu postavljeni")

    client = Garmin(user, pwd)

    def _do_login():
        client.login()

    _retry_with_backoff(_do_login, label="garmin_login")
    return client


def _fetch_daily(client, day: dt.date) -> dict:
    def _call():
        try:
            return client.get_user_summary(day.isoformat())
        except Exception:
            return client.get_stats(day.isoformat())

    try:
        return _retry_with_backoff(_call, label="garmin_daily") or {}
    except Exception:
        return {}


def _fetch_sleep(client, day: dt.date) -> dict:
    def _call():
        try:
            return client.get_sleep_data(day.isoformat())
        except Exception:
            return client.get_sleep(day.isoformat())

    try:
        return _retry_with_backoff(_call, label="garmin_sleep") or {}
    except Exception:
        return {}


def _fetch_training_readiness(client, day: dt.date) -> dict:
    try:
        def _call():
            return client.get_training_readiness(day.isoformat())
        return _retry_with_backoff(_call, label="garmin_training_readiness") or {}
    except Exception:
        return {}


def _fetch_body_battery(client, day: dt.date) -> dict:
    try:
        def _call():
            return client.get_body_battery(day.isoformat())
        return _retry_with_backoff(_call, label="garmin_body_battery") or {}
    except Exception:
        return {}


def _fetch_hrv(client, day: dt.date) -> dict:
    try:
        def _call():
            return client.get_hrv_data(day.isoformat())
        return _retry_with_backoff(_call, label="garmin_hrv") or {}
    except Exception:
        return {}


def _is_rate_limited_error(err: Exception) -> bool:
    msg = str(err).lower()
    if "429" in msg or "rate limit" in msg or "1015" in msg:
        return True
    code = getattr(err, "status_code", None) or getattr(err, "code", None)
    try:
        if code and int(code) in (429, 1015):
            return True
    except Exception:
        pass
    return False


def _retry_with_backoff(fn, label: str, *, max_attempts: int = 6, base_delay: float = 1.0, max_delay: float = 60.0):
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
                extra={"label": label, "attempt": attempt, "max": max_attempts,
                       "rate_limited": is_rl, "sleep_sec": round(sleep_s, 2),
                       "error": str(e)},
            )
            time.sleep(sleep_s)
    if last_err is not None:
        raise last_err
    return None


def _max_body_battery_from_series(bb: dict | list | None) -> Optional[int]:
    """Vrati maksimalni Body Battery u danu iz raznih serija."""
    try:
        if not bb:
            return None
        series = None
        if isinstance(bb, dict):
            series = (
                bb.get("bodyBatteryValues")
                or bb.get("bodyBatteryTimeValuePairs")
                or bb.get("timeSeries")
            )
        if isinstance(series, list):
            vals = []
            for it in series:
                # mogu biti dictovi: {"value": 79, "time": ...} ili {"y": 79}
                vals.append(_extract_number(_coalesce(
                    it.get("value") if isinstance(it, dict) else None,
                    it.get("y") if isinstance(it, dict) else None,
                    it
                )))
            vals = [v for v in vals if v is not None]
            if vals:
                return max(vals)
        # fallbackovi kad nema serije
        if isinstance(bb, dict):
            return _extract_number(_coalesce(
                bb.get("overallValue"), bb.get("max"), bb.get("overall"),
                bb.get("highestValue"), bb.get("bodyBatteryOverall")
            ))
    except Exception:
        return None
    return None


def _extract_hrv_ms(daily: dict, sleep: dict, hrv: dict) -> Optional[int]:
    """Pokušaj pronaći nightly RMSSD prosjek kroz više mjesta."""
    try:
        # 1) direktno iz hrv payload-a
        n = _extract_number(_coalesce(
            hrv.get("avgRmssd") if isinstance(hrv, dict) else None,
            hrv.get("averageRmssd") if isinstance(hrv, dict) else None,
            hrv.get("rmssdAvg") if isinstance(hrv, dict) else None,
            hrv.get("nightlyAverage") if isinstance(hrv, dict) else None,
        ))
        if n is not None:
            return n
        # 2) ponekad daily ima hrvSummary
        if isinstance(daily, dict):
            n = _extract_number(_coalesce(
                (daily.get("hrvSummary") or {}).get("lastNightAvg"),
                (daily.get("hrvSummary") or {}).get("avgRmssd"),
                daily.get("averageHrv"),
                daily.get("hrvAverage"),
            ))
            if n is not None:
                return n
        # 3) nekad je u sleep strukturi
        if isinstance(sleep, dict):
            sw = _coalesce(
                sleep.get("sleepWindow"), sleep.get("dailySleepDTO"),
                sleep.get("sleepSummary"), sleep.get("sleepSummaryDTO"),
                sleep.get("sleepData"), {}
            )
            n = _extract_number(_coalesce(
                sw.get("avgRmssd"), sw.get("averageRmssd"),
                sw.get("hrvAverage"), sw.get("hrvAvg")
            ))
            if n is not None:
                return n
    except Exception:
        return None
    return None


def fetch_day(day: dt.date) -> List[List[Optional[str | float | int]]]:
    """
    Dnevni Garmin → UnifiedRow.
    Puni: bedtime (HH:MM), wake_time (HH:MM), sleep_duration_min (HH:MM), sleep_score,
          rhr_bpm (min/RHR), hrv_ms (ako dostupno), readiness_or_body_battery_score (MAX),
          steps, active_calories, activity_score (Training Readiness ako postoji).
    """
    if GARMIN_MODE == "capture":
        daily = _read_cache("daily", day)
        sleep = _read_cache("sleep", day)
        tr    = _read_cache("training_readiness", day)
        bb    = _read_cache("body_battery", day)
        hrv   = _read_cache("hrv", day)
        if not daily and not sleep:
            raise RuntimeError("Capture datoteke nisu nađene.")
    else:
        client = _login_client()
        try:
            daily = _fetch_daily(client, day) or {}
            sleep = _fetch_sleep(client, day) or {}
            tr    = _fetch_training_readiness(client, day) or {}
            bb    = _fetch_body_battery(client, day) or {}
            hrv   = _fetch_hrv(client, day) or {}
        finally:
            try:
                client.logout()
            except Exception:
                pass

    # ---------- SLEEP ----------
    bedtime = None
    waketime = None
    dur_min = None
    sleep_score = None
    lowest_hr = None

    try:
        if isinstance(sleep, dict):
            sw = _coalesce(
                sleep.get("sleepWindow"), sleep.get("dailySleepDTO"),
                sleep.get("sleepSummary"), sleep.get("sleepSummaryDTO"),
                sleep.get("sleepData"), {}
            )
            bedtime = _coalesce(
                sw.get("sleepStartTimestampLocal"), sw.get("sleepStartTimeLocal"),
                sw.get("sleepStartTime"), sw.get("startTimeLocal"), sw.get("startTimeGMT")
            )
            waketime = _coalesce(
                sw.get("sleepEndTimestampLocal"), sw.get("sleepEndTimeLocal"),
                sw.get("sleepEndTime"), sw.get("endTimeLocal"), sw.get("endTimeGMT")
            )
            dur_sec = _coalesce(sw.get("sleepTimeSeconds"), sw.get("sleepDuration"), sw.get("duration"))
            if dur_sec is not None:
                try:
                    dur_sec = int(dur_sec)
                    dur_min = seconds_to_minutes(dur_sec)
                except Exception:
                    dur_min = None

            raw_score = _coalesce(
                (sw.get("sleepScores") or {}).get("overall"),
                sw.get("overallScore"), sw.get("sleepScore")
            )
            sleep_score = _extract_number(raw_score)

            lowest_hr = _coalesce(
                sw.get("lowestHeartRate"), sw.get("minHeartRate"),
                sleep.get("lowestHeartRate")
            )
    except Exception:
        pass

    # ---------- DAILY ----------
    steps = None
    active_cal = None
    bb_score = None
    try:
        if isinstance(daily, dict):
            steps = _coalesce(daily.get("steps"), daily.get("totalSteps"))
            active_cal = _coalesce(daily.get("activeKilocalories"), daily.get("activeCalories"))
            # fallback BB iz dailyja
            bb_score = _extract_number(_coalesce(
                daily.get("bodyBatteryOverallValue"), daily.get("bodyBatteryMax"),
                daily.get("bodyBatteryOverall"),
            ))
            lowest_hr = _coalesce(lowest_hr, daily.get("restingHeartRate"))
    except Exception:
        pass

    # ---------- TRAINING READINESS / BODY BATTERY / HRV ----------
    activity_score = None
    try:
        if isinstance(tr, dict):
            tr_overall = _extract_number(_coalesce(tr.get("overallScore"), tr.get("overall")))
            if tr_overall is not None:
                activity_score = tr_overall
                bb_score = _coalesce(bb_score, tr_overall)  # kao readiness fallback
    except Exception:
        pass

    # MAX body battery iz serije
    try:
        mx = _max_body_battery_from_series(bb)
        if mx is not None:
            bb_score = mx
    except Exception:
        pass

    # HRV prosjek (RMSSD)
    hrv_ms = _extract_hrv_ms(daily, sleep, hrv)

    # Normalizacija
    try: steps = int(steps) if steps is not None else None
    except Exception: steps = None
    try: active_cal = int(active_cal) if active_cal is not None else None
    except Exception: active_cal = None
    try: lowest_hr = int(lowest_hr) if lowest_hr is not None else None
    except Exception: lowest_hr = None

    # format (pomak -1h)
    bedtime_hm  = _to_hm(bedtime,  shift_hours=-1)
    waketime_hm = _to_hm(waketime, shift_hours=-1)
    dur_hhmm    = _min_to_hhmm(dur_min)

    has_any = any(v is not None for v in (
        bedtime_hm, waketime_hm, dur_hhmm, sleep_score, lowest_hr, hrv_ms, bb_score, steps, active_cal, activity_score
    ))
    if not has_any:
        return []

    row = UnifiedRow(
        date=iso_date(day),
        source="garmin",
        bedtime=bedtime_hm,
        wake_time=waketime_hm,
        sleep_duration_min=dur_hhmm,
        sleep_score=sleep_score,  # type: ignore[arg-type]
        rhr_bpm=lowest_hr,
        hrv_ms=hrv_ms,
        readiness_or_body_battery_score=bb_score,  # type: ignore[arg-type]
        health_score=None,
        steps=steps,
        active_calories=active_cal,
        activity_score=activity_score,
    ).as_row()

    return [row]
