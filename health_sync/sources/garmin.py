# sources/garmin.py
from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import structlog
from playwright.sync_api import sync_playwright

from ..models import UnifiedRow
from ..utils import iso_date, seconds_to_minutes
from ..config import get_settings

logger = structlog.get_logger()

BASE_MODERN = "https://connect.garmin.com/modern"
BASE_API    = "https://connectapi.garmin.com"
DEBUG = os.getenv("GARMIN_DEBUG") == "1"

# ------------- helpers -------------
def _min_to_hhmm(m: Optional[int]) -> Optional[str]:
    if m is None: return None
    m = int(m); h, mm = divmod(m, 60)
    return f"{h:02d}:{mm:02d}"

def _coalesce(*vals):
    for v in vals:
        if v is not None:
            return v
    return None

def _parse_ms(ms: Optional[int]) -> Optional[dt.datetime]:
    if ms is None: return None
    try:
        return dt.datetime.utcfromtimestamp(int(ms) / 1000.0)
    except Exception:
        return None

def _only_hms_from_ms(ms: Optional[int]) -> Optional[str]:
    t = _parse_ms(ms)
    return t.strftime("%H:%M:%S") if t else None

def _state_path() -> Path:
    return Path(get_settings().GARMIN_STORAGE_STATE or "./state/garmin.json")

def _load_state() -> dict:
    p = _state_path()
    if not p.exists():
        raise RuntimeError(f"GARMIN_STORAGE_STATE not found: {p}")
    return json.loads(p.read_text(encoding="utf-8"))

def _token_and_fgp() -> tuple[Optional[str], Optional[str]]:
    js = _load_state()
    token = None
    fgp = None
    for origin in js.get("origins", []):
        for kv in origin.get("localStorage", []):
            if kv.get("name") == "token":
                try:
                    token = json.loads(kv["value"]).get("access_token")
                except Exception:
                    pass
    for ck in js.get("cookies", []):
        if ck.get("name") == "JWT_FGP":
            fgp = ck.get("value")
            break
    return token, fgp

def _headers() -> Dict[str, str]:
    token, fgp = _token_and_fgp()
    h = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://connect.garmin.com",
        "Referer": f"{BASE_MODERN}/",
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
        "x-app-id": "com.garmin.connect.web",
        "NK": "NT",
        "Accept-Language": "en-US,en;q=0.9",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
        h["di-auth"] = f"Bearer {token}"
    if fgp:
        h["DI-DEVICE-ID"] = fgp
        h["DI-APP-PLATFORM"] = "web"
    return h

def _urls(path: str) -> List[str]:
    # path like "/proxy/wellness-service/wellness/dailySummary"
    # try modern + connectapi variant
    api_path = path.removeprefix("/proxy")
    return [f"{BASE_MODERN}{path}", f"{BASE_API}{api_path}"]

def _fetch_json(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    One request context (Playwright) and try modern + connectapi.
    """
    st = str(_state_path())
    with sync_playwright() as pw:
        ctx = pw.request.new_context(storage_state=st, extra_http_headers=_headers())
        for url in _urls(path):
            resp = ctx.get(url, params=params)
            ok = resp.ok
            txt = ""
            try:
                txt = resp.text()[:160]
            except Exception:
                pass
            if DEBUG:
                logger.info("garmin_req", url=url, status=resp.status, ok=ok, peek=txt)
            if not ok:
                continue
            try:
                js = resp.json()
            except Exception:
                js = None
            # treat {} / [] as empty
            if isinstance(js, dict) and not js:
                continue
            if isinstance(js, list) and not js:
                continue
            if js is not None:
                return js
        return None

# ------------- extractors -------------
def _extract_sleep(js: Any) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[int], Optional[int]]:
    if not isinstance(js, dict):
        return None, None, None, None, None

    start_ms = _coalesce(js.get("sleepStartTimestampGMT"),
                         js.get("sleepStartTimestampUTC"),
                         js.get("overallSleepStartTimestamp"))
    end_ms   = _coalesce(js.get("sleepEndTimestampGMT"),
                         js.get("sleepEndTimestampUTC"),
                         js.get("overallSleepEndTimestamp"))
    dur_sec  = _coalesce(js.get("durationInSeconds"),
                         js.get("sleepTimeSeconds"),
                         js.get("sleepingSeconds"))
    try: dur_min = int(seconds_to_minutes(int(dur_sec))) if dur_sec is not None else None
    except Exception: dur_min = None

    score = _coalesce(js.get("overallSleepScore"), js.get("sleepScore"))
    try: score = int(score) if score is not None else None
    except Exception: score = None

    lowest = _coalesce(js.get("lowestHeartRate"),
                       js.get("lowestRespirationHeartRate"),
                       js.get("minHeartRate"))
    try: lowest = int(lowest) if lowest is not None else None
    except Exception: lowest = None

    return (_only_hms_from_ms(start_ms),
            _only_hms_from_ms(end_ms),
            dur_min, score, lowest)

def _extract_daily_summary(js: Any) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    if not isinstance(js, dict):
        return None, None, None
    steps = _coalesce(js.get("steps"), js.get("totalSteps"))
    kcals = _coalesce(js.get("activeKilocalories"), js.get("activeCalories"))
    rhr   = _coalesce(js.get("restingHeartRate"), js.get("minHeartRate"))
    try: steps = int(steps) if steps is not None else None
    except Exception: steps = None
    try: kcals = int(kcals) if kcals is not None else None
    except Exception: kcals = None
    try: rhr   = int(rhr)   if rhr   is not None else None
    except Exception: rhr = None
    return steps, kcals, rhr

def _extract_usersummary(js: Any) -> Dict[str, Any]:
    if not isinstance(js, dict):
        return {}
    out: Dict[str, Any] = {}
    out["steps"] = _coalesce(js.get("steps"), js.get("totalSteps"))
    out["activeKilocalories"] = _coalesce(js.get("activeKilocalories"), js.get("activeCalories"))
    out["restingHeartRate"] = _coalesce(js.get("restingHeartRate"), js.get("minHeartRate"))
    out["sleepStartTimestampGMT"] = _coalesce(js.get("sleepStartTimestampGMT"), js.get("sleepStartTimestampUTC"))
    out["sleepEndTimestampGMT"]   = _coalesce(js.get("sleepEndTimestampGMT"),   js.get("sleepEndTimestampUTC"))
    out["sleepTimeSeconds"]       = _coalesce(js.get("sleepTimeSeconds"),       js.get("sleepingSeconds"))
    out["sleepScore"] = js.get("sleepScore")
    return out

def _extract_hrv(js: Any) -> Optional[int]:
    if not isinstance(js, dict):
        return None
    cand = _coalesce(js.get("avgRmssd"), js.get("rmssd"), js.get("averageRmssd"))
    try:
        return int(round(float(cand))) if cand is not None else None
    except Exception:
        return None

def _extract_body_battery(js: Any) -> Optional[int]:
    if not isinstance(js, dict):
        return None
    val = _coalesce(js.get("mostRecentValue"),
                    js.get("mostRecent"),
                    js.get("bodyBatteryMostRecent"),
                    js.get("bodyBatteryMax"))
    try:
        return int(val) if val is not None else None
    except Exception:
        return None

# ------------- public -------------
def fetch_day(day: dt.date) -> List[List[Optional[str | float | int]]]:
    bedtime = waketime = None
    sleep_score = None
    lowest_hr_sleep = None
    sleep_dur_hhmm = None
    steps = None
    active_cals = None
    rhr = None
    hrv_ms = None
    readiness_or_bb = None

    # Sleep
    sleep_js = (_fetch_json("/proxy/wellness-service/wellness/dailySleepData", {"date": day.isoformat()})
                or _fetch_json(f"/proxy/wellness-service/wellness/dailySleepData/{day.isoformat()}"))
    if DEBUG: logger.info("garmin_sleep_raw", date=str(day), has=bool(sleep_js))
    if sleep_js:
        bt, wt, dur_min, sc, lowest = _extract_sleep(sleep_js)
        bedtime, waketime = bt, wt
        sleep_score, lowest_hr_sleep = sc, lowest
        sleep_dur_hhmm = _min_to_hhmm(dur_min)

    # Daily summary
    daily_js = (_fetch_json("/proxy/wellness-service/wellness/dailySummary", {"date": day.isoformat()})
                or _fetch_json(f"/proxy/wellness-service/wellness/dailySummary/{day.isoformat()}"))
    if DEBUG: logger.info("garmin_daily_raw", date=str(day), has=bool(daily_js))
    if daily_js:
        s, ac, r = _extract_daily_summary(daily_js)
        steps = steps or s
        active_cals = active_cals or ac
        rhr = rhr or r
        if lowest_hr_sleep is None:
            lowest_hr_sleep = _coalesce(daily_js.get("minHeartRate"), daily_js.get("lowestHeartRate"))

    # Fallback: usersummary-service
    if steps is None or (bedtime is None and waketime is None and sleep_dur_hhmm is None):
        us_js = _fetch_json("/proxy/usersummary-service/usersummary/daily",
                            {"calendarDate": day.isoformat()})
        if DEBUG: logger.info("garmin_usersummary_raw", date=str(day), has=bool(us_js))
        if us_js:
            m = _extract_usersummary(us_js)
            if steps is None and m.get("steps") is not None:
                try: steps = int(m["steps"])
                except Exception: pass
            if active_cals is None and m.get("activeKilocalories") is not None:
                try: active_cals = int(m["activeKilocalories"])
                except Exception: pass
            if rhr is None and m.get("restingHeartRate") is not None:
                try: rhr = int(m["restingHeartRate"])
                except Exception: pass
            if bedtime is None or waketime is None or sleep_dur_hhmm is None:
                bt = _only_hms_from_ms(m.get("sleepStartTimestampGMT"))
                wt = _only_hms_from_ms(m.get("sleepEndTimestampGMT"))
                dur_s = m.get("sleepTimeSeconds")
                dur_hhmm = _min_to_hhmm(int(seconds_to_minutes(dur_s)) if dur_s is not None else None)
                bedtime  = bedtime  or bt
                waketime = waketime or wt
                sleep_dur_hhmm = sleep_dur_hhmm or dur_hhmm
                if sleep_score is None and m.get("sleepScore") is not None:
                    try: sleep_score = int(m["sleepScore"])
                    except Exception: pass

    # HRV
    hrv_js = (_fetch_json("/proxy/wellness-service/wellness/dailyHrv", {"date": day.isoformat()})
              or _fetch_json("/proxy/wellness-service/wellness/hrv", {"date": day.isoformat()}))
    if DEBUG: logger.info("garmin_hrv_raw", date=str(day), has=bool(hrv_js))
    hrv_ms = _extract_hrv(hrv_js) if hrv_js else None
    if rhr is None and isinstance(hrv_js, dict):
        rhr = _coalesce(hrv_js.get("restingHeartRate"), rhr)

    # Body Battery
    bb_js = (_fetch_json("/proxy/wellness-service/wellness/bodyBattery", {"date": day.isoformat()})
             or _fetch_json(f"/proxy/wellness-service/wellness/bodyBattery/{day.isoformat()}"))
    if DEBUG: logger.info("garmin_bb_raw", date=str(day), has=bool(bb_js))
    readiness_or_bb = _extract_body_battery(bb_js) if bb_js else None

    rhr_final = _coalesce(lowest_hr_sleep, rhr)

    has_any = any(v is not None for v in (
        bedtime, waketime, sleep_dur_hhmm, sleep_score,
        rhr_final, hrv_ms, steps, active_cals, readiness_or_bb
    ))
    if not has_any:
        return []

    row = UnifiedRow(
        date=iso_date(day),
        source="garmin",
        bedtime=bedtime,
        wake_time=waketime,
        sleep_duration_min=sleep_dur_hhmm,
        sleep_score=sleep_score,
        rhr_bpm=rhr_final,
        hrv_ms=hrv_ms,
        readiness_or_body_battery_score=readiness_or_bb,
        steps=steps,
        active_calories=active_cals,
        activity_score=None,
    )
    return [row.as_row()]
