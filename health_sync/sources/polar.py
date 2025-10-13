# sources/polar.py
from __future__ import annotations

import datetime as dt
import os
from typing import Optional, List, Any, Dict, Tuple

import httpx
import structlog
from ..config import get_settings

logger = structlog.get_logger()

BASE_URL = "https://www.polaraccesslink.com/v3"
DEBUG = os.getenv("POLAR_DEBUG") == "1"


# --------------------------- Auth ---------------------------

def _ensure_access_token() -> str:
    token = get_settings().POLAR_ACCESS_TOKEN
    if not token:
        raise RuntimeError("POLAR_ACCESS_TOKEN nije postavljen.")
    return token


def _auth_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_ensure_access_token()}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


# --------------------------- Helpers ---------------------------

def _parse_iso(ts: Optional[str]) -> Optional[dt.datetime]:
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(ts)
    except Exception:
        return None


def parse_iso_to_local_time(ts: Optional[str]) -> str:
    t = _parse_iso(ts)
    return t.strftime("%H:%M:%S") if t else ""


def _list_payload_items(js: Any) -> List[dict]:
    """Vrati listu itema ako je response list/obj s listom pod uobičajenim ključevima."""
    if not js:
        return []
    if isinstance(js, list):
        return js
    if isinstance(js, dict):
        for key in (
            "data", "items", "sleep", "nights", "summaries",
            "recharges",                         # <— GLAVNI FIX
            "activity-log", "activities", "activity", "results",
            "activity_summary", "summary", "metrics"
        ):
            v = js.get(key)
            if isinstance(v, list):
                return v
    return []


def _pick_record_for_day(items: List[dict], day: dt.date,
                         date_field: Optional[str] = None,
                         start_keys=("sleep_start_time","start_time","bedtime_start"),
                         end_keys=("sleep_end_time","end_time","bedtime_end")) -> dict:
    if not items:
        return {}

    if date_field:
        for it in items:
            if it.get(date_field) == day.isoformat():
                return it

    start_day = dt.datetime.combine(day, dt.time(0, 0, 0))
    end_day = start_day + dt.timedelta(days=1)

    def _end_ts(it: dict) -> Optional[dt.datetime]:
        for k in end_keys:
            if it.get(k):
                t = _parse_iso(it.get(k))
                return t.replace(tzinfo=None) if t else None
        return None

    cand = [it for it in items if (et := _end_ts(it)) and (start_day <= et < end_day)]
    if cand:
        def _dur_s(it: dict) -> int:
            for k in ("total_sleep_time", "actual_sleep_time", "duration"):
                try:
                    return int(it.get(k) or 0)
                except Exception:
                    pass
            return 0
        cand.sort(key=_dur_s)
        return cand[-1]

    # ako nema jasnog end_time, izaberi s najvećim preklopom u danu
    def _overlap_sec(it: dict) -> float:
        s = None; e = None
        for k in start_keys:
            if it.get(k):
                s = _parse_iso(it.get(k)); break
        for k in end_keys:
            if it.get(k):
                e = _parse_iso(it.get(k)); break
        if not s or not e:
            return -1.0
        s, e = s.replace(tzinfo=None), e.replace(tzinfo=None)
        a = max(s, start_day); b = min(e, end_day)
        return max(0.0, (b - a).total_seconds())

    items.sort(key=_overlap_sec)
    best = items[-1]
    return best if _overlap_sec(best) > 0 else {}


# ---------- Deep search (value + JSON path) ----------

def _deep_find(obj: Any, keys: Tuple[str, ...]) -> Tuple[Optional[Any], Optional[str]]:
    """Vrati prvu vrijednost za bilo koji ključ u `keys` i JSON path do nje."""
    def visit(o: Any, path: str) -> Tuple[Optional[Any], Optional[str]]:
        if isinstance(o, dict):
            for k in keys:  # direktni pogodak
                if k in o and o[k] is not None:
                    return o[k], f"{path}.{k}" if path else k
            for k, v in o.items():  # rekurzija
                val, p = visit(v, f"{path}.{k}" if path else k)
                if p is not None:
                    return val, p
        elif isinstance(o, list):
            for i, v in enumerate(o):
                val, p = visit(v, f"{path}[{i}]")
                if p is not None:
                    return val, p
        return None, None
    return visit(obj, "")


# --------------------------- HTTP helpers ---------------------------

def _safe_json(r: httpx.Response) -> Any:
    if r.status_code in (204, 205):
        return None
    try:
        return r.json()
    except Exception:
        return None


def _get_json(client: httpx.Client, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    r = client.get(f"{BASE_URL}{path}", headers=_auth_headers(), params=params)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return _safe_json(r)


# --------------------------- Data getters ---------------------------

def get_sleep(day: dt.date) -> dict:
    with httpx.Client(timeout=30) as client:
        js = _get_json(client, f"/users/sleep/{day.isoformat()}")
        if js is None:
            js = _get_json(client, "/users/sleep",
                           params={"start_date": (day - dt.timedelta(days=1)).isoformat(),
                                   "end_date":   (day + dt.timedelta(days=1)).isoformat()})
        items = _list_payload_items(js)
        obj = _pick_record_for_day(items, day) if items else (js or {})
        if DEBUG:
            logger.info("polar_sleep_raw", date=str(day), raw=obj)
        return obj or {}


def get_recharge(day: dt.date) -> dict:
    with httpx.Client(timeout=30) as client:
        js = _get_json(client, "/users/nightly-recharge", params={"date": day.isoformat()})
        if js is None:
            js = _get_json(client, "/users/nightly-recharge",
                           params={"start_date": (day - dt.timedelta(days=1)).isoformat(),
                                   "end_date":   (day + dt.timedelta(days=1)).isoformat()})
        items = _list_payload_items(js)           # <— sada vidi i 'recharges'
        obj = _pick_record_for_day(items, day, date_field="date") if items else (js or {})
        if DEBUG:
            logger.info("polar_recharge_raw", date=str(day), raw=obj)
        return obj or {}


def get_daily_activity(day: dt.date) -> dict:
    with httpx.Client(timeout=30) as client:
        js = _get_json(client, "/users/daily-activity", params={"date": day.isoformat()})
        if js is None:
            js = _get_json(client, "/users/daily-activity",
                           params={"start_date": day.isoformat(),
                                   "end_date":   (day + dt.timedelta(days=1)).isoformat()})
        if not _list_payload_items(js) and not isinstance(js, dict):
            js = _get_json(client, f"/users/daily-activity/{day.isoformat()}")

        items = _list_payload_items(js)
        obj = None
        if items:
            obj = next((it for it in items if it.get("date") == day.isoformat()), items[-1])
        elif isinstance(js, dict):
            obj = js

        if DEBUG:
            logger.info("polar_activity_raw", date=str(day), raw=(obj or {}))
        return obj or {}


# --------------------------- Public API ---------------------------

def fetch_day(day: dt.date) -> List[List[Any]]:
    # sirovi podaci
    sleep = get_sleep(day)
    recharge = get_recharge(day)
    activity = get_daily_activity(day)

    date_str = day.isoformat()
    source = "polar"

    # --- SLEEP ---
    bedtime = parse_iso_to_local_time(sleep.get("sleep_start_time")) if sleep else ""
    wake_time = parse_iso_to_local_time(sleep.get("sleep_end_time")) if sleep else ""

    sleep_duration = ""
    if sleep:
        try:
            total_sec = int(sleep.get("light_sleep", 0)) + int(sleep.get("deep_sleep", 0)) + int(sleep.get("rem_sleep", 0))
            sleep_duration = round(total_sec / 3600, 2)   # sati s 2 dec
        except Exception:
            pass

    sleep_score = sleep.get("sleep_score", "") if sleep else ""

    rhr_bpm = ""
    if sleep and isinstance(sleep.get("heart_rate_samples"), dict) and sleep["heart_rate_samples"]:
        try:
            vals = [int(v) for v in sleep["heart_rate_samples"].values() if v is not None]
            if vals:
                rhr_bpm = min(vals)
        except Exception:
            pass
    if rhr_bpm == "":
        rhr_bpm = sleep.get("heart_rate_avg", "") if sleep else ""

    # --- RECHARGE: HRV + Readiness ---
    hrv_ms, hrv_path = _deep_find(recharge, ("heart_rate_variability_avg", "rmssd_ms", "rmssd", "hrv"))
    readiness, readiness_path = _deep_find(recharge, ("ans_charge", "overall_score", "recharge_score", "score", "nightly_recharge_status"))

    # normaliziraj tipove
    def _to_int(x):
        try:
            return int(round(float(x)))
        except Exception:
            return x

    hrv_ms = "" if hrv_ms is None else _to_int(hrv_ms)
    readiness = "" if readiness is None else _to_int(readiness)

    if DEBUG:
        logger.info("polar_recharge_extracted",
                    date=str(day), hrv_ms=hrv_ms, hrv_path=hrv_path,
                    readiness=readiness, readiness_path=readiness_path)

    # --- ACTIVITY: steps + active calories (nema fallbacka na total) ---
    steps, steps_path = _deep_find(activity, ("steps", "daily_steps", "step_count"))
    active_calories, acal_path = _deep_find(activity, ("active_calories", "active-calories", "caloriesActive"))

    steps = "" if steps is None else _to_int(steps)
    active_calories = "" if active_calories is None else _to_int(active_calories)

    if DEBUG:
        logger.info("polar_activity_extracted",
                    date=str(day), steps=steps, steps_path=steps_path,
                    active_calories=active_calories, active_calories_path=acal_path)

    activity_score = ""
    sc, sc_path = _deep_find(activity, ("activity_score",))
    if sc is not None:
        activity_score = _to_int(sc)
        if DEBUG:
            logger.info("polar_activity_score_extracted", date=str(day), score=activity_score, path=sc_path)

    # --- Workout polja (prazno) ---
    row = [
        date_str, source,
        bedtime, wake_time, sleep_duration, sleep_score, rhr_bpm,
        hrv_ms, readiness, steps, active_calories, activity_score,
        "", "", "", "", "", "", "", "",
        f"polar:{date_str}",
    ]

    # ako je sve ključno prazno – ne vraćaj ništa
    if all(v in ("", None) for v in row[2:12]):
        return []
    return [row]
