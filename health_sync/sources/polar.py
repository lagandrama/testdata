# sources/polar.py
from __future__ import annotations

import datetime as dt
import os
from typing import Optional, List, Any, Dict, Tuple

import httpx
import structlog

from ..models import UnifiedRow
from ..utils import iso_date, seconds_to_minutes
from ..config import get_settings

logger = structlog.get_logger()

BASE_URL = "https://www.polaraccesslink.com/v3"
FLOW_BASE = "https://flow.polar.com"
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


def _user_id() -> str:
    uid = getattr(get_settings(), "POLAR_USER_ID", 63062572)
    if not uid:
        raise RuntimeError("POLAR_USER_ID nije postavljen u konfiguraciji.")
    return str(uid)


# --------------------------- Helpers ---------------------------

def _parse_iso(ts: Optional[str]) -> Optional[dt.datetime]:
    if not ts:
        return None
    try:
        return dt.datetime.fromisoformat(ts)
    except Exception:
        return None


def _only_hms(ts: Optional[str]) -> Optional[str]:
    t = _parse_iso(ts)
    return t.strftime("%H:%M:%S") if t else None


def _coalesce(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


def _min_to_hhmm(m: Optional[int]) -> Optional[str]:
    if m is None:
        return None
    m = int(m)
    h, mm = divmod(m, 60)
    return f"{h:02d}:{mm:02d}"


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


def _post_empty(client: httpx.Client, path: str) -> httpx.Response:
    return client.post(f"{BASE_URL}{path}", headers=_auth_headers())


def _put_empty(client: httpx.Client, abs_url: str) -> httpx.Response:
    return client.put(abs_url, headers=_auth_headers())


def _list_payload_items(js: Any) -> List[dict]:
    if not js:
        return []
    if isinstance(js, list):
        return js
    if isinstance(js, dict):
        for key in ("data", "items", "sleep", "nights", "summaries", "activities", "physical_activities"):
            v = js.get(key)
            if isinstance(v, list):
                return v
    return []


# --------------------------- Extractors ---------------------------

def _extract_sleep_fields(obj: dict) -> tuple[
    Optional[str], Optional[str], Optional[int], Optional[int], Optional[int]
]:
    """(asleep_start_iso, asleep_end_iso, duration_sec, sleep_score, lowest_hr)"""
    if not isinstance(obj, dict) or not obj:
        return None, None, None, None, None

    start = _coalesce(obj.get("sleep_start_time"), obj.get("start_time"), obj.get("bedtime_start"))
    end   = _coalesce(obj.get("sleep_end_time"),   obj.get("end_time"),   obj.get("bedtime_end"))

    dur_s = _coalesce(obj.get("total_sleep_time"), obj.get("actual_sleep_time"), obj.get("duration"))
    try:
        dur_s = int(dur_s) if dur_s is not None else None
    except Exception:
        dur_s = None

    if dur_s is None:
        try:
            dur_s = sum(int(obj.get(k) or 0) for k in ("light_sleep", "deep_sleep", "rem_sleep")) or None
        except Exception:
            dur_s = None

    score = _coalesce(obj.get("sleep_score"), obj.get("score"))

    lowest_hr = _coalesce(obj.get("lowest_heart_rate"), obj.get("lowest_hr"), obj.get("lowest_hrt"))
    if lowest_hr is None:
        samples = obj.get("heart_rate_samples")
        if isinstance(samples, dict) and samples:
            try:
                lowest_hr = min(int(v) for v in samples.values() if v is not None)
            except Exception:
                lowest_hr = None

    try:
        lowest_hr = int(lowest_hr) if lowest_hr is not None else None
    except Exception:
        lowest_hr = None

    return start, end, dur_s, score, lowest_hr


def _pick_record_for_day(items: List[dict], day: dt.date) -> dict:
    if not items:
        return {}

    start_day = dt.datetime.combine(day, dt.time(0, 0, 0))
    end_day = start_day + dt.timedelta(days=1)

    def _end_ts(it: dict) -> Optional[dt.datetime]:
        t = _parse_iso(_coalesce(it.get("sleep_end_time"), it.get("end_time"), it.get("bedtime_end")))
        return t.replace(tzinfo=None) if t else None

    cand = [it for it in items if (et := _end_ts(it)) and (start_day <= et < end_day)]
    if cand:
        cand.sort(key=lambda it: int(_coalesce(it.get("total_sleep_time"), it.get("actual_sleep_time"), it.get("duration"), 0)))
        return cand[-1]

    def overlap_sec(it: dict) -> float:
        s = _parse_iso(_coalesce(it.get("sleep_start_time"), it.get("start_time"), it.get("bedtime_start")))
        e = _parse_iso(_coalesce(it.get("sleep_end_time"),   it.get("end_time"),   it.get("bedtime_end")))
        if not s or not e:
            return -1.0
        s, e = s.replace(tzinfo=None), e.replace(tzinfo=None)
        a = max(s, start_day); b = min(e, end_day)
        return max(0.0, (b - a).total_seconds())

    items.sort(key=lambda it: (overlap_sec(it), int(_coalesce(it.get("total_sleep_time"), it.get("duration"), 0))))
    best = items[-1]
    if overlap_sec(best) <= 0:
        return {}
    return best


def _extract_activity_fields(obj: dict) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """(steps, active_calories, resting_hr) iz daily summary-ja."""
    steps = _coalesce(obj.get("steps"), obj.get("step_count"), obj.get("stepCount"))
    kcals = _coalesce(
        obj.get("active_calories"), obj.get("calories_active"),
        obj.get("calories"), obj.get("calories_exercise"), obj.get("activeCalories")
    )
    rhr   = _coalesce(obj.get("resting_heart_rate"), obj.get("resting_hr"), obj.get("lowest_resting_hr"))
    try: steps = int(steps) if steps is not None else None
    except Exception: steps = None
    try: kcals = int(kcals) if kcals is not None else None
    except Exception: kcals = None
    try: rhr   = int(rhr)   if rhr   is not None else None
    except Exception: rhr = None
    return steps, kcals, rhr


def _extract_recharge_fields(obj: dict) -> tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Vrati (hrv_ms, resting_hr, readiness_score) iz Nightly Recharge.
    Pokriva varijante: obj, obj['ans'], obj['ans_charge'], obj['recharge']['ans'].
    """
    if not isinstance(obj, dict) or not obj:
        return None, None, None

    candidates = [obj]
    for key in ("ans", "ans_charge"):
        if isinstance(obj.get(key), dict):
            candidates.append(obj[key])
    if isinstance(obj.get("recharge"), dict) and isinstance(obj["recharge"].get("ans"), dict):
        candidates.append(obj["recharge"]["ans"])

    hrv = None
    rhr = None
    readiness = None

    for o in candidates:
        if hrv is None:
            cand = _coalesce(o.get("rmssd"), o.get("rmssd_ms"), o.get("hrv"), o.get("heart_rate_variability_avg"))
            try: hrv = int(cand) if cand is not None else None
            except Exception: hrv = None
        if rhr is None:
            cand = _coalesce(o.get("resting_hr"), o.get("resting_heart_rate"))
            try: rhr = int(cand) if cand is not None else None
            except Exception: rhr = None
        if readiness is None:
            cand = _coalesce(
                o.get("ans_charge"),
                o.get("ans_charge_score"),
                o.get("overall_score"),
                o.get("recharge_score"),
                o.get("score"),
                o.get("nightly_recharge_status")
            )
            try:
                readiness = int(cand) if (cand is not None and str(cand).isdigit()) else cand
            except Exception:
                readiness = None

    return hrv, rhr, readiness


# --------------------------- Activity: novi v3 endpoint ---------------------------

def _fetch_activity_v3(client: httpx.Client, day: dt.date) -> Tuple[Optional[int], Optional[int]]:
    """
    Primarno dohvaćanje (steps, active_calories) preko novog v3 endpointa:
      GET /users/activities/{date}?steps=true
    """
    try:
        js = _get_json(client, f"/users/activities/{day.isoformat()}", params={"steps": "true"})
    except Exception:
        js = None

    if DEBUG:
        logger.info("polar_activity_v3_raw", date=str(day), raw=js)

    if not js:
        return None, None

    # Endpoint može vratiti dict ili list (ovisno o verziji/rangu)
    item: dict | None = None
    if isinstance(js, dict):
        item = js
    else:
        items = _list_payload_items(js)
        item = next((it for it in items if it.get("date") == day.isoformat()), items[-1] if items else None)

    if not item:
        return None, None

    steps, kcals, _ = _extract_activity_fields(item)
    return steps, kcals


# --------------------------- Activity: fallback preko transakcija ---------------------------

def _fetch_activity_via_transactions(client: httpx.Client, day: dt.date) -> Tuple[Optional[int], Optional[int]]:
    """
    Pokušaj dohvatiti (steps, active_calories) preko activity-transactions.
    """
    uid = _user_id()
    r = _post_empty(client, f"/users/{uid}/activity-transactions")
    if r.status_code not in (200, 201, 204):
        if DEBUG:
            logger.info("polar_activity_tx_create_failed", status=r.status_code, body=_safe_json(r))
        return None, None

    tx_url = r.headers.get("Location")
    if not tx_url:
        if DEBUG:
            logger.info("polar_activity_tx_no_location", headers=dict(r.headers))
        return None, None

    steps, active = None, None

    for suffix in ("/activities", "/physical-activities"):
        try:
            g = client.get(f"{tx_url}{suffix}", headers=_auth_headers(),
                           params={"from-date": day.isoformat(), "to-date": day.isoformat()})
            if g.status_code == 404:
                g = client.get(f"{tx_url}{suffix}", headers=_auth_headers())
            g.raise_for_status()
            js = _safe_json(g)
        except Exception:
            js = None

        if DEBUG:
            logger.info("polar_activity_tx_payload", endpoint=f"{tx_url}{suffix}", raw=js)

        items = _list_payload_items(js)
        if not items and isinstance(js, dict):
            items = js.get("activities") or js.get("physical_activities") or []

        for it in items:
            it_date = it.get("date")
            if not it_date:
                st = _parse_iso(it.get("start_time") or it.get("created"))
                it_date = st.date().isoformat() if st else None
            if it_date and it_date != day.isoformat():
                continue

            s, kcals, _ = _extract_activity_fields(it)

            if s is None and isinstance(it.get("samples"), list):
                try:
                    s = sum(int(samp.get("steps") or samp.get("step_count") or 0) for samp in it["samples"])
                except Exception:
                    pass
            if kcals is None and isinstance(it.get("samples"), list):
                try:
                    ac = [int(samp.get("activeCalories") or samp.get("caloriesActive") or 0) for samp in it["samples"]]
                    kc = sum(ac) if any(ac) else None
                    kcals = kc if kc is not None else kcals
                except Exception:
                    pass

            steps = steps or s
            active = active or kcals

        if steps is not None or active is not None:
            break

    try:
        _put_empty(client, tx_url)
    except Exception:
        pass

    return steps, active


# --------------------------- Fallback: Flow loadFour (samo steps) ---------------------------

def _fetch_steps_via_flow(day: dt.date) -> Optional[int]:
    """
    Fallback na Flow privatni API:
      GET /api/activity-timeline/loadFour?day=YYYY-MM-DD&maxSampleCount=200
    Očekuje cookie sesiju:
      - POLAR_FLOW_SESSION (FLOW_SESSION)
      - (opcionalno) POLAR_PLAY_SESSION_FLOW (PLAY_SESSION_FLOW)
    """
    sess = getattr(get_settings(), "POLAR_FLOW_SESSION", None)  # cookie FLOW_SESSION
    play_sess = getattr(get_settings(), "POLAR_PLAY_SESSION_FLOW", None)  # cookie PLAY_SESSION_FLOW
    if not sess:
        return None

    cookies = {"FLOW_SESSION": sess}
    if play_sess:
        cookies["PLAY_SESSION_FLOW"] = play_sess
    cookies.setdefault("timezone", "120")

    url = f"{FLOW_BASE}/api/activity-timeline/loadFour"
    params = {"day": day.isoformat(), "maxSampleCount": 200}
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{FLOW_BASE}/diary/activity",
        "User-Agent": "python-httpx"
    }

    try:
        with httpx.Client(timeout=30, headers=headers, cookies=cookies) as c:
            r = c.get(url, params=params)
            if r.status_code != 200:
                if DEBUG:
                    logger.info("flow_steps_http_error", status=r.status_code, text=r.text[:300])
                return None
            js = r.json()
    except Exception as e:
        if DEBUG:
            logger.info("flow_steps_error", err=str(e))
        return None

    day_key = day.isoformat()
    if not isinstance(js, dict) or day_key not in js:
        return None
    node = js.get(day_key) or {}
    panel = node.get("dataPanelData") or {}
    steps = panel.get("stepCount")
    try:
        return int(steps) if steps is not None else None
    except Exception:
        return None


# --------------------------- Public API ---------------------------

def fetch_day(day: dt.date) -> list[list[Optional[str | float | int]]]:
    """
    Dohvati Polar podatke za zadani dan i mapiraj u UnifiedRow.
    - Sleep -> /users/sleep
    - Nightly Recharge -> /users/nightly-recharge
    - Steps/Active calories -> /users/activities/{date}?steps=true (primarno),
      pa activity-transactions, pa Flow fallback (samo steps)
    """
    with httpx.Client(timeout=30) as client:
        # ---------- SLEEP ----------
        obj_sleep: dict = {}
        js_sleep = _get_json(client, f"/users/sleep/{day.isoformat()}")
        if js_sleep is None:
            js_sleep = _get_json(
                client, "/users/sleep",
                params={"start_date": (day - dt.timedelta(days=1)).isoformat(),
                        "end_date":   (day + dt.timedelta(days=1)).isoformat()}
            )
            items = _list_payload_items(js_sleep)
            obj_sleep = _pick_record_for_day(items, day)
        else:
            obj_sleep = js_sleep or {}

        if DEBUG:
            logger.info("polar_sleep_raw", date=str(day), raw=obj_sleep)

        start, end, dur_s, score, lowest_hr_sleep = _extract_sleep_fields(obj_sleep)

        bedtime  = _only_hms(start)
        waketime = _only_hms(end)
        dur_hhmm = _min_to_hhmm(seconds_to_minutes(dur_s))  # HH:MM

        steps: Optional[int] = None
        active_cals: Optional[int] = None
        rhr = lowest_hr_sleep
        hrv_ms: Optional[int] = None
        readiness = None

        # ---------- NIGHTLY RECHARGE ----------
        js_nr = _get_json(client, "/users/nightly-recharge", params={"date": day.isoformat()})
        if js_nr is None:
            js_nr = _get_json(
                client, "/users/nightly-recharge",
                params={"start_date": (day - dt.timedelta(days=1)).isoformat(),
                        "end_date":   (day + dt.timedelta(days=1)).isoformat()}
            )
        if not _list_payload_items(js_nr) and not isinstance(js_nr, dict):
            js_nr = _get_json(client, f"/users/nightly-recharge/{day.isoformat()}")

        if DEBUG:
            logger.info("polar_recharge_raw", date=str(day), raw=js_nr)

        if js_nr:
            item = None
            items = _list_payload_items(js_nr)
            if items:
                item = next((it for it in items if it.get("date") == day.isoformat()), items[-1])
            elif isinstance(js_nr, dict):
                item = js_nr
            if item:
                hrv_candidate, rhr_candidate, readiness_candidate = _extract_recharge_fields(item)
                if hrv_ms is None: hrv_ms = hrv_candidate
                if rhr is None:     rhr    = rhr_candidate
                if readiness is None: readiness = readiness_candidate

        # ---------- ACTIVITY: primarno novi v3 endpoint ----------
        try:
            s, kcals = _fetch_activity_v3(client, day)
            if s is not None:
                steps = s
            if kcals is not None:
                active_cals = kcals  # samo 'active' – ne koristimo total
        except Exception as e:
            if DEBUG:
                logger.info("polar_activity_v3_error", err=str(e))

        # ---------- Fallback: transakcije ----------
        if steps is None and active_cals is None:
            try:
                s, kcals = _fetch_activity_via_transactions(client, day)
                if s is not None:
                    steps = s
                if kcals is not None:
                    active_cals = kcals
            except Exception as e:
                if DEBUG:
                    logger.info("polar_activity_tx_error", err=str(e))

    # ---------- Fallback: Flow (samo koraci) ----------
    if steps is None:
        try:
            steps = _fetch_steps_via_flow(day)
            if DEBUG:
                logger.info("flow_steps_fallback", date=str(day), steps=steps)
        except Exception:
            pass

    has_any = any(v is not None for v in (bedtime, waketime, dur_hhmm, score, rhr, hrv_ms, steps, active_cals, readiness))
    if not has_any:
        return []

    unified = UnifiedRow(
        date=iso_date(day),
        source="polar",
        bedtime=bedtime,
        wake_time=waketime,
        sleep_duration_min=dur_hhmm,   # HH:MM
        sleep_score=score,
        rhr_bpm=rhr,
        hrv_ms=hrv_ms,
        readiness_or_body_battery_score=readiness,
        steps=steps,
        active_calories=active_cals,   # ne prepisujemo totalnim kalorijama
        activity_score=None,
    )
    return [unified.as_row()]
