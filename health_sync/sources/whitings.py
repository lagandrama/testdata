# sources/whitings.py
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

BASE_URL = "https://wbsapi.withings.net"
DEBUG = os.getenv("WITHINGS_DEBUG") == "1"


# --------------------------- Auth ---------------------------

def _ensure_access_token() -> str:
    token = getattr(get_settings(), "WITHINGS_ACCESS_TOKEN", None) or os.getenv("WITHINGS_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("WITHINGS_ACCESS_TOKEN nije postavljen.")
    return token


def _auth_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_ensure_access_token()}",
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }


def _user_id() -> Optional[str]:
    uid = getattr(get_settings(), "WITHINGS_USER_ID", None) or os.getenv("WITHINGS_USER_ID")
    return str(uid) if uid else None


# --------------------------- Helpers ---------------------------

def _only_hms_from_unix(ts_unix: Optional[int]) -> Optional[str]:
    if ts_unix is None:
        return None
    try:
        return dt.datetime.fromtimestamp(int(ts_unix)).strftime("%H:%M:%S")
    except Exception:
        return None


def _min_to_hhmm(m: Optional[int]) -> Optional[str]:
    if m is None:
        return None
    m = int(m)
    h, mm = divmod(m, 60)
    return f"{h:02d}:{mm:02d}"


def _safe_json(r: httpx.Response) -> Any:
    try:
        return r.json()
    except Exception:
        return None


def _post_form(client: httpx.Client, path: str, form: Dict[str, Any]) -> Any:
    r = client.post(f"{BASE_URL}{path}", headers=_auth_headers(), data=form, timeout=30)
    r.raise_for_status()
    js = _safe_json(r)
    # Withings standard response: {"status":0, "body":{...}}
    if not isinstance(js, dict):
        return None
    if js.get("status") != 0:
        if DEBUG:
            logger.info("withings_api_nonzero_status", path=path, payload=form, resp=js)
        return None
    return js.get("body")


def _coalesce(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


# --------------------------- Extractors ---------------------------

def _extract_sleep_series_item(series_item: dict) -> Tuple[
    Optional[str], Optional[str], Optional[int], Optional[int], Optional[int]
]:
    """
    Iz Withings sleep getsummary 'series' item-a vrati:
    (bedtime_hms, wake_hms, sleep_duration_min, sleep_score, rhr_bpm)

    - bedtime/wake: iz startdate/enddate (unix)
    - trajanje: total_sleep_time (sek) -> minute
    - score: sleep_score
    - rhr: hr_min (minimum HR tokom sna) ako postoji, fallback na hr_average
    """
    if not isinstance(series_item, dict):
        return None, None, None, None, None

    s = series_item.get("startdate")      # unix
    e = series_item.get("enddate")        # unix
    data = series_item.get("data") or {}

    bedtime = _only_hms_from_unix(s)
    waketime = _only_hms_from_unix(e)

    total_sleep_sec = _coalesce(
        data.get("total_sleep_time"),
        data.get("asleepduration"),
        data.get("duration")
    )
    try:
        total_sleep_min = seconds_to_minutes(int(total_sleep_sec)) if total_sleep_sec is not None else None
    except Exception:
        total_sleep_min = None

    score = _coalesce(data.get("sleep_score"), data.get("score"))

    rhr = _coalesce(data.get("hr_min"), data.get("hr_average"))  # bpm
    try:
        rhr = int(rhr) if rhr is not None else None
    except Exception:
        rhr = None

    return bedtime, waketime, total_sleep_min, score, rhr


def _extract_activity_fields(day_item: dict) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    Iz Withings getactivity dana vrati (steps, active_calories, activity_score?)

    getactivity polja (ovisno o korisniku/uređaju):
      - steps
      - calories (ukupno)
      - soft/medium/intense (sekunde)
      - totalcalories / totalactive / elevation itd.
    Active calories nisu uvijek eksplicitne; koristimo:
      active_calories = data.get("calories")  (najčešće = aktivne, ne bazalni metabolizam)
    activity_score: Withings nema univerzalan score; ostavi None.
    """
    if not isinstance(day_item, dict):
        return None, None, None

    steps = day_item.get("steps")
    calories = _coalesce(day_item.get("calories"), day_item.get("caloriesactive"), day_item.get("totalcalories"))
    try:
        steps = int(steps) if steps is not None else None
    except Exception:
        steps = None
    try:
        calories = int(round(float(calories))) if calories is not None else None
    except Exception:
        calories = None
    return steps, calories, None


def _extract_weight_kg(measures_body: dict) -> Optional[float]:
    """
    Iz getmeas odgovora pronađi mjerenje type=1 (weight).
    Withings skala vraća 'value * 10^unit'. kg = value * 10^unit
    """
    if not isinstance(measures_body, dict):
        return None
    grps = measures_body.get("measuregrps") or []
    if not grps:
        return None
    # uzmi najnoviji za taj period
    grps = sorted(grps, key=lambda g: int(g.get("date") or 0))
    for grp in reversed(grps):
        for m in grp.get("measures", []):
            if int(m.get("type", -1)) == 1:
                try:
                    val = float(m.get("value"))
                    unit = int(m.get("unit", 0))
                    kg = val * (10 ** unit)
                    return round(kg, 2)
                except Exception:
                    pass
    return None


# --------------------------- Public API ---------------------------

def fetch_day(day: dt.date) -> list[list[Optional[str | float | int]]]:
    """
    Dohvati Withings podatke za zadani dan i mapiraj u UnifiedRow.
      - Sleep summary:  POST /v2/sleep?action=getsummary
      - Activity:       POST /v2/measure?action=getactivity
      - Body weight:    POST /measure?action=getmeas  (opcionalno; uzimamo zadnje mjerenje tog dana)

    Polja koja punimo:
      bedtime, wake_time, sleep_duration_min (HH:MM), sleep_score,
      rhr_bpm (sleep hr_min), hrv_ms=None,
      readiness_or_body_battery_score=None (Withings nema "readiness"; sleep_score ide u sleep_score),
      steps, active_calories, activity_score=None.
    """
    with httpx.Client() as client:
        # ---------- SLEEP SUMMARY ----------
        sleep_body = _post_form(
            client,
            "/v2/sleep",
            {
                "action": "getsummary",
                "lastupdate": 0,  # da dobijemo sve relevantno
                "startdateymd": day.isoformat(),
                "enddateymd": day.isoformat(),
                "data_fields": (
                    "total_sleep_time,asleepduration,sleep_score,hr_average,hr_min,hr_max,"
                    "total_timeinbed,wakeupcount"
                ),
            },
        )
        bedtime = waketime = None
        dur_hhmm = None
        sleep_score = None
        rhr_bpm = None

        if DEBUG:
            logger.info("withings_sleep_raw", date=str(day), raw=sleep_body)

        if isinstance(sleep_body, dict):
            series = sleep_body.get("series") or []
            # pokušaj naći entry s tim danom
            item = None
            for s in series:
                if isinstance(s, dict) and s.get("date") == day.isoformat():
                    item = s
                    break
            if not item and series:
                item = series[-1]

            if item:
                bt, wt, sleep_min, sc, rhr = _extract_sleep_series_item(item)
                bedtime, waketime = bt, wt
                dur_hhmm = _min_to_hhmm(sleep_min)
                sleep_score = sc
                rhr_bpm = rhr

        # ---------- ACTIVITY (steps, calories) ----------
        act_body = _post_form(
            client,
            "/v2/measure",
            {
                "action": "getactivity",
                "startdateymd": day.isoformat(),
                "enddateymd": day.isoformat(),
            },
        )
        steps = None
        active_cals = None
        activity_score = None
        if DEBUG:
            logger.info("withings_activity_raw", date=str(day), raw=act_body)

        if isinstance(act_body, dict):
            acts = act_body.get("activities") or []
            if acts:
                steps, active_cals, activity_score = _extract_activity_fields(acts[0])

        # ---------- WEIGHT (opcionalno; možeš ne koristiti u Sheet-u) ----------
        weight_kg = None
        meas_body = _post_form(
            client,
            "/measure",
            {
                "action": "getmeas",
                "startdate": int(dt.datetime.combine(day, dt.time.min).timestamp()),
                "enddate": int(dt.datetime.combine(day, dt.time.max).timestamp()),
            },
        )
        if DEBUG:
            logger.info("withings_getmeas_raw", date=str(day), raw=meas_body)

        weight_kg = _extract_weight_kg(meas_body)

    has_any = any(v is not None for v in (
        bedtime, waketime, dur_hhmm, sleep_score, rhr_bpm, steps, active_cals, activity_score, weight_kg
    ))
    if not has_any:
        return []

    unified = UnifiedRow(
        date=iso_date(day),
        source="withings",
        bedtime=bedtime,
        wake_time=waketime,
        sleep_duration_min=dur_hhmm,  # HH:MM
        sleep_score=sleep_score,
        rhr_bpm=rhr_bpm,
        hrv_ms=None,
        readiness_or_body_battery_score=None,
        steps=steps,
        active_calories=active_cals,
        activity_score=activity_score,
    )
    row = unified.as_row()

    # Ako želiš da u Sheet ide i težina, možeš je “parkirati” u neku slobodnu kolonu,
    # ali pošto UnifiedRow ima fiksan skup kolona, ovdje to ne ubacujemo.
    # Alternativa: dodaj u UnifiedRow novu kolonu, ili odvojen 'body' sheet.

    return [row]
