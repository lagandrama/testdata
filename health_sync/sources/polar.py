# sources/polar.py
from __future__ import annotations

import datetime as dt
import os
from typing import Optional, List, Tuple

import httpx
import structlog

from ..models import UnifiedRow
from ..utils import iso_date, seconds_to_minutes
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


def _get_json(client: httpx.Client, url: str, params: dict | None = None) -> Tuple[int, dict | list | None]:
    r = client.get(url, headers=_auth_headers(), params=params or {})
    if r.status_code == 404:
        return 404, None
    r.raise_for_status()
    try:
        return r.status_code, r.json()
    except Exception:
        return r.status_code, None


# --------------------------- Mapping: Sleep ---------------------------

def _extract_sleep_fields_polar(obj: dict) -> tuple[Optional[str], Optional[str], Optional[int], Optional[int]]:
    """
    Vrati (asleep_start_iso, asleep_end_iso, duration_sec, sleep_score) iz Polar sleep objekta.
    Podržava više naziva polja (različite verzije Polar podataka).
    """
    start = _coalesce(obj.get("sleep_start_time"), obj.get("start_time"), obj.get("bedtime_start"))
    end   = _coalesce(obj.get("sleep_end_time"),   obj.get("end_time"),   obj.get("bedtime_end"))
    dur_s = _coalesce(obj.get("total_sleep_time"), obj.get("actual_sleep_time"), obj.get("duration"))
    try:
        dur_s = int(dur_s) if dur_s is not None else None
    except Exception:
        pass
    score = _coalesce(obj.get("sleep_score"), obj.get("score"))
    return start, end, dur_s, score


def _pick_record_for_day(items: List[dict], day: dt.date) -> dict:
    """
    Odaberi zapis koji pripada danu:
    1) preferiramo onaj kojem 'end' pada unutar [00:00, +1d),
    2) fallback: maksimalni preklop sa danom.
    """
    if not items:
        return {}

    start_day = dt.datetime.combine(day, dt.time(0, 0, 0))
    end_day = start_day + dt.timedelta(days=1)

    def _end_ts(it: dict) -> Optional[dt.datetime]:
        t = _parse_iso(_coalesce(it.get("sleep_end_time"), it.get("end_time"), it.get("bedtime_end")))
        return t.replace(tzinfo=None) if t else None

    # 1) kraj u danu
    cand = [it for it in items if (et := _end_ts(it)) and (start_day <= et < end_day)]
    if cand:
        cand.sort(key=lambda it: int(_coalesce(it.get("total_sleep_time"), it.get("actual_sleep_time"), 0)))
        return cand[-1]

    # 2) fallback: najveći preklop
    def overlap_sec(it: dict) -> float:
        s = _parse_iso(_coalesce(it.get("sleep_start_time"), it.get("start_time"), it.get("bedtime_start")))
        e = _parse_iso(_coalesce(it.get("sleep_end_time"),   it.get("end_time"),   it.get("bedtime_end")))
        if not s or not e:
            return -1.0
        s, e = s.replace(tzinfo=None), e.replace(tzinfo=None)
        a = max(s, start_day)
        b = min(e, end_day)
        return max(0.0, (b - a).total_seconds())

    items.sort(key=lambda it: (overlap_sec(it), int(_coalesce(it.get("total_sleep_time"), 0))))
    return items[-1]


# --------------------------- Public API ---------------------------

def fetch_day(day: dt.date) -> list[list[Optional[str | float | int]]]:
    """
    Dohvati Polar sleep + daily activity + nightly recharge za dan
    i mapiraj u UnifiedRow:
      - bedtime / wake_time = stvarni početak/kraj sna
      - sleep_duration_min = 'hh:mm' (Time asleep = total_sleep_time)
      - steps / active_calories iz daily activity (ako postoji)
      - rhr_bpm / hrv_ms + readiness_or_body_battery_score iz Nightly Recharge (ako postoji)
    """
    rows: list[list[Optional[str | float | int]]] = []

    with httpx.Client(timeout=30) as client:
        # --- SLEEP ---
        status, js = _get_json(client, f"{BASE_URL}/users/sleep/{day.isoformat()}")
        if status == 404:
            status, js = _get_json(
                client,
                f"{BASE_URL}/users/sleep",
                params={"start_date": (day - dt.timedelta(days=1)).isoformat(),
                        "end_date":   (day + dt.timedelta(days=1)).isoformat()},
            )
            items: List[dict] = []
            if isinstance(js, dict):
                for key in ("sleep", "nights", "data", "items"):
                    if isinstance(js.get(key), list):
                        items = js[key]
                        break
            elif isinstance(js, list):
                items = js
            sleep_obj = _pick_record_for_day(items, day) if items else {}
        else:
            sleep_obj = js if isinstance(js, dict) else {}

        if DEBUG:
            logger.info("polar_sleep_raw", date=str(day), raw=sleep_obj)

        start, end, dur_s, sleep_score = _extract_sleep_fields_polar(sleep_obj)
        bedtime = _only_hms(start)
        waketime = _only_hms(end)
        sleep_duration_hhmm = _min_to_hhmm(seconds_to_minutes(dur_s))

        # --- DAILY ACTIVITY (best-effort) ---
        steps = None
        active_calories = None
        act_score = None
        status, act = _get_json(client, f"{BASE_URL}/users/activity/{day.isoformat()}")
        if isinstance(act, dict):
            steps = _coalesce(act.get("steps"), act.get("daily_steps"))
            active_calories = _coalesce(act.get("active_calories"), act.get("activity_calories"))
            act_score = _coalesce(act.get("activity_score"), act.get("score"))

        # --- NIGHTLY RECHARGE (best-effort) ---
        rhr_bpm = None
        hrv_ms = None
        readiness = None
        status, nr = _get_json(client, f"{BASE_URL}/users/nightly-recharge/{day.isoformat()}")
        if isinstance(nr, dict):
            # RHR
            rhr_bpm = _coalesce(nr.get("resting_heart_rate"), nr.get("rhr"), nr.get("avg_rhr"))
            # HRV (RMSSD u ms)
            hrv_ms = _coalesce(nr.get("hrv"), nr.get("avg_rmssd"), nr.get("rmssd"))
            # Readiness-like: različite varijante; pokušaj numeričke, fallback mapiranje statusa
            readiness = _coalesce(nr.get("overall_score"), nr.get("score"))
            if readiness is None:
                status_txt = _coalesce(nr.get("recharge_status"), nr.get("status"))
                if isinstance(status_txt, str):
                    m = {"POOR": 30, "OK": 60, "GOOD": 90}
                    readiness = m.get(status_txt.upper())

        unified = UnifiedRow(
            date=iso_date(day),
            source="polar",
            bedtime=bedtime,
            wake_time=waketime,
            sleep_duration_min=sleep_duration_hhmm,  # 'hh:mm'
            sleep_score=sleep_score,
            rhr_bpm=int(rhr_bpm) if rhr_bpm is not None else None,
            hrv_ms=int(hrv_ms) if hrv_ms is not None else None,
            readiness_or_body_battery_score=readiness,
            steps=steps,
            active_calories=active_calories,
            activity_score=act_score,
        )
        rows.append(unified.as_row())

    return rows
