from __future__ import annotations

import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Any

import httpx
import structlog

from ..models import UnifiedRow
from ..utils import (
    iso_date,
    seconds_to_minutes,
    normalize_workout_type,
    meters_to_km,
    mps_to_speed_and_pace,
)
from ..config import get_settings

logger = structlog.get_logger()

BASE_URL = "https://api.ouraring.com/v2"

DEFAULT_TOKENS_PATH = Path(__file__).parent / "oura_tokens.json"
TOKENS_PATH = Path(os.getenv("OURA_TOKENS_PATH", str(DEFAULT_TOKENS_PATH)))
DEBUG = os.getenv("OURA_DEBUG") == "1"


# --------------------------- OAuth ---------------------------

def _load_tokens() -> dict:
    if not TOKENS_PATH.exists():
        return {}
    with TOKENS_PATH.open("r", encoding="utf-8") as f:
        tokens = json.load(f)
    now = int(time.time())
    tokens.setdefault("created_at", now)
    if "expires_at" not in tokens:
        exp_in = int(tokens.get("expires_in", 3600))
        tokens["expires_at"] = tokens["created_at"] + exp_in
    return tokens


def _save_tokens(tokens: dict) -> None:
    TOKENS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TOKENS_PATH.open("w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)


def _ensure_access_token() -> Tuple[str, dict]:
    env_token = get_settings().OURA_ACCESS_TOKEN
    if env_token:
        return env_token, {}
    tokens = _load_tokens()
    if not tokens:
        raise RuntimeError("Nema Oura tokena — pokreni OAuth flow.")
    access_token = tokens.get("access_token")
    now = int(time.time())
    if now >= int(tokens.get("expires_at", now - 1)) - 60:
        from ..sources.oura_oauth import refresh_tokens
        new_tokens = refresh_tokens(tokens["refresh_token"])
        new_tokens.setdefault("created_at", int(time.time()))
        new_tokens.setdefault(
            "expires_at", new_tokens["created_at"] + int(new_tokens.get("expires_in", 3600))
        )
        _save_tokens(new_tokens)
        access_token = new_tokens["access_token"]
    return access_token, tokens


def _auth_headers() -> dict[str, str]:
    token, _ = _ensure_access_token()
    return {"Authorization": f"Bearer {token}"}


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
    if t:
        return t.strftime("%H:%M:%S")
    return None


def _window_18(day: dt.date, tzinfo: Optional[dt.tzinfo]) -> tuple[dt.datetime, dt.datetime]:
    start = dt.datetime.combine(day, dt.time(18, 0)).replace(tzinfo=tzinfo)
    end = start + dt.timedelta(days=1)
    return start, end


def _overlap_seconds(a_start, a_end, b_start, b_end):
    start = max(a_start, b_start)
    end = min(a_end, b_end)
    return max(0.0, (end - start).total_seconds())


def _pick_sleep_ending_in_day(periods, day):
    """Period kojem bedtime_end pada unutar [18:00, +1d 18:00)."""
    if not periods:
        return {}
    ref = _parse_iso(periods[0].get("bedtime_start")) or dt.datetime.combine(day, dt.time.min)
    w_start, w_end = _window_18(day, ref.tzinfo)
    candidates = []
    for p in periods:
        e = _parse_iso(p.get("bedtime_end"))
        if not e:
            continue
        if w_start <= e < w_end:
            prefer = 1 if p.get("type") == "long_sleep" else 0
            dur = float(p.get("duration", 0) or 0)
            candidates.append((prefer, dur, e, p))
    if not candidates:
        return {}
    candidates.sort(key=lambda t: (t[0], t[1], t[2]))
    return candidates[-1][3]


def _pick_sleep_for_day(periods, day):
    """Fallback: najveći preklop s 18–18 prozorom."""
    if not periods:
        return {}
    ref = _parse_iso(periods[0].get("bedtime_start")) or dt.datetime.combine(day, dt.time.min)
    w_start, w_end = _window_18(day, ref.tzinfo)
    best = None
    best_key = (-1.0, 0)
    for p in periods:
        s = _parse_iso(p.get("bedtime_start"))
        e = _parse_iso(p.get("bedtime_end"))
        if not s or not e:
            continue
        overlap = _overlap_seconds(s, e, w_start, w_end)
        is_long = 1 if (p.get("type") == "long_sleep") else 0
        if (overlap, is_long) > best_key:
            best_key = (overlap, is_long)
            best = p
    return best or {}


# --------------------------- Glavna funkcija ---------------------------

def fetch_day(day: dt.date) -> list[list[Optional[str | float | int]]]:
    """
    “Dan sna” definiran 18:00 → 18:00.
    Ako sleep završava prije 18:00, vežemo ga uz prethodni kalendarski dan.
    """
    periods_start = (day - dt.timedelta(days=1)).isoformat()
    periods_end = (day + dt.timedelta(days=1)).isoformat()
    headers = _auth_headers()
    rows: list[list[Optional[str | float | int]]] = []

    with httpx.Client(timeout=30) as client:
        # Sleep
        js = client.get(f"{BASE_URL}/usercollection/sleep",
                        params={"start_date": periods_start, "end_date": periods_end},
                        headers=headers).json()
        periods = js.get("data", []) or []
        non_naps = [p for p in periods if p.get("type") != "nap"] or periods
        sleep_period = _pick_sleep_ending_in_day(non_naps, day) or _pick_sleep_for_day(non_naps, day)

        if DEBUG:
            logger.info("sleep_choice",
                        date=str(day),
                        bed_start=sleep_period.get("bedtime_start"),
                        bed_end=sleep_period.get("bedtime_end"),
                        d_type=sleep_period.get("type"),
                        duration=sleep_period.get("duration"),
                        total_periods=len(periods))

        # ostali endpointi
        def get_first(endpoint):
            js = client.get(f"{BASE_URL}/usercollection/{endpoint}",
                            params={"start_date": day.isoformat(), "end_date": (day + dt.timedelta(days=1)).isoformat()},
                            headers=headers).json()
            return js.get("data", [{}])[0] if js.get("data") else {}

        sleep_daily = get_first("daily_sleep")
        readiness = get_first("daily_readiness")
        activity = get_first("daily_activity")

        # ako wake_time < 18:00, vežemo ga na prethodni dan
        bed_end = _parse_iso(sleep_period.get("bedtime_end"))
        adjusted_date = day
        if bed_end and bed_end.hour < 18:
            adjusted_date = day - dt.timedelta(days=1)

        duration_sec = sleep_period.get("duration") or sleep_daily.get("total_sleep_duration")
        rhr = sleep_period.get("lowest_heart_rate") or sleep_daily.get("average_bpm")
        hrv = sleep_daily.get("average_hrv") or sleep_period.get("average_hrv")

        unified = UnifiedRow(
            date=iso_date(adjusted_date),
            source="oura",
            bedtime=_only_hms(sleep_period.get("bedtime_start")),
            wake_time=_only_hms(sleep_period.get("bedtime_end")),
            sleep_duration_min=seconds_to_minutes(duration_sec),
            sleep_score=sleep_daily.get("score"),
            rhr_bpm=int(rhr) if rhr else None,
            hrv_ms=int(hrv) if hrv else None,
            readiness_or_body_battery_score=readiness.get("score"),
            steps=activity.get("steps"),
            active_calories=activity.get("active_calories"),
            activity_score=activity.get("score"),
        )
        rows.append(unified.as_row())

    return rows
