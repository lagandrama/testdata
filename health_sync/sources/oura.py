from __future__ import annotations

import datetime as dt
from typing import Optional

import httpx
import structlog

from ..models import UnifiedRow
from ..utils import iso_date, seconds_to_minutes, normalize_workout_type, meters_to_km, mps_to_speed_and_pace, round_2dp
from ..config import get_settings

logger = structlog.get_logger()

BASE_URL = "https://api.ouraring.com/v2"


def _auth_headers() -> dict[str, str]:
    token = get_settings().OURA_ACCESS_TOKEN
    if not token:
        # OAuth2 flow not implemented yet; placeholder
        raise RuntimeError("OURA_ACCESS_TOKEN or OAuth2 not configured")
    return {"Authorization": f"Bearer {token}"}


async def _async_get(client: httpx.AsyncClient, url: str, params: dict[str, str]) -> dict:
    resp = await client.get(url, params=params, headers=_auth_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_day(day: dt.date) -> list[list[Optional[str | float | int]]]:
    # synchronous wrapper for simplicity
    start = day.isoformat()
    end = (day + dt.timedelta(days=1)).isoformat()

    headers = _auth_headers()
    rows: list[list[Optional[str | float | int]]] = []

    with httpx.Client(timeout=30) as client:
        # Daily sleep
        try:
            js = client.get(
                f"{BASE_URL}/usercollection/daily_sleep",
                params={"start_date": start, "end_date": end},
                headers=headers,
            ).json()
            sleep = js.get("data", [{}])[0] if js.get("data") else {}
        except Exception:
            sleep = {}

        # Readiness
        try:
            js = client.get(
                f"{BASE_URL}/usercollection/daily_readiness",
                params={"start_date": start, "end_date": end},
                headers=headers,
            ).json()
            readiness = js.get("data", [{}])[0] if js.get("data") else {}
        except Exception:
            readiness = {}

        # Activity
        try:
            js = client.get(
                f"{BASE_URL}/usercollection/daily_activity",
                params={"start_date": start, "end_date": end},
                headers=headers,
            ).json()
            activity = js.get("data", [{}])[0] if js.get("data") else {}
        except Exception:
            activity = {}

        unified = UnifiedRow(
            date=iso_date(day),
            source="oura",
            bedtime=sleep.get("bedtime_start"),
            wake_time=sleep.get("bedtime_end"),
            sleep_duration_min=seconds_to_minutes(sleep.get("duration")),
            sleep_score=sleep.get("score"),
            rhr_bpm=int(sleep.get("average_bpm")) if sleep.get("average_bpm") else None,
            hrv_ms=int(sleep.get("average_hrv")) if sleep.get("average_hrv") else None,
            readiness_or_body_battery_score=readiness.get("score"),
            steps=activity.get("steps"),
            active_calories=activity.get("active_calories"),
            activity_score=activity.get("score"),
        )
        rows.append(unified.as_row())

        # Workouts
        try:
            js = client.get(
                f"{BASE_URL}/usercollection/workout",
                params={"start_date": start, "end_date": end},
                headers=headers,
            ).json()
            workouts = js.get("data", [])
        except Exception:
            workouts = []

        for w in workouts:
            w_type = normalize_workout_type(w.get("sport"))
            duration_min = seconds_to_minutes(w.get("duration"))
            avg_hr = w.get("average_heart_rate")
            max_hr = w.get("max_heart_rate")
            distance_km = meters_to_km(w.get("distance"))
            avg_speed_kmh, pace_min_per_km = mps_to_speed_and_pace(w.get("average_speed"))
            calories = w.get("calories")
            row = UnifiedRow(
                date=iso_date(day),
                source="oura",
                workout_type=w_type,
                workout_duration_min=duration_min,
                workout_active_calories=calories,
                workout_avg_hr_bpm=avg_hr,
                workout_max_hr_bpm=max_hr,
                distance_km=distance_km,
                pace_min_per_km=pace_min_per_km,
                avg_speed_kmh=avg_speed_kmh,
                source_record_id=str(w.get("id")) if w.get("id") else None,
            ).as_row()
            rows.append(row)

    return rows
