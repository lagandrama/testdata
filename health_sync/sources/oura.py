from __future__ import annotations

import datetime as dt
import json
import os
import time
from pathlib import Path
from typing import Optional, Tuple

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

# Gdje spremamo OAuth tokene dobivene iz oura_oauth.py
# možeš promijeniti putem env var: OURA_TOKENS_PATH
DEFAULT_TOKENS_PATH = Path(__file__).parent / "oura_tokens.json"
TOKENS_PATH = Path(os.getenv("OURA_TOKENS_PATH", str(DEFAULT_TOKENS_PATH)))


def _load_tokens() -> dict:
    if not TOKENS_PATH.exists():
        return {}
    with TOKENS_PATH.open("r", encoding="utf-8") as f:
        tokens = json.load(f)

    # ako nemamo created_at/expires_at, izračunaj ih grubo
    now = int(time.time())
    if "created_at" not in tokens:
        tokens["created_at"] = now
    if "expires_at" not in tokens:
        # expires_in je u sekundama; ako ni njega nema, pretpostavi 1h
        exp_in = int(tokens.get("expires_in", 3600))
        tokens["expires_at"] = tokens["created_at"] + exp_in

    return tokens


def _save_tokens(tokens: dict) -> None:
    TOKENS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TOKENS_PATH.open("w", encoding="utf-8") as f:
        json.dump(tokens, f, ensure_ascii=False, indent=2)


def _ensure_access_token() -> Tuple[str, dict]:
    """
    Vraća (access_token, tokens_dict).
    Preferira OAuth (oura_tokens.json); fallback je env OURA_ACCESS_TOKEN.
    Ako je OAuth access_token istekao, automatski radi refresh preko oura_oauth.refresh_tokens.
    """
    # 1) Fallback: env var
    env_token = get_settings().OURA_ACCESS_TOKEN
    if env_token:
        return env_token, {}

    # 2) OAuth: tokens file
    tokens = _load_tokens()
    if not tokens:
        raise RuntimeError(
            "Nije pronađen Oura token. Pokreni OAuth flow (python -m health_sync.sources.oura_oauth) "
            "ILI postavi OURA_ACCESS_TOKEN u .env"
        )

    access_token = tokens.get("access_token")
    if not access_token:
        raise RuntimeError("oura_tokens.json ne sadrži access_token")

    # Je li token pred istekom?
    now = int(time.time())
    expires_at = int(tokens.get("expires_at", now - 1))
    if now >= (expires_at - 60):
        logger.info("Oura access token expired or near expiry — refreshing...")
        try:
            # lazy import da izbjegnemo kružnu ovisnost
            from ..sources.oura_oauth import refresh_tokens  # type: ignore

            new_tokens = refresh_tokens(tokens["refresh_token"])
            # očekujemo barem access_token, expires_in; dodaj created_at/expires_at ako nedostaju
            new_tokens.setdefault("created_at", int(time.time()))
            new_tokens.setdefault(
                "expires_at", new_tokens["created_at"] + int(new_tokens.get("expires_in", 3600))
            )
            _save_tokens(new_tokens)
            access_token = new_tokens["access_token"]
            tokens = new_tokens
            logger.info("Oura access token refreshed successfully.")
        except Exception as e:
            logger.error("Failed to refresh Oura token", error=str(e))
            raise

    return access_token, tokens


def _auth_headers() -> dict[str, str]:
    token, _ = _ensure_access_token()
    return {"Authorization": f"Bearer {token}"}


async def _async_get(client: httpx.AsyncClient, url: str, params: dict[str, str]) -> dict:
    resp = await client.get(url, params=params, headers=_auth_headers(), timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_day(day: dt.date) -> list[list[Optional[str | float | int]]]:
    """
    Dohvati Oura daily sleep / readiness / activity + workouts za konkretan dan.
    Vraća listu redaka formata UnifiedRow.as_row().
    """
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
            # Oura v2 obično vraća {"data": [ ... ]}; ako nema podataka, data je [].
            sleep = js.get("data", [{}])[0] if js.get("data") else {}
        except Exception as e:
            logger.warning("Failed to fetch daily_sleep", error=str(e))
            sleep = {}

        # Readiness
        try:
            js = client.get(
                f"{BASE_URL}/usercollection/daily_readiness",
                params={"start_date": start, "end_date": end},
                headers=headers,
            ).json()
            readiness = js.get("data", [{}])[0] if js.get("data") else {}
        except Exception as e:
            logger.warning("Failed to fetch daily_readiness", error=str(e))
            readiness = {}

        # Activity
        try:
            js = client.get(
                f"{BASE_URL}/usercollection/daily_activity",
                params={"start_date": start, "end_date": end},
                headers=headers,
            ).json()
            activity = js.get("data", [{}])[0] if js.get("data") else {}
        except Exception as e:
            logger.warning("Failed to fetch daily_activity", error=str(e))
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
        except Exception as e:
            logger.warning("Failed to fetch workout", error=str(e))
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
