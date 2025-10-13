# health_sync/sheets.py
from __future__ import annotations

import os, json
from typing import Optional, List
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "Unified"
HEADER = [
    "date","source","bedtime","wake_time","sleep_duration_min","sleep_score","rhr_bpm","hrv_ms",
    "readiness_or_body_battery_score","health_score","steps","active_calories","activity_score",
    "workout_type","workout_duration_min","workout_active_calories","workout_avg_hr_bpm",
    "workout_max_hr_bpm","distance_km","pace_min_per_km","avg_speed_kmh","workout_or_strain_score",
    "source_record_id",
]

def _creds():
    """
    Podrži oba načina:
    - GOOGLE_SERVICE_ACCOUNT_FILE = putanja do JSON ključa
    - GOOGLE_SERVICE_ACCOUNT_JSON = inline JSON string **ili** putanja (fallback)
    """
    file_path = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    raw = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")

    if file_path and os.path.exists(file_path):
        return Credentials.from_service_account_file(file_path, scopes=SCOPES)

    if raw:
        # ako je raw putanja do fajla
        if os.path.exists(raw):
            return Credentials.from_service_account_file(raw, scopes=SCOPES)
        # inače pretpostavi da je inline JSON string
        info = json.loads(raw)
        return Credentials.from_service_account_info(info, scopes=SCOPES)

    raise RuntimeError(
        "Service account not provided. Set GOOGLE_SERVICE_ACCOUNT_FILE (path) "
        "or GOOGLE_SERVICE_ACCOUNT_JSON (inline JSON or path)."
    )

def _svc():
    svc = build("sheets", "v4", credentials=_creds())
    return svc.spreadsheets(), svc.spreadsheets().values()

def _ensure_tab(spreadsheets, values, spreadsheet_id: str):
    meta = spreadsheets.get(spreadsheetId=spreadsheet_id).execute()
    titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
    if SHEET_NAME not in titles:
        spreadsheets.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests":[{"addSheet":{"properties":{"title":SHEET_NAME}}}]}
        ).execute()
        values.update(
            spreadsheetId=spreadsheet_id,
            range=f"{SHEET_NAME}!A1",
            valueInputOption="RAW",
            body={"values":[HEADER]},
        ).execute()

def append_rows(rows: list[list[Optional[str | float | int]]]) -> None:
    if not rows:
        return
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    spreadsheets, values = _svc()
    _ensure_tab(spreadsheets, values, spreadsheet_id)
    values.append(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_NAME}!A:Z",
        valueInputOption="USER_ENTERED",
        insertDataOption="INSERT_ROWS",
        body={"values":[ [("" if v is None else v) for v in r] for r in rows ]},
    ).execute()
