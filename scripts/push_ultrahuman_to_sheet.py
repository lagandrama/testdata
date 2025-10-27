# scripts/push_ultrahuman_to_sheet.py
import os, sys
from datetime import date, timedelta, datetime
from dotenv import load_dotenv

# .env prije importa klijenta
load_dotenv(override=True)

# učini health_sync importabilnim
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from health_sync.sources.ultrahuman import UltrahumanClient  # noqa: E402

import gspread  # noqa: E402
from google.oauth2.service_account import Credentials  # noqa: E402
from typing import Any, Dict, Iterable, Optional, Union  # noqa: E402

# ---------- Google Sheets helpers ----------
def open_sheet():
    creds_file = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    spreadsheet_id = os.getenv("SPREADSHEET_ID")
    if not creds_file or not spreadsheet_id:
        raise RuntimeError("Set GOOGLE_APPLICATION_CREDENTIALS i SPREADSHEET_ID u .env")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(creds_file, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(spreadsheet_id)

UNIFIED_HEADER = [
    "date","source","bedtime","wake_time","sleep_duration_min","sleep_score",
    "rhr_bpm","hrv_ms","readiness_or_body_battery_score","health_score","steps",
    "active_calories","activity_score","workout_type","workout_duration_min",
    "workout_active_calories","workout_avg_hr_bpm","workout_max_hr_bpm",
    "distance_km","pace_min_per_km","avg_speed_kmh","workout_or_strain_score"
]

def get_or_create_ws(sh, title, header):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=2000, cols=len(header))
        ws.append_row(header, value_input_option="RAW")
        return ws
    # osiguraj header
    first = ws.row_values(1)
    if first != header:
        ws.resize(rows=1)  # očisti tab (zadrži samo header)
        ws.update("A1", [header])
    return ws

def read_existing_dates(ws):
    vals = ws.col_values(1)  # kolona A
    return set(v for v in vals[1:] if v)

# ---------- helpers ----------
Number = Union[int, float]

def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def _fmt_hh_mm_from_iso(ts: Optional[str], shift_hours: int) -> Optional[str]:
    if not ts:
        return None
    try:
        dt = _parse_iso(ts) + timedelta(hours=shift_hours)
        return dt.strftime("%H:%M")
    except Exception:
        return None

def _fmt_minutes_hh_mm(total_minutes: Optional[Union[int, float]]) -> Optional[str]:
    if total_minutes is None:
        return None
    try:
        m = int(round(float(total_minutes)))
        h, mm = divmod(m, 60)
        return f"{h:01d}:{mm:02d}"
    except Exception:
        return None

def _deep_find_first(obj: Any, keys: Iterable[str]) -> Optional[Any]:
    """
    Robustno nađi prvo pojavljivanje bilo kojeg ključa (case‑insensitive) bilo gdje u structuri.
    Vraća vrijednost ako je scalar (int/float/str/bool), inače nastavlja pretragu u djeci.
    """
    from collections import deque
    keyset = {k.lower() for k in keys}
    q = deque([obj])
    while q:
        cur = q.popleft()
        if isinstance(cur, dict):
            # prvo direktni ključevi
            for k, v in cur.items():
                if k.lower() in keyset and isinstance(v, (int, float, str)) and v is not None:
                    return v
            # pa potom rekurzija
            for v in cur.values():
                if isinstance(v, (dict, list)):
                    q.append(v)
        elif isinstance(cur, list):
            for v in cur:
                if isinstance(v, (dict, list)):
                    q.append(v)
    return None

def _deep_find_time(obj: Any, keys: Iterable[str]) -> Optional[str]:
    val = _deep_find_first(obj, keys)
    if isinstance(val, str):
        return val
    return None

def _deep_find_number(obj: Any, keys: Iterable[str]) -> Optional[Number]:
    val = _deep_find_first(obj, keys)
    if isinstance(val, (int, float)):
        return float(val)
    # ponekad je string numerički
    if isinstance(val, str):
        try:
            return float(val)
        except Exception:
            return None
    return None

# ---------- Fetch & map ----------
def main():
    FROM = (date.today() - timedelta(days=14))
    TO = date.today()

    tz_shift = int(os.getenv("TZ_SHIFT_HOURS", "2"))
    overwrite = os.getenv("ULTRAHUMAN_OVERWRITE", "0") == "1"

    uc = UltrahumanClient()

    rows = []
    d = FROM
    while d <= TO:
        day = d.isoformat()
        try:
            payload = uc.get_metrics(day)
        except Exception as e:
            print(f"[warn] metrics {day} failed: {e}")
            d += timedelta(days=1)
            continue

        # --- mapiranja (robustna, više aliasa) ---
        # Sleep
        bedtime_iso = _deep_find_time(payload, ["bedtime", "bed_time", "sleep_start", "start_time", "start"])
        waketime_iso = _deep_find_time(payload, ["wake_time", "waketime", "sleep_end", "end_time", "end"])
        sleep_minutes = _deep_find_number(payload, [
            "sleep_duration_min","total_sleep_minutes","sleep_minutes","total_sleep","duration_min"
        ])
        # ako je total_sleep u sekundama (često ~ 25-35k), pretvori u minute
        if sleep_minutes and sleep_minutes > 2000:
            sleep_minutes = sleep_minutes / 60.0

        sleep_score = _deep_find_number(payload, ["sleep_score", "sleep_quality_score"])

        # RHR / HRV
        rhr = _deep_find_number(payload, ["rhr_bpm","resting_heart_rate","resting_hr","avg_rhr","lowest_rhr","rhr"])
        hrv = _deep_find_number(payload, ["hrv_ms","avg_hrv","rmssd_ms","rmssd","hrv"])

        # “Readiness” – Ultrahuman koristi Recovery Index / Recovery Score
        readiness = _deep_find_number(payload, ["recovery_index","recovery_score","readiness","readiness_score"])

        # Health/metabolic
        health_score = _deep_find_number(payload, ["health_score","metabolic_score"])

        # Koraci / kalorije / activity
        steps = _deep_find_number(payload, ["steps","total_steps","step_count"])
        active_kcals = _deep_find_number(payload, ["active_calories","total_calories","calories_active","calories"])
        activity_score = _deep_find_number(payload, ["activity_score","movement_index","movement_score"])

        rows.append([
            day,
            "ultrahuman",
            _fmt_hh_mm_from_iso(bedtime_iso, tz_shift),
            _fmt_hh_mm_from_iso(waketime_iso, tz_shift),
            _fmt_minutes_hh_mm(sleep_minutes),
            sleep_score,
            rhr,
            hrv,
            readiness,
            health_score,
            steps,
            active_kcals,
            activity_score,
            None,  # workout_type
            None,  # workout_duration_min
            None,  # workout_active_calories
            None,  # workout_avg_hr_bpm
            None,  # workout_max_hr_bpm
            None,  # distance_km
            None,  # pace_min_per_km
            None,  # avg_speed_kmh
            None,  # workout_or_strain_score
        ])

        d += timedelta(days=1)

    # upis u "Ultrahuman" tab
    sh = open_sheet()
    ws = get_or_create_ws(sh, "Ultrahuman", UNIFIED_HEADER)

    if overwrite:
        ws.resize(rows=1)
        if rows:
            ws.append_rows(rows, value_input_option="RAW")
        print(f"Prepisano {len(rows)} redova u 'Ultrahuman'.")
        return

    existing = read_existing_dates(ws)
    to_write = [r for r in rows if r[0] not in existing]
    if to_write:
        ws.append_rows(to_write, value_input_option="RAW")
        print(f"Upisano {len(to_write)} novih redova u 'Ultrahuman'.")
    else:
        print("Nema novih redova za upis u 'Ultrahuman'.")


if __name__ == "__main__":
    main()
