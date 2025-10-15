import os
import argparse
import datetime as dt
from typing import List, Dict, Any, Optional
from garminconnect import Garmin
import gspread
from google.oauth2.service_account import Credentials

# === KONFIGURACIJA ===
DEFAULT_SHEET_KEY = "117d9d2SEvlDCX0zJsbeUg5Coueu13-KUnKcDua_9p7M"
DEFAULT_TAB = "Garmin"

HEADERS = [
    "date","source","bedtime","wake_time","sleep_duration","sleep_score",
    "rhr_bpm","hrv_ms","readiness_or_bb_score","steps","active_calories",
    "activity_score","workout_type","workout_duration","workout_active",
    "workout_avg_hr","workout_max_hr","distance_km","pace_min_per_km",
    "avg_speed_kmh","source_record_id"
]

TIME_SHIFT_HOURS = -2  # jer Garmin kasni +2h


# === POMOĆNE FUNKCIJE ===
def hhmm_from_epoch_ms(ms: Optional[int], shift_hours: int = 0) -> str:
    """Pretvara Garmin epoch ms u hh:mm, uz pomak vremena."""
    if not ms:
        return ""
    t = dt.datetime.fromtimestamp(ms / 1000.0) + dt.timedelta(hours=shift_hours)
    return t.strftime("%H:%M")

def hhmm_from_seconds(seconds: Optional[int]) -> str:
    """Pretvara trajanje (sekunde) u hh:mm bez računanja."""
    if not seconds:
        return ""
    m = int(seconds) // 60
    return f"{m // 60:02d}:{m % 60:02d}"

def open_ws_by_key(sheet_key: str, tab_name: str):
    sa_path = os.getenv("SHEETS_SA_JSON", "secrets/service_account.json")
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(sa_path, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(sheet_key)
    try:
        ws = sh.worksheet(tab_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=tab_name, rows=200, cols=len(HEADERS))
        ws.update(values=[HEADERS], range_name="A1")
        return ws

    existing = ws.get_values("A1:U1")
    if not existing or not existing[0] or existing[0][0] != "date":
        ws.update(values=[HEADERS], range_name="A1")
    return ws

def read_existing_dates(ws) -> Dict[str, int]:
    vals = ws.col_values(1)
    return {v: i for i, v in enumerate(vals, start=1) if i > 1 and v}


# === GLAVNA FUNKCIJA ZA PRETVARANJE ===
def to_row(day: dt.date, sleep: Dict[str, Any], hrv: Dict[str, Any], daily: Dict[str, Any]) -> List[Any]:
    s_dto = (sleep or {}).get("dailySleepDTO") or {}

    bedtime = hhmm_from_epoch_ms(s_dto.get("sleepStartTimestampLocal"), TIME_SHIFT_HOURS)
    waketime = hhmm_from_epoch_ms(s_dto.get("sleepEndTimestampLocal"), TIME_SHIFT_HOURS)
    sleep_duration = hhmm_from_seconds(s_dto.get("sleepTimeSeconds"))

    sleep_score = (
        s_dto.get("sleepScores", {}).get("overall", {}).get("value")
        if s_dto.get("sleepScores") else ""
    )

    hrv_last_night = (hrv.get("hrvSummary") or {}).get("lastNightAvg") if hrv else ""

    readiness_bb = daily.get("bodyBatteryHighestValue") if daily else ""

    return [
        day.isoformat(), "Garmin",
        bedtime, waketime, sleep_duration, sleep_score,
        daily.get("restingHeartRate") if daily else "",
        hrv_last_night, readiness_bb,
        daily.get("totalSteps") if daily else "",
        daily.get("activeKilocalories") if daily else "",
        "", "", "", "", "", "", "", "", "", ""
    ]


# === GLAVNI FETCH ===
def fetch_range(api: Garmin, since: dt.date, until: dt.date) -> Dict[str, List[Any]]:
    data = {}
    day = since
    while day <= until:
        s = day.isoformat()
        try:
            sleep = api.get_sleep_data(s)
        except Exception:
            sleep = {}
        try:
            hrv = api.get_hrv_data(s)
        except Exception:
            hrv = {}
        try:
            daily = api.get_stats(s)
        except Exception:
            daily = {}
        data[s] = to_row(day, sleep, hrv, daily)
        day += dt.timedelta(days=1)
    return data


# === CLI I GLAVNA LOGIKA ===
def parse_args():
    p = argparse.ArgumentParser()
    g = p.add_mutually_exclusive_group(required=False)
    g.add_argument("--days", type=int, help="koliko dana unazad (default 7)")
    p.add_argument("--since", help="YYYY-MM-DD")
    p.add_argument("--until", help="YYYY-MM-DD")
    p.add_argument("--sheet_key", default=DEFAULT_SHEET_KEY)
    p.add_argument("--tab", default=DEFAULT_TAB)
    return p.parse_args()

def main():
    args = parse_args()

    today = dt.date.today()
    if args.since and args.until:
        d0 = dt.date.fromisoformat(args.since)
        d1 = dt.date.fromisoformat(args.until)
    else:
        days = args.days or 7
        d1 = today - dt.timedelta(days=1)
        d0 = d1 - dt.timedelta(days=days - 1)

    email = os.getenv("GARMIN_EMAIL")
    password = os.getenv("GARMIN_PASSWORD")
    api = Garmin(email, password) if email and password else Garmin()
    api.login()

    data = fetch_range(api, d0, d1)

    ws = open_ws_by_key(args.sheet_key, args.tab)
    existing = read_existing_dates(ws)

    updates, appends = [], []
    for iso_date, row in data.items():
        if iso_date in existing:
            r = existing[iso_date]
            ws.update(values=[row], range_name=f"A{r}:U{r}", value_input_option="USER_ENTERED")
        else:
            appends.append(row)

    if appends:
        ws.append_rows(appends, value_input_option="USER_ENTERED")

    print(f"✅ done | updated: {len(data)} | tab '{ws.title}'")

    api.logout()

if __name__ == "__main__":
    main()
