# testdata/scripts/push_rollaone_to_sheet.py
import os, sys
from datetime import date, timedelta, datetime, timezone
from dotenv import load_dotenv

# učitaj .env prije importa klijenta
load_dotenv(override=True)

# učini health_sync importabilnim
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from health_sync.sources.rollaone import RollaOneClient

import gspread
from google.oauth2.service_account import Credentials
from collections import defaultdict

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
    first = ws.row_values(1)
    if first != header:
        ws.resize(rows=1)
        ws.update("A1", [header])
    return ws

def read_existing_dates(ws):
    vals = ws.col_values(1)
    return set(v for v in vals[1:] if v)

# ---------- time helpers ----------
def _parse_iso(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def _fmt_hh_mm_from_iso(ts: str | None, shift_hours: int = 2) -> str | None:
    if not ts:
        return None
    try:
        dt = _parse_iso(ts) + timedelta(hours=shift_hours)
        return dt.strftime("%H:%M")
    except Exception:
        return None

def _fmt_minutes_hh_mm(total_minutes: float | int | None) -> str | None:
    if total_minutes is None:
        return None
    try:
        m = int(round(float(total_minutes)))
        h, mm = divmod(m, 60)
        return f"{h:01d}:{mm:02d}"
    except Exception:
        return None

def _date_key_from_any(d: dict) -> str | None:
    """
    Pokuša izvući YYYY-MM-DD iz tipičnih polja ili unix timestamp-a.
    """
    for k in ("period_start", "date", "day", "start_date"):
        if k in d and d[k]:
            v = d[k]
            if isinstance(v, str):
                try:
                    # ako je 'YYYY-MM-DD...' uzmi date dio
                    if len(v) >= 10 and v[4] == "-" and v[7] == "-":
                        return v[:10]
                    return _parse_iso(v).date().isoformat()
                except Exception:
                    pass
            if isinstance(v, (int, float)):
                try:
                    return datetime.fromtimestamp(v, tz=timezone.utc).date().isoformat()
                except Exception:
                    pass
    # ponekad se koristi 'timestamp' ili 'ts'
    for k in ("timestamp", "ts"):
        if k in d and isinstance(d[k], (int, float)):
            try:
                return datetime.fromtimestamp(d[k], tz=timezone.utc).date().isoformat()
            except Exception:
                pass
    return None

def _first_present(d: dict, *names):
    for n in names:
        if n in d and d[n] is not None:
            return d[n]
    return None

# ---------- Fetch & merge ----------
def main():
    FROM = (date.today() - timedelta(days=14)).strftime("%Y-%m-%d")
    TO   = date.today().strftime("%Y-%m-%d")

    TZ_SHIFT = int(os.getenv("ROLLAONE_TZ_SHIFT_HOURS", "2"))
    overwrite = os.getenv("ROLLAONE_OVERWRITE", "0") == "1"

    rc = RollaOneClient()

    # --- steps ---
    steps_map: dict[str, float] = {}
    for d in rc.steps(FROM, TO, "daily"):
        k = _date_key_from_any(d)
        if not k:
            continue
        val = _first_present(d, "steps", "count", "value")
        if val is not None:
            steps_map[k] = val

    # --- active calories ---
    kcals_map: dict[str, float] = {}
    for d in rc.calories(FROM, TO, "daily"):
        k = _date_key_from_any(d)
        if not k:
            continue
        val = _first_present(d, "active_calories", "calories", "kcal", "value")
        if val is not None:
            kcals_map[k] = val

    # --- HRV (daily avg/rmssd) ---
    hrv_map: dict[str, float] = {}
    for d in rc.hrv(FROM, TO, "daily"):
        k = _date_key_from_any(d)
        if not k:
            continue
        val = _first_present(d, "avg", "hrv", "mean", "rmssd_ms", "value")
        if val is not None:
            hrv_map[k] = val

    # --- Sleep (daily score + segmenti za vrijeme i trajanje) ---
    sleep_by_day: dict[str, dict] = {}

    # daily agregati (ako postoje; npr. score)
    for d in rc.sleep(FROM, TO, "daily"):
        k = _date_key_from_any(d)
        if not k:
            continue
        score = _first_present(d, "sleep_score", "score", "sleep_quality_score")
        if score is not None:
            sleep_by_day.setdefault(k, {})["sleep_score"] = score

    # segmenti (izvor istine za vrijeme i trajanje)
    SEG_GAP_HOURS = 3.0
    segs = rc.sleep(FROM, TO, "all") or []
    if segs:
        norm = []
        for s in segs:
            st = s.get("start_time") or s.get("start")
            et = s.get("end_time")   or s.get("end")
            stage = (s.get("stage") or s.get("phase") or "").lower()
            if not st or not et:
                continue
            try:
                st_dt = _parse_iso(st) + timedelta(hours=TZ_SHIFT)
                et_dt = _parse_iso(et) + timedelta(hours=TZ_SHIFT)
            except Exception:
                continue
            if et_dt <= st_dt:
                continue
            norm.append((st_dt, et_dt, stage, st, et))  # čuvamo original ISO za kasnije formatiranje

        norm.sort(key=lambda x: x[0])

        clusters = []
        cur = []
        for item in norm:
            if not cur:
                cur = [item]; continue
            prev_end = cur[-1][1]
            if (item[0] - prev_end).total_seconds() / 3600.0 > SEG_GAP_HOURS:
                clusters.append(cur); cur = [item]
            else:
                cur.append(item)
        if cur:
            clusters.append(cur)

        # po noći → mapiraj na dan buđenja (datum zadnjeg segmenta nakon shifta)
        for cl in clusters:
            wake_local = cl[-1][1]
            day_key = wake_local.date().isoformat()

            # total bez 'awake'
            total_min = 0.0
            for st_loc, et_loc, stage, _st_iso, _et_iso in cl:
                if stage not in ("awake", "wake", "awakenings"):
                    total_min += (et_loc - st_loc).total_seconds() / 60.0

            bed_iso = cl[0][3]
            wake_iso = cl[-1][4]

            sleep_by_day.setdefault(day_key, {})
            sleep_by_day[day_key]["bedtime"] = bed_iso
            sleep_by_day[day_key]["wake_time"] = wake_iso
            sleep_by_day[day_key]["sleep_duration_min"] = total_min

    # --- prep rows (merge) ---
    by_day = defaultdict(dict)

    for day, v in steps_map.items(): by_day[day]["steps"] = v
    for day, v in kcals_map.items(): by_day[day]["active_calories"] = v
    for day, v in hrv_map.items():   by_day[day]["hrv_ms"] = v

    for day, obj in sleep_by_day.items():
        by_day[day]["sleep_score"] = obj.get("sleep_score")
        by_day[day]["sleep_duration_min"] = obj.get("sleep_duration_min")
        if obj.get("bedtime"):   by_day[day]["bedtime"] = obj["bedtime"]
        if obj.get("wake_time"): by_day[day]["wake_time"] = obj["wake_time"]

    # (Opcionalno) daily score za korake i aktivne kalorije – poziv po danu
    # Ako želiš, otkomentiraj:
    # for day in list(by_day.keys()):
    #     try:
    #         s = rc.score_steps(day)
    #         if s is not None:
    #             by_day[day]["activity_score"] = s
    #     except Exception:
    #         pass
    #     try:
    #         s = rc.score_active_calories(day)
    #         if s is not None:
    #             # možeš prepisati/mergeati kako želiš; ovdje ostavljam u activity_score
    #             by_day[day]["activity_score"] = by_day[day].get("activity_score") or s
    #     except Exception:
    #         pass

    rows = []
    for day in sorted(by_day.keys()):
        d = by_day[day]
        bedtime_fmt   = _fmt_hh_mm_from_iso(d.get("bedtime"), shift_hours=TZ_SHIFT)
        waketime_fmt  = _fmt_hh_mm_from_iso(d.get("wake_time"), shift_hours=TZ_SHIFT)
        sleep_dur_fmt = _fmt_minutes_hh_mm(d.get("sleep_duration_min"))

        rows.append([
            day, "rollaone",
            bedtime_fmt,
            waketime_fmt,
            sleep_dur_fmt,
            d.get("sleep_score"),
            None,                        # rhr_bpm (nema dedicated endpointa u javnoj dokumentaciji)
            d.get("hrv_ms"),
            None,                        # readiness/body battery (nema endpointa)
            None,                        # health_score (nema generic /health/score)
            d.get("steps"),
            d.get("active_calories"),
            d.get("activity_score"),
            None, None, None, None, None,  # workout* polja (nije pokriveno ovdje)
            None, None, None, None
        ])

    # --- write to sheet ---
    sh = open_sheet()
    ws = get_or_create_ws(sh, "RollaOne", UNIFIED_HEADER)

    if overwrite:
        ws.resize(rows=1)
        if rows:
            ws.append_rows(rows, value_input_option="RAW")
        print(f"Prepisano {len(rows)} redova u 'RollaOne'.")
        return

    existing = read_existing_dates(ws)
    to_write = [r for r in rows if r[0] not in existing]
    if to_write:
        ws.append_rows(to_write, value_input_option="RAW")
        print(f"Upisano {len(to_write)} novih redova u 'RollaOne'.")
    else:
        print("Nema novih redova za upis u 'RollaOne'.")

if __name__ == "__main__":
    main()
