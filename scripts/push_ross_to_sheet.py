# scripts/push_ross_to_sheet.py
import os, sys
from datetime import date, timedelta, datetime
from dotenv import load_dotenv

# .env prije importa klijenta
load_dotenv(override=True)

# učini health_sync importabilnim
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from health_sync.sources.ross import RossClient

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
def _key_date(d: dict) -> str | None:
    for k in ("period_start", "date", "day", "period", "start_date"):
        if k in d and d[k]:
            return d[k]
    return None

def _first_present(d: dict, *names):
    for n in names:
        if n in d and d[n] is not None:
            return d[n]
    return None

def _parse_iso(ts: str) -> datetime:
    # prihvati ISO i sa 'Z'
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))

def _fmt_hh_mm_from_iso(ts: str | None, shift_hours: int = 2) -> str | None:
    """Pretvori ISO string u 'hh:mm' i pomakni za +shift_hours (default +2)."""
    if not ts:
        return None
    try:
        dt = _parse_iso(ts) + timedelta(hours=shift_hours)
        return dt.strftime("%H:%M")
    except Exception:
        return None

def _fmt_minutes_hh_mm(total_minutes: float | int | None) -> str | None:
    """Pretvori minute u 'hh:mm'. Ako nema vrijednosti, vrati None (prazna ćelija)."""
    if total_minutes is None:
        return None
    try:
        m = int(round(float(total_minutes)))
        h, mm = divmod(m, 60)
        return f"{h:01d}:{mm:02d}"
    except Exception:
        return None

def _safe_score(rc: RossClient, score_name: str, from_date: str, to_date: str, gran="daily") -> dict[str, float]:
    """Sigurno dohvaća score i vraća mapu {date: score}. Ako endpoint ne postoji, vrati {}."""
    try:
        return { _key_date(d): d.get("score") for d in rc.score(score_name, from_date, to_date, gran) if _key_date(d) }
    except Exception:
        return {}

# ---------- Fetch & merge ----------
def main():
    FROM = (date.today() - timedelta(days=14)).strftime("%Y-%m-%d")
    TO   = date.today().strftime("%Y-%m-%d")

    rc = RossClient()

    # stabilni endpointi: period_start + vrijednost
    steps = {d["period_start"]: d.get("steps") for d in rc.steps(FROM, TO, "daily")}
    kcals = {d["period_start"]: d.get("calories") for d in rc.calories(FROM, TO, "daily")}
    rhr   = {d["period_start"]: d.get("rhr") for d in rc.rhr(FROM, TO, "daily")}

    # HRV: (avg|hrv|mean|rmssd_ms|value)
    hrv = {}
    for d in rc.hrv(FROM, TO, "daily"):
        k = _key_date(d)
        if not k:
            continue
        hrv[k] = _first_present(d, "avg", "hrv", "mean", "rmssd_ms", "value")

    # Sleep (daily): koristimo score ako postoji; vrijeme/duration će doći iz segmenata (segmenti = izvor istine)
    sleep: dict[str, dict] = {}
    for d in rc.sleep_daily(FROM, TO):
        k = _key_date(d)
        if not k:
            continue
        score = _first_present(d, "sleep_score", "score", "sleep_quality_score")
        sleep[k] = {"sleep_score": score}  # NE uzimamo vrijeme/dur odavde

    # --- SEGMENTI: prioritet za bedtime/waketime/Total (bez 'awake'), tz shift +2h, grupiranje u noći ---
    SEG_GAP_HOURS = 3.0       # >3h pauze = nova noć
    TZ_SHIFT_HOURS = 2        # traženo: +2h

    segs = rc.sleep(FROM, TO, "all") or []
    if segs:
        # normaliziraj i primijeni +2h u datumskoj logici (za određivanje dana buđenja),
        # ali u sleep dict spremamo original ISO – formatiranje (s +2h) radimo pri upisu.
        norm = []
        for s in segs:
            st = s.get("start_time") or s.get("start")
            et = s.get("end_time")   or s.get("end")
            stage = (s.get("stage") or s.get("phase") or "").lower()
            if not st or not et:
                continue
            st_dt = _parse_iso(st)
            et_dt = _parse_iso(et)
            if et_dt <= st_dt:
                continue
            # za grupiranje koristimo datume pomaknute +2h (lokalni prikaz)
            norm.append((st_dt + timedelta(hours=TZ_SHIFT_HOURS),
                         et_dt + timedelta(hours=TZ_SHIFT_HOURS),
                         stage,
                         st, et))  # original ISO za format kasnije

        norm.sort(key=lambda x: x[0])

        # klasterizacija u noći
        clusters = []
        cur = []
        for item in norm:
            if not cur:
                cur = [item]; continue
            prev_end_local = cur[-1][1]
            if (item[0] - prev_end_local).total_seconds() / 3600.0 > SEG_GAP_HOURS:
                clusters.append(cur); cur = [item]
            else:
                cur.append(item)
        if cur:
            clusters.append(cur)

        # izračun po noći → mapiraj na dan buđenja (datum zadnjeg segmenta nakon shifta)
        for cl in clusters:
            bed_local = cl[0][0]
            wake_local = cl[-1][1]
            # total sleep (bez 'awake')
            total_min = 0.0
            for st_loc, et_loc, stage, _st_iso, _et_iso in cl:
                if stage not in ("awake", "wake", "awakenings"):
                    total_min += (et_loc - st_loc).total_seconds() / 60.0

            day_key = wake_local.date().isoformat()  # dan buđenja

            # original ISO (bez shifta); formatirat ćemo kasnije s +2h
            bed_iso = cl[0][3]
            wake_iso = cl[-1][4]

            if day_key not in sleep:
                sleep[day_key] = {}
            # segmenti imaju prioritet
            sleep[day_key]["bedtime"] = bed_iso
            sleep[day_key]["wake_time"] = wake_iso
            sleep[day_key]["sleep_duration_min"] = total_min

    # score/get: dopuni sleep_score + readiness/body_battery + health + activity
    sleep_score_map   = _safe_score(rc, "sleep",     FROM, TO, "daily")
    readiness_score   = _safe_score(rc, "readiness", FROM, TO, "daily")
    health_score      = _safe_score(rc, "health",    FROM, TO, "daily")
    activity_score    = _safe_score(rc, "activity",  FROM, TO, "daily")

    for day, sc in sleep_score_map.items():
        sleep.setdefault(day, {})
        if sleep[day].get("sleep_score") is None:
            sleep[day]["sleep_score"] = sc

    # merge po datumu
    by_day = defaultdict(dict)
    for day, v in steps.items(): by_day[day]["steps"] = v
    for day, v in kcals.items(): by_day[day]["active_calories"] = v
    for day, v in rhr.items():   by_day[day]["rhr_bpm"] = v
    for day, v in hrv.items():   by_day[day]["hrv_ms"] = v

    for day, obj in sleep.items():
        by_day[day]["sleep_score"] = obj.get("sleep_score")
        by_day[day]["sleep_duration_min"] = obj.get("sleep_duration_min")
        if obj.get("bedtime"):   by_day[day]["bedtime"] = obj["bedtime"]       # ISO
        if obj.get("wake_time"): by_day[day]["wake_time"] = obj["wake_time"]   # ISO

    for day, v in readiness_score.items(): by_day[day]["readiness_or_body_battery_score"] = v
    for day, v in health_score.items():    by_day[day]["health_score"] = v
    for day, v in activity_score.items():  by_day[day]["activity_score"] = v

    # pripremi redove (formatiraj hh:mm i shift +2h za bed/wake; trajanje u hh:mm)
    rows = []
    for day in sorted(by_day.keys()):
        d = by_day[day]
        bedtime_fmt   = _fmt_hh_mm_from_iso(d.get("bedtime"), shift_hours=2)
        waketime_fmt  = _fmt_hh_mm_from_iso(d.get("wake_time"), shift_hours=2)
        sleep_dur_fmt = _fmt_minutes_hh_mm(d.get("sleep_duration_min"))

        rows.append([
            day, "ross",
            bedtime_fmt,                    # bedtime "hh:mm" (+2h)
            waketime_fmt,                   # wake_time "hh:mm" (+2h)
            sleep_dur_fmt,                  # sleep_duration_min "hh:mm"
            d.get("sleep_score"),
            d.get("rhr_bpm"),
            d.get("hrv_ms"),
            d.get("readiness_or_body_battery_score"),
            d.get("health_score"),
            d.get("steps"),
            d.get("active_calories"),
            d.get("activity_score"),
            None,               # workout_type
            None,               # workout_duration_min
            None,               # workout_active_calories
            None,               # workout_avg_hr_bpm
            None,               # workout_max_hr_bpm
            None,               # distance_km
            None,               # pace_min_per_km
            None,               # avg_speed_kmh
            None,               # workout_or_strain_score
        ])

    # upis u "Ross" tab (upsert po datumu) + opcioni overwrite
    sh = open_sheet()
    ws = get_or_create_ws(sh, "Ross", UNIFIED_HEADER)

    overwrite = os.getenv("ROSS_OVERWRITE", "0") == "1"
    if overwrite:
        ws.resize(rows=1)  # ostavi header
        ws.append_rows(rows, value_input_option="RAW")
        print(f"Prepisano {len(rows)} redova u 'Ross'.")
        return

    existing = read_existing_dates(ws)
    to_write = [r for r in rows if r[0] not in existing]
    if to_write:
        ws.append_rows(to_write, value_input_option="RAW")
        print(f"Upisano {len(to_write)} novih redova u 'Ross'.")
    else:
        print("Nema novih redova za upis u 'Ross'.")

if __name__ == "__main__":
    main()
