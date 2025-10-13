#!/usr/bin/env python3
# scripts/push_polar_to_sheet.py

from __future__ import annotations

import argparse
import datetime as dt
import os
from dotenv import load_dotenv
load_dotenv()
from typing import List, Dict, Any

from health_sync.sources import polar

# --- logger (structlog ako postoji, inače standardni logging) ---
try:
    import structlog  # type: ignore
    logger = structlog.get_logger()
except Exception:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger("polar_push")

# --- Google Sheets SDK ---
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --- osiguraj da je project root na sys.path (da 'sources' bude importabilan) ---
import sys, pathlib
CURR = pathlib.Path(__file__).resolve()
ROOT = None
for p in [CURR.parent, *CURR.parents]:
    if (p / "sources").exists():
        ROOT = p
        break
if ROOT and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# --- Polar fetch ---
from health_sync.sources import polar  # -> polar.fetch_day(day) vraća List[List[Any]]

# ---------------------- Sheet helpers ----------------------

# Finalni redoslijed kolona kakav vidiš u sheetu (A-U = 21 kolona)
HEADERS = [
    "date",
    "source",
    "bedtime",
    "wake_time",
    "sleep_duration",
    "sleep_score",
    "rhr_bpm",
    "hrv_ms",
    "readiness_or_body_battery_score",
    "steps",
    "active_calories",
    "activity_score",
    "workout_type",
    "workout_duration",
    "workout_active",
    "workout_avg_hr",
    "workout_max_hr",
    "distance_km",
    "pace_min_per_km",
    "avg_speed_kmh",
    "source_record_id",
]


def _gsvc():
    creds_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not creds_path:
        raise SystemExit("GOOGLE_APPLICATION_CREDENTIALS nije postavljen.")
    if not os.path.exists(creds_path):
        raise SystemExit(f"Ne postoji datoteka s credsom: {creds_path}")
    creds = Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds)


def _ensure_header(svc, sheet_id: str, tab: str):
    # pročitaj prvih 1-2 reda; ako nema headera ili nije isti, upiši ga
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"{tab}!A1:U1"
    ).execute()
    values = resp.get("values", [])
    if not values or values[0] != HEADERS:
        logger.info("writing_header", extra={"tab": tab})
        svc.spreadsheets().values().update(
            spreadsheetId=sheet_id,
            range=f"{tab}!A1",
            valueInputOption="RAW",
            body={"values": [HEADERS]},
        ).execute()


def _read_existing_map(svc, sheet_id: str, tab: str) -> Dict[str, int]:
    """
    Vrati mapu ključeva -> row_index (1-based):
      key = f"{date}|{source}"
    """
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"{tab}!A2:B10000"
    ).execute()
    rows = resp.get("values", []) or []
    mapping: Dict[str, int] = {}
    for i, r in enumerate(rows, start=2):  # data krece od reda 2
        if not r:
            continue
        date = r[0] if len(r) > 0 else ""
        source = r[1] if len(r) > 1 else ""
        if date and source:
            mapping[f"{date}|{source}"] = i
    return mapping


def _pad_to_headers(row: List[Any]) -> List[Any]:
    # `polar.fetch_day` vraća listu vrijednosti u istom rasporedu kao HEADERS;
    # ako je kraća, dopuni prazninama.
    padded = list(row)
    if len(padded) < len(HEADERS):
        padded.extend([""] * (len(HEADERS) - len(padded)))
    return padded[: len(HEADERS)]


def upsert_rows(svc, sheet_id: str, tab: str, rows: List[List[Any]]):
    _ensure_header(svc, sheet_id, tab)
    existing = _read_existing_map(svc, sheet_id, tab)

    to_update = []
    to_append = []

    for row in rows:
        row = _pad_to_headers(row)
        key = f"{row[0]}|{row[1]}"  # date|source
        if key in existing:
            row_idx = existing[key]
            rng = f"{tab}!A{row_idx}:U{row_idx}"
            to_update.append({"range": rng, "values": [row]})
        else:
            to_append.append(row)

    # batch update
    if to_update:
        svc.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id,
            body={"valueInputOption": "RAW", "data": to_update},
        ).execute()

    if to_append:
        svc.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=f"{tab}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": to_append},
        ).execute()


# ---------------------- Date helpers ----------------------

def daterange(end_date: dt.date, days: int) -> List[dt.date]:
    # uključivo 'end_date', unatrag 'days' dana
    start = end_date - dt.timedelta(days=days - 1)
    out: List[dt.date] = []
    cur = start
    while cur <= end_date:
        out.append(cur)
        cur += dt.timedelta(days=1)
    return out


# ---------------------- Main ----------------------

def main():
    parser = argparse.ArgumentParser(description="Push Polar sleep u Google Sheet")
    parser.add_argument("--sheet-id", default=os.getenv("GSHEET_ID"), required=False)
    parser.add_argument("--tab", default=os.getenv("GSHEET_TAB", "Hardware test"))
    parser.add_argument("--date", help="YYYY-MM-DD (ako se ne zada, koristi danas)")
    parser.add_argument("--days", type=int, default=1, help="Koliko dana unatrag (default 1)")
    args = parser.parse_args()

    sheet_id = args.sheet_id
    if not sheet_id:
        raise SystemExit("Set GSHEET_ID env var ili proslijedi --sheet-id")

    if args.date:
        end_date = dt.date.fromisoformat(args.date)
    else:
        end_date = dt.date.today()

    days = max(1, int(args.days))
    dates = daterange(end_date, days)

    svc = _gsvc()

    all_rows: List[List[Any]] = []
    for d in dates:
        try:
            rows = polar.fetch_day(d)  # očekujemo 1 red
            logger.info("fetched", extra={"date": str(d), "rows": len(rows)})
            all_rows.extend(rows)
        except Exception as e:
            logger.error("fetch_error", extra={"date": str(d), "err": str(e)})

    if not all_rows:
        # structlog: warn je deprecated; standard logging: warning
        try:
            logger.warning("no_rows_to_push")
        except Exception:
            logger.warn("no_rows_to_push")  # type: ignore[attr-defined]
        return

    upsert_rows(svc, sheet_id, args.tab, all_rows)
    logger.info("done", extra={"pushed": len(all_rows)})


if __name__ == "__main__":
    main()
