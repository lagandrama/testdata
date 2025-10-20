#!/usr/bin/env python3
# scripts/push_garmin_to_sheet.py

from __future__ import annotations

import argparse
import datetime as dt
import os
from dotenv import load_dotenv
load_dotenv()
from typing import List, Any
import time, random

# --- logger ---
try:
    import structlog  # type: ignore
    logger = structlog.get_logger()
except Exception:
    import logging
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    logger = logging.getLogger("garmin_push")

# --- Google Sheets SDK ---
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# --- sys.path ensure project root ---
import sys, pathlib
CURR = pathlib.Path(__file__).resolve()
ROOT = None
for p in [CURR.parent, *CURR.parents]:
    if (p / "sources").exists():
        ROOT = p
        break
if ROOT and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# --- Garmin fetch ---
from health_sync.sources import garmin  # -> garmin.fetch_day(day)

# ---------------------- Sheet helpers ----------------------

from health_sync.models import UnifiedRow

HEADERS = UnifiedRow.headers()


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


def _col_letter(index_1_based: int) -> str:
    n = int(index_1_based)
    letters = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters))


def _get_sheet_id(svc, sheet_id: str, tab: str) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=sheet_id).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == tab:
            return int(props["sheetId"])  # type: ignore[arg-type]
    raise RuntimeError(f"Tab '{tab}' not found")


def _ensure_header(svc, sheet_id: str, tab: str):
    last_col = _col_letter(len(HEADERS))
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"{tab}!A1:{last_col}1"
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


def _read_existing_map(svc, sheet_id: str, tab: str):
    last_col = _col_letter(len(HEADERS))
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"{tab}!A2:{last_col}10000"
    ).execute()
    rows = resp.get("values", []) or []
    mapping = {}
    src_idx = len(HEADERS) - 1
    for i, r in enumerate(rows, start=2):
        if not r:
            continue
        date = r[0] if len(r) > 0 else ""
        source = r[1] if len(r) > 1 else ""
        src_id = r[src_idx] if len(r) > src_idx else "daily"
        if date and source:
            mapping[f"{date}|{source}|{src_id or 'daily'}"] = i
    return mapping


def _pad_to_headers(row: List[Any]) -> List[Any]:
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
            last_col = _col_letter(len(HEADERS))
            rng = f"{tab}!A{row_idx}:{last_col}{row_idx}"
            to_update.append({"range": rng, "values": [row]})
        else:
            to_append.append(row)

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

    # sort by date
    sheet_tab_id = _get_sheet_id(svc, sheet_id, tab)
    svc.spreadsheets().batchUpdate(
        spreadsheetId=sheet_id,
        body={
            "requests": [
                {
                    "sortRange": {
                        "range": {"sheetId": sheet_tab_id, "startRowIndex": 1},
                        "sortSpecs": [{"dimensionIndex": 0, "sortOrder": "ASCENDING"}],
                    }
                }
            ]
        },
    ).execute()


def daterange(end_date: dt.date, days: int):
    start = end_date - dt.timedelta(days=days - 1)
    cur = start
    out = []
    while cur <= end_date:
        out.append(cur)
        cur += dt.timedelta(days=1)
    return out


def main():
    parser = argparse.ArgumentParser(description="Push Garmin daily to Google Sheet")
    parser.add_argument("--sheet-id", default=os.getenv("GSHEET_ID"), required=False)
    parser.add_argument("--tab", default=(os.getenv("GSHEET_TAB_GARMIN") or "Garmin"))
    parser.add_argument("--date", help="YYYY-MM-DD (default today)")
    parser.add_argument("--days", type=int, default=1, help="How many days back (default 1)")
    args = parser.parse_args()

    sheet_id = args.sheet_id
    if not sheet_id:
        raise SystemExit("Set GSHEET_ID env var or pass --sheet-id")

    if args.date:
        end_date = dt.date.fromisoformat(args.date)
    else:
        end_date = dt.date.today()

    days = max(1, int(args.days))
    dates = daterange(end_date, days)

    svc = _gsvc()

    all_rows: List[List[Any]] = []
    long_pause_done = False
    for d in dates:
        try:
            rows = garmin.fetch_day(d)
            logger.info("fetched", extra={"date": str(d), "rows": len(rows)})
            all_rows.extend(rows)
        except Exception as e:
            msg = str(e).lower()
            rl = any(tok in msg for tok in ["429", "rate limit", "1015"]) or getattr(e, "status_code", None) in (429, 1015)
            logger.error("fetch_error", extra={"date": str(d), "err": str(e), "rate_limited": rl})
            if rl and not long_pause_done:
                wait_min = random.randint(30, 60)
                logger.warning("rate_limit_pause", extra={"minutes": wait_min})
                time.sleep(wait_min * 60)
                long_pause_done = True
                # retry once after long pause
                try:
                    rows = garmin.fetch_day(d)
                    logger.info("fetched_after_pause", extra={"date": str(d), "rows": len(rows)})
                    all_rows.extend(rows)
                except Exception as e2:
                    logger.error("fetch_error_after_pause", extra={"date": str(d), "err": str(e2)})
            # continue to next date

        # gentle pacing between days to avoid bursts
        time.sleep(random.uniform(5.0, 12.0))

    if not all_rows:
        try:
            logger.warning("no_rows_to_push")
        except Exception:
            logger.warn("no_rows_to_push")  # type: ignore[attr-defined]
        return

    upsert_rows(svc, sheet_id, args.tab, all_rows)
    logger.info("done", extra={"pushed": len(all_rows)})


if __name__ == "__main__":
    main()


