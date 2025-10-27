#!/usr/bin/env python3
# scripts/push_polar_to_sheet.py

from __future__ import annotations

import argparse
import datetime as dt
import os
from dotenv import load_dotenv
load_dotenv()
from typing import List, Dict, Any

# --- osiguraj da je project root na sys.path PRIJE bilo kakvih import-a paketa ---
import sys, pathlib
CURR = pathlib.Path(__file__).resolve()

ROOT = None
for p in [CURR.parent, *CURR.parents]:
    # preferirani slučaj: .../health_sync/sources/
    if (p / "health_sync" / "sources").exists():
        ROOT = p
        break
    # fallback: projekt bez top-level paketa, samo sources/
    if (p / "sources").exists():
        ROOT = p
        break

if ROOT and str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# dodatno: ako postoji health_sync/ dodaj i njega
if ROOT and (pathlib.Path(ROOT) / "health_sync").exists():
    pkg_root = str(pathlib.Path(ROOT) / "health_sync")
    if pkg_root not in sys.path:
        sys.path.insert(0, pkg_root)

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

# --- Polar fetch (pokušaj kao health_sync.sources, pa kao standalone sources) ---
try:
    from health_sync.sources import polar  # noqa: F401
except ModuleNotFoundError:
    from sources import polar  # type: ignore

# ---------------------- Sheet helpers ----------------------

if 'health_sync' in sys.modules:
    from health_sync.models import UnifiedRow  # type: ignore
else:
    from models import UnifiedRow  # fallback kada nema top-level paketa

# Finalni redoslijed kolona usklađen s UnifiedRow (A-U = 21 kolona)
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


def _read_existing_map(svc, sheet_id: str, tab: str) -> Dict[str, int]:
    last_col = _col_letter(len(HEADERS))
    resp = svc.spreadsheets().values().get(
        spreadsheetId=sheet_id, range=f"{tab}!A2:{last_col}10000"
    ).execute()
    rows = resp.get("values", []) or []
    mapping: Dict[str, int] = {}
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

    to_update, to_append = [], []

    for row in rows:
        row = _pad_to_headers(row)
        key = f"{row[0]}|{row[1]}"
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


def daterange(end_date: dt.date, days: int) -> List[dt.date]:
    start = end_date - dt.timedelta(days=days - 1)
    out: List[dt.date] = []
    cur = start
    while cur <= end_date:
        out.append(cur)
        cur += dt.timedelta(days=1)
    return out


def main():
    parser = argparse.ArgumentParser(description="Push Polar sleep u Google Sheet")
    parser.add_argument("--sheet-id", default=os.getenv("GSHEET_ID"), required=False)
    parser.add_argument("--tab", default=(os.getenv("GSHEET_TAB_POLAR") or "Polar"))
    parser.add_argument("--date", help="YYYY-MM-DD (ako se ne zada, koristi danas)")
    parser.add_argument("--days", type=int, default=1, help="Koliko dana unatrag (default 1)")
    args = parser.parse_args()

    sheet_id = args.sheet_id
    if not sheet_id:
        raise SystemExit("Set GSHEET_ID env var ili proslijedi --sheet-id")

    end_date = dt.date.fromisoformat(args.date) if args.date else dt.date.today()
    dates = daterange(end_date, max(1, int(args.days)))

    svc = _gsvc()

    all_rows: List[List[Any]] = []
    for d in dates:
        try:
            rows = polar.fetch_day(d)
            logger.info("fetched", extra={"date": str(d), "rows": len(rows)})
            all_rows.extend(rows)
        except Exception as e:
            logger.error("fetch_error", extra={"date": str(d), "err": str(e)})

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
