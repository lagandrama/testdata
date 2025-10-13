# scripts/push_oura_to_sheet.py
from __future__ import annotations
import os
import datetime as dt
from typing import List
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=True)

from health_sync.sources.oura import fetch_day
from health_sync.models import UnifiedRow  # headers + redoslijed

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
SERVICE_ACCOUNT_FILE = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "secrets/service_account.json")
SHEET_NAME = os.getenv("SHEET_NAME", "Oura")
SINCE_DAYS = int(os.getenv("OURA_SINCE_DAYS", "7"))
REWRITE = os.getenv("OURA_REWRITE", "0") in ("1", "true", "True", "yes")

HEADER: List[str] = UnifiedRow.headers()
IDX_DATE = HEADER.index("date")
IDX_SOURCE = HEADER.index("source")
IDX_SRC_ID = HEADER.index("source_record_id")

def _auth_sheet():
    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SPREADSHEET_ID)
    return sh

def _reset_worksheet(sh, worksheet_name: str):
    # briše postojeći tab (ako postoji) i kreira novi s headerom
    try:
        ws_old = sh.worksheet(worksheet_name)
        sh.del_worksheet(ws_old)
    except Exception:
        pass
    ws = sh.add_worksheet(title=worksheet_name, rows="2000", cols=str(len(HEADER)))
    ws.update(range_name="A1", values=[HEADER], value_input_option="RAW")
    return ws

def _ensure_header_and_get_existing_keys(sh, worksheet_name: str):
    # kreira tab ako ne postoji i sigurno postavlja header; vraća existing keys za dedupe
    try:
        ws = sh.worksheet(worksheet_name)
    except Exception:
        ws = sh.add_worksheet(title=worksheet_name, rows="2000", cols=str(len(HEADER)))

    ws.resize(rows=max(ws.row_count, 1), cols=max(ws.col_count, len(HEADER)))
    ws.update(range_name="A1", values=[HEADER], value_input_option="RAW")

    # učitaj postojeće vrijednosti u tri kolone koje trebaju za dedupe
    def _col_vals(idx_one_based: int) -> List[str]:
        try:
            vals = ws.col_values(idx_one_based)[1:]  # preskoči header
        except Exception:
            vals = []
        return vals

    dates = _col_vals(IDX_DATE + 1)
    sources = _col_vals(IDX_SOURCE + 1)
    src_ids = _col_vals(IDX_SRC_ID + 1)

    n = max(len(dates), len(sources), len(src_ids))
    dates += [""] * (n - len(dates))
    sources += [""] * (n - len(sources))
    src_ids += [""] * (n - len(src_ids))

    existing = {f"{d}|{s}|{sid or 'daily'}" for d, s, sid in zip(dates, sources, src_ids)}
    return ws, existing

def _normalize_rows(rows: List[List]) -> List[List]:
    fixed = []
    for r in rows:
        r = [("" if v is None else v) for v in r]
        if len(r) < len(HEADER):
            r += [""] * (len(HEADER) - len(r))
        else:
            r = r[:len(HEADER)]
        fixed.append(r)
    return fixed

def main():
    if not SPREADSHEET_ID:
        raise SystemExit("SPREADSHEET_ID nije postavljen u .env")

    today = dt.date.today()
    dates = [today - dt.timedelta(days=i) for i in range(1, SINCE_DAYS + 1)]
    dates.sort()

    all_rows: List[List] = []
    for d in dates:
        all_rows.extend(fetch_day(d))
    if not all_rows:
        print("Nema redaka za upis.")
        return

    sh = _auth_sheet()
    cleaned = _normalize_rows(all_rows)

    if REWRITE:
        # potpuno poravnaj tab: izbriši, postavi header, upiši sve redove iz perioda
        ws = _reset_worksheet(sh, SHEET_NAME)
        ws.append_rows(cleaned, value_input_option="RAW")
        print(f"Rewrite završio. Upisano {len(cleaned)} redaka u '{SHEET_NAME}'.")
        return

    # standardni mod: osiguraj header i dedupe
    ws, existing_keys = _ensure_header_and_get_existing_keys(sh, SHEET_NAME)
    new_rows = []
    for r in cleaned:
        key = f"{r[IDX_DATE]}|{r[IDX_SOURCE]}|{r[IDX_SRC_ID] or 'daily'}"
        if key not in existing_keys:
            new_rows.append(r)

    if not new_rows:
        print("Nema novih redaka za upis (sve postoji).")
        return

    ws.append_rows(new_rows, value_input_option="RAW")
    print(f"Upisano {len(new_rows)} redaka u '{SHEET_NAME}'.")
    
if __name__ == "__main__":
    main()
