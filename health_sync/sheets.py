# health_sync/sheets.py
from __future__ import annotations

import os, json
from typing import Optional, List
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from .models import UnifiedRow

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_NAME = "Unified"
# Keep headers in sync with UnifiedRow to avoid column shifts
HEADER = UnifiedRow.headers()

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

def _get_sheet_id(spreadsheets, spreadsheet_id: str) -> int:
    meta = spreadsheets.get(spreadsheetId=spreadsheet_id).execute()
    for s in meta.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == SHEET_NAME:
            return int(props["sheetId"])  # type: ignore[arg-type]
    raise RuntimeError(f"Sheet '{SHEET_NAME}' not found")

def _col_letter(index_1_based: int) -> str:
    n = int(index_1_based)
    letters: list[str] = []
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(65 + rem))
    return "".join(reversed(letters))

def _pad_row(row: list[object]) -> list[object]:
    padded = list(row)
    missing = len(HEADER) - len(padded)
    if missing > 0:
        padded.extend([""] * missing)
    return padded[: len(HEADER)]

def _read_existing_map(values_svc, spreadsheet_id: str) -> dict[str, int]:
    last_col = _col_letter(len(HEADER))
    resp = values_svc.get(
        spreadsheetId=spreadsheet_id,
        range=f"{SHEET_NAME}!A2:{last_col}100000",
    ).execute()
    rows = resp.get("values", []) or []
    mapping: dict[str, int] = {}
    for idx, r in enumerate(rows, start=2):
        if not r:
            continue
        date = r[0] if len(r) > 0 else ""
        source = r[1] if len(r) > 1 else ""
        # source_record_id je zadnja kolona
        src_id = r[len(HEADER) - 1] if len(r) >= len(HEADER) else "daily"
        if date and source:
            mapping[f"{date}|{source}|{src_id or 'daily'}"] = idx
    return mapping

def append_rows(rows: list[list[Optional[str | float | int]]]) -> None:
    if not rows:
        return
    spreadsheet_id = os.environ["SPREADSHEET_ID"]
    spreadsheets, values = _svc()
    _ensure_tab(spreadsheets, values, spreadsheet_id)

    existing = _read_existing_map(values, spreadsheet_id)
    to_update: list[dict] = []
    to_append: list[list[object]] = []

    for r in rows:
        row = _pad_row([("" if v is None else v) for v in r])
        key = f"{row[0]}|{row[1]}|{row[-1] or 'daily'}"
        if key in existing:
            row_idx = existing[key]
            last_col = _col_letter(len(HEADER))
            rng = f"{SHEET_NAME}!A{row_idx}:{last_col}{row_idx}"
            to_update.append({"range": rng, "values": [row]})
        else:
            to_append.append(row)

    if to_update:
        values.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"valueInputOption": "RAW", "data": to_update},
        ).execute()

    if to_append:
        values.append(
            spreadsheetId=spreadsheet_id,
            range=f"{SHEET_NAME}!A:A",
            valueInputOption="RAW",
            insertDataOption="INSERT_ROWS",
            body={"values": to_append},
        ).execute()

    # Sort whole sheet by date column (A) ascending, skip header
    sheet_id = _get_sheet_id(spreadsheets, spreadsheet_id)
    spreadsheets.batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [
                {
                    "sortRange": {
                        "range": {"sheetId": sheet_id, "startRowIndex": 1},
                        "sortSpecs": [{"dimensionIndex": 0, "sortOrder": "ASCENDING"}],
                    }
                }
            ]
        },
    ).execute()
