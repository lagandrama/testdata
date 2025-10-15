# scripts/debug_garmin_endpoints.py
from __future__ import annotations

import sys
from pathlib import Path
import argparse
import json

# ensure project root on sys.path (…/testdata/testdata)
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from health_sync.sources.garmin import _fetch_json  # uses Playwright storage state

def peek(js: object, n: int = 200) -> str:
    try:
        s = json.dumps(js) if not isinstance(js, str) else js
        return (s[:n] + "…") if len(s) > n else s
    except Exception:
        return str(js)[:n]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = ap.parse_args()

    date = args.date
    tests = [
        ("/proxy/wellness-service/wellness/dailySummary", {"date": date}),
        (f"/proxy/wellness-service/wellness/dailySummary/{date}", None),
        ("/proxy/wellness-service/wellness/dailySleepData", {"date": date}),
        (f"/proxy/wellness-service/wellness/dailySleepData/{date}", None),
        ("/proxy/usersummary-service/usersummary/daily", {"calendarDate": date}),
        ("/proxy/wellness-service/wellness/dailyHrv", {"date": date}),
        ("/proxy/wellness-service/wellness/bodyBattery", {"date": date}),
    ]

    for path, params in tests:
        js = _fetch_json(path, params)
        print(f"{path}  has_data={bool(js)}  sample={peek(js)}")

if __name__ == "__main__":
    main()
