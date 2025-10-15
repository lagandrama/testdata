from __future__ import annotations
import datetime as dt
from pathlib import Path

from health_sync.sources.garmin import _client, BASE  # adjust import if you run from testdata/testdata

def main():
    day = dt.date.today().isoformat()
    paths = [
        "/proxy/wellness-service/wellness/dailySummary",
        "/proxy/wellness-service/wellness/dailySleepData",
        "/proxy/wellness-service/wellness/dailyHrv",
        "/proxy/wellness-service/wellness/bodyBattery",
    ]
    with _client() as c:
        for p in paths:
            r = c.get(f"{BASE}{p}", params={"date": day})
            print(p, r.status_code, (r.text[:200] if r.text else r.content[:200]))

if __name__ == "__main__":
    main()
