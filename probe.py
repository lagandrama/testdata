from health_sync.sources.garmin import _client, BASE 
import datetime as dt
day = dt.date.today().isoformat()
with _client() as c:
    for p in [
        "/proxy/wellness-service/wellness/dailySummary",
        "/proxy/wellness-service/wellness/dailySleepData",
        "/proxy/wellness-service/wellness/dailyHrv",
        "/proxy/wellness-service/wellness/bodyBattery",
    ]:
        r = c.get(f"{BASE}{p}", params={"date": day})
        print(p, r.status_code, r.text[:200])