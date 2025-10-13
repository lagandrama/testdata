from dotenv import load_dotenv; load_dotenv()
import os
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

SID = os.environ["SPREADSHEET_ID"]
KEY = os.environ["GOOGLE_SERVICE_ACCOUNT_FILE"]

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
creds = Credentials.from_service_account_file(KEY, scopes=SCOPES)
svc = build("sheets", "v4", credentials=creds)
spreadsheets = svc.spreadsheets()
values = svc.spreadsheets().values()

HEADER = [
    "date","source","bedtime","wake_time","sleep_duration_min","sleep_score","rhr_bpm","hrv_ms",
    "readiness_or_body_battery_score","health_score","steps","active_calories","activity_score",
    "workout_type","workout_duration_min","workout_active_calories","workout_avg_hr_bpm",
    "workout_max_hr_bpm","distance_km","pace_min_per_km","avg_speed_kmh","workout_or_strain_score",
    "source_record_id",
]

# ensure tab
meta = spreadsheets.get(spreadsheetId=SID).execute()
titles = {s["properties"]["title"] for s in meta.get("sheets", [])}
if "Unified" not in titles:
    spreadsheets.batchUpdate(spreadsheetId=SID, body={"requests":[{"addSheet":{"properties":{"title":"Unified"}}}]}).execute()
    values.update(spreadsheetId=SID, range="Unified!A1", valueInputOption="RAW", body={"values":[HEADER]}).execute()

# append one row
values.append(
    spreadsheetId=SID,
    range="Unified!A:Z",
    valueInputOption="USER_ENTERED",
    insertDataOption="INSERT_ROWS",
    body={"values":[["2025-10-07","manual-test"] + [""]*21]},
).execute()
print("DONE")
