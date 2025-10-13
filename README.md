# health-sync

Automates collecting daily health metrics from multiple sources and appends normalized rows into a single Google Sheet.

## Quick start

1. Install Poetry and dependencies:
   - Windows PowerShell:
     ```powershell
     (Invoke-WebRequest -Uri https://install.python-poetry.org -UseBasicParsing).Content | py -
     poetry install
     ```
2. Create `.env` from `.env.example` and fill credentials.
3. Ensure your Google service account has Editor access to the spreadsheet and share the Sheet with the service account email.
4. Verify Sheets auth:
   ```powershell
   poetry run health-sync test-row
   ```

## CLI
- `health-sync fetch --sources oura,polar,garmin,apple,ross,rollaone --since 7d`
- `health-sync backfill --start 2025-01-01 --end 2025-10-07 --sources oura,polar`
- `health-sync playwright-login --target garmin|ross|rollaone`
- `health-sync test-row`

## Unified schema columns
In order: date, source, bedtime, wake_time, sleep_duration_min, sleep_score, rhr_bpm, hrv_ms, readiness_or_body_battery_score, health_score, steps, active_calories, activity_score, workout_type, workout_duration_min, workout_active_calories, workout_avg_hr_bpm, workout_max_hr_bpm, distance_km, pace_min_per_km, avg_speed_kmh, workout_or_strain_score, source_record_id

## Sources
- Oura v2: PAT supported, OAuth2 planned. Sleep/Readiness/Activity implemented, workouts basic.
- Polar: skeleton.
- Garmin: skeleton with Playwright fallback planned.
- Apple Health: XML importer + FastAPI webhook planned.
- ROSS / Rolla One: Playwright scrapers planned.

## Docker
- Build and run:
  ```bash
  docker build -t health-sync .
  docker run --env-file .env health-sync
  ```
- Compose: see `docker-compose.yml`.

## Dev
- Pre-commit: `pre-commit install`
- Test: `pytest`
