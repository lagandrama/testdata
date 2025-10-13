__all__ = ["UNIFIED_HEADER", "__version__"]

__version__ = "0.1.0"

# Unified header in exact order required for the Google Sheet
UNIFIED_HEADER = [
    "date",
    "source",
    "bedtime",
    "wake_time",
    "sleep_duration_min",
    "sleep_score",
    "rhr_bpm",
    "hrv_ms",
    "readiness_or_body_battery_score",
    "health_score",
    "steps",
    "active_calories",
    "activity_score",
    "workout_type",
    "workout_duration_min",
    "workout_active_calories",
    "workout_avg_hr_bpm",
    "workout_max_hr_bpm",
    "distance_km",
    "pace_min_per_km",
    "avg_speed_kmh",
    "workout_or_strain_score",
    "source_record_id",
]


