# --- PATCH: health_sync/models.py --------------------------------------------
from dataclasses import dataclass
from typing import Optional, List, Any

OURA_HEADERS: List[str] = [
    "date", "source",
    "bedtime", "wake_time", "sleep_duration_min", "sleep_score",
    "rhr_bpm", "hrv_ms", "readiness_or_body_battery_score",
    "health_score",
    "steps", "active_calories", "activity_score",
    "workout_type", "workout_duration_min", "workout_active_calories",
    "workout_avg_hr_bpm", "workout_max_hr_bpm",
    "distance_km", "pace_min_per_km", "avg_speed_kmh",
    "workout_or_strain_score",
    "source_record_id",
]

@dataclass
class UnifiedRow:
    date: str
    source: str
    bedtime: Optional[str] = None
    wake_time: Optional[str] = None
    sleep_duration_min: Optional[float] = None
    sleep_score: Optional[int] = None
    rhr_bpm: Optional[int] = None
    hrv_ms: Optional[int] = None
    readiness_or_body_battery_score: Optional[int] = None
    health_score: Optional[int] = None
    steps: Optional[int] = None
    active_calories: Optional[int] = None
    activity_score: Optional[int] = None
    workout_type: Optional[str] = None
    workout_duration_min: Optional[float] = None
    workout_active_calories: Optional[int] = None
    workout_avg_hr_bpm: Optional[int] = None
    workout_max_hr_bpm: Optional[int] = None
    distance_km: Optional[float] = None
    pace_min_per_km: Optional[float] = None
    avg_speed_kmh: Optional[float] = None
    workout_or_strain_score: Optional[int] = None
    source_record_id: Optional[str] = None

    @staticmethod
    def headers() -> List[str]:
        return OURA_HEADERS

    def as_row(self) -> List[Any]:
        # REDOSLIJED MORA ODGOVARATI headers()
        return [
            self.date, (self.source.lower() if isinstance(self.source, str) else self.source),
            self.bedtime, self.wake_time, self.sleep_duration_min, self.sleep_score,
            self.rhr_bpm, self.hrv_ms, self.readiness_or_body_battery_score,
            self.health_score,
            self.steps, self.active_calories, self.activity_score,
            self.workout_type, self.workout_duration_min, self.workout_active_calories,
            self.workout_avg_hr_bpm, self.workout_max_hr_bpm,
            self.distance_km, self.pace_min_per_km, self.avg_speed_kmh,
            self.workout_or_strain_score,
            self.source_record_id,
        ]
# --- END PATCH ----------------------------------------------------------------
