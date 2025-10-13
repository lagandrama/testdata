from __future__ import annotations

from typing import Optional, Literal

from pydantic import BaseModel, Field


UnifiedWorkoutType = Literal[
    "run",
    "ride",
    "swim",
    "strength",
    "walk",
    "hike",
    "yoga",
    "other",
]


class UnifiedRow(BaseModel):
    date: str
    source: str
    bedtime: Optional[str] = None
    wake_time: Optional[str] = None
    sleep_duration_min: Optional[int] = None
    sleep_score: Optional[int] = None
    rhr_bpm: Optional[int] = None
    hrv_ms: Optional[int] = None
    readiness_or_body_battery_score: Optional[int] = None
    health_score: Optional[int] = None
    steps: Optional[int] = None
    active_calories: Optional[int] = None
    activity_score: Optional[int] = None
    workout_type: Optional[UnifiedWorkoutType] = None
    workout_duration_min: Optional[int] = None
    workout_active_calories: Optional[int] = None
    workout_avg_hr_bpm: Optional[int] = None
    workout_max_hr_bpm: Optional[int] = None
    distance_km: Optional[float] = None
    pace_min_per_km: Optional[float] = None
    avg_speed_kmh: Optional[float] = None
    workout_or_strain_score: Optional[int] = None
    source_record_id: Optional[str] = None

    def as_row(self) -> list[Optional[str | float | int]]:
        return [
            self.date,
            self.source,
            self.bedtime,
            self.wake_time,
            self.sleep_duration_min,
            self.sleep_score,
            self.rhr_bpm,
            self.hrv_ms,
            self.readiness_or_body_battery_score,
            self.health_score,
            self.steps,
            self.active_calories,
            self.activity_score,
            self.workout_type,
            self.workout_duration_min,
            self.workout_active_calories,
            self.workout_avg_hr_bpm,
            self.workout_max_hr_bpm,
            self.distance_km,
            self.pace_min_per_km,
            self.avg_speed_kmh,
            self.workout_or_strain_score,
            self.source_record_id,
        ]


# Source payload placeholders (expand as needed)
class OuraDailySleep(BaseModel):
    bedtime_start: Optional[str] = None
    bedtime_end: Optional[str] = None
    duration: Optional[int] = None
    score: Optional[int] = None
    average_bpm: Optional[float] = None
    average_hrv: Optional[float] = None


class OuraDailyReadiness(BaseModel):
    score: Optional[int] = None


class OuraDailyActivity(BaseModel):
    steps: Optional[int] = None
    active_calories: Optional[int] = None
    score: Optional[int] = None


class OuraWorkout(BaseModel):
    id: str
    type: Optional[str] = None
    start_datetime: Optional[str] = None
    duration: Optional[int] = None
    average_heart_rate: Optional[int] = None
    max_heart_rate: Optional[int] = None
    calories: Optional[int] = None
    distance: Optional[float] = None
    average_speed: Optional[float] = None
    average_pace: Optional[float] = None
