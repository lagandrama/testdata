from __future__ import annotations
from dotenv import load_dotenv; load_dotenv()

from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # General
    TZ: str = Field(default="Europe/Sarajevo")
    SPREADSHEET_ID: str = Field(
        default="1U6ozBmOkN_jRU44AHFlwISull1fYPLu0DCitNguvrIo",
        description="Google Spreadsheet ID",
    )
    GOOGLE_SERVICE_ACCOUNT_JSON: Optional[str] = Field(
        default=None,
        description="Either JSON string of service account or path to JSON file",
    )

    # Oura
    OURA_CLIENT_ID: Optional[str] = None
    OURA_CLIENT_SECRET: Optional[str] = None
    OURA_REDIRECT_URI: Optional[str] = None
    OURA_ACCESS_TOKEN: Optional[str] = None  # PAT fallback

    # Polar
    POLAR_CLIENT_ID: Optional[str] = None
    POLAR_CLIENT_SECRET: Optional[str] = None
    POLAR_REDIRECT_URI: Optional[str] = None

    # Garmin (playwright)
    GARMIN_USERNAME: Optional[str] = None
    GARMIN_PASSWORD: Optional[str] = None
    GARMIN_STORAGE_STATE: str = Field(default="./state/garmin.json")

    # Rolla ROSS
    ROLLA_ROSS_URL: Optional[str] = None
    ROLLA_ROSS_USER: Optional[str] = None
    ROLLA_ROSS_PASS: Optional[str] = None
    ROLLA_ROSS_STATE: str = Field(default="./state/ross.json")

    # Rolla One
    ROLLA_ONE_URL: Optional[str] = None
    ROLLA_ONE_USER: Optional[str] = None
    ROLLA_ONE_PASS: Optional[str] = None
    ROLLA_ONE_STATE: str = Field(default="./state/rollaone.json")


def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]


