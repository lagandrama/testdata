from __future__ import annotations

import datetime as dt
from typing import Optional


# Strategy A: XML importer of export.xml from /data/apple_health/
# Strategy B: FastAPI webhook endpoint (implemented elsewhere)

def fetch_day(day: dt.date) -> list[list[Optional[str | float | int]]]:
    # TODO: Implement Apple Health XML parsing and FastAPI webhook ingest
    return []
