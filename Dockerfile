# syntax=docker/dockerfile:1

FROM mcr.microsoft.com/playwright/python:latest AS base

ENV POETRY_HOME=/opt/poetry \
    POETRY_VERSION=1.8.3 \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir poetry==${POETRY_VERSION}
WORKDIR /app

COPY pyproject.toml README.md ./
RUN poetry config virtualenvs.create false
RUN poetry install --no-interaction --no-ansi --only main

COPY health_sync ./health_sync

RUN playwright install --with-deps chromium

CMD ["health-sync", "fetch", "--sources", "oura,polar,garmin", "--since", "2d"]
