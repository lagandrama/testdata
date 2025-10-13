# health_sync/cli.py
from __future__ import annotations

import os
import datetime as dt
from typing import Optional

import typer
import structlog
from dotenv import load_dotenv

from .sheets import append_rows
from .models import UnifiedRow
from .utils import iso_date

load_dotenv()  # učitaj .env automatski

app = typer.Typer(no_args_is_help=True, help="health-sync CLI")
log = structlog.get_logger()


# ---------- helperi ----------

def _parse_since(since: str) -> dt.date:
    s = since.strip().lower()
    if s.endswith("d") and s[:-1].isdigit():
        days = int(s[:-1])
        return dt.date.today() - dt.timedelta(days=days)
    return dt.date.fromisoformat(s)


def _iter_days(start_date: dt.date, end_date: dt.date):
    delta = (end_date - start_date).days
    for n in range(delta + 1):
        yield start_date + dt.timedelta(days=n)


def _load_selected_sources(names: set[str]):
    """Dinamički učitaj samo tražene izvore; preskoči one koji još nisu implementirani."""
    loaded = {}
    # (ime, modul path)
    candidates = {
        "oura": ".sources.oura",
        "polar": ".sources.polar",
        "garmin": ".sources.garmin",
        "apple": ".sources.apple_health",
        "ross": ".sources.rolla_ross",
        "rollaone": ".sources.rolla_one",
    }
    for short, mod_path in candidates.items():
        if short in names:
            try:
                loaded[short] = __import__(mod_path, fromlist=["dummy"])
            except Exception as e:
                log.warning("source_import_failed", source=short, error=str(e))
                typer.echo(f"[WARN] Izvor '{short}' još nije spreman ili se nije mogao učitati: {e}")
    return loaded


# ---------- komande ----------

@app.command("diag")
def diag() -> None:
    """Brza dijagnostika (.env, ključ, Spreadsheet ID)."""
    sid = os.getenv("SPREADSHEET_ID")
    key_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE")
    has_json = bool(os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON"))

    typer.echo(f"SPREADSHEET_ID: {sid}")
    typer.echo(f"GOOGLE_SERVICE_ACCOUNT_FILE: {key_file}  (exists={os.path.exists(key_file or '')})")
    typer.echo(f"GOOGLE_SERVICE_ACCOUNT_JSON set: {has_json}")


@app.command("test-row")
def test_row() -> None:
    """Upiši jedan probni red u 'Unified' tab da provjeriš Google Sheets auth."""
    sid = os.getenv("SPREADSHEET_ID")
    if not sid:
        raise typer.BadParameter("SPREADSHEET_ID nije postavljen u .env")

    today = iso_date(dt.date.today())
    row = UnifiedRow(date=today, source="cli-test").as_row()

    append_rows([row])
    typer.echo(f"OK — upisan 1 red u Spreadsheet {sid}, tab 'Unified'.")


@app.command()
def fetch(
    sources: str = typer.Option(
        ...,
        help="Zarezom odvojeni izvori npr. oura,polar,garmin,apple,ross,rollaone",
    ),
    since: str = typer.Option(
        "2d",
        help="Početak: ISO datum (YYYY-MM-DD) ili relativno npr. 7d",
    ),
) -> None:
    """Dovuci svježe podatke i upiši u Google Sheet."""
    start_date = _parse_since(since)
    end_date = dt.date.today()
    _do_range(sources, start_date, end_date)


@app.command()
def backfill(
    start: str = typer.Option(..., help="Početni datum YYYY-MM-DD"),
    end: str = typer.Option(..., help="Završni datum YYYY-MM-DD (uključivo)"),
    sources: str = typer.Option(..., help="Zarezom odvojeni izvori"),
) -> None:
    """Backfill za dani raspon datuma (uključivo)."""
    start_date = dt.date.fromisoformat(start)
    end_date = dt.date.fromisoformat(end)
    _do_range(sources, start_date, end_date)


@app.command("playwright-login")
def playwright_login(target: str = typer.Argument(..., help="garmin|ross|rollaone")) -> None:
    """Pomoćni hint za Playwright login skripte (state JSON)."""
    if target not in {"garmin", "ross", "rollaone"}:
        raise typer.BadParameter("target mora biti jedno od: garmin|ross|rollaone")
    typer.echo(f"Pokreni: python -m health_sync.playwright.{target}_login (nakon što postaviš .env za taj servis)")


# ---------- core range dohvat ----------

def _do_range(sources: str, start_date: dt.date, end_date: dt.date) -> None:
    if start_date > end_date:
        raise typer.BadParameter("start date je nakon end date")

    selected = {s.strip().lower() for s in sources.split(",") if s.strip()}
    if not selected:
        raise typer.BadParameter("Nisi naveo nijedan izvor.")

    modules = _load_selected_sources(selected)
    if not modules:
        raise typer.BadParameter("Nijedan traženi izvor se nije učitao.")

    all_rows: list[list[Optional[str | float | int]]] = []
    for day in _iter_days(start_date, end_date):
        for name, mod in modules.items():
            try:
                if hasattr(mod, "fetch_day"):
                    rows = mod.fetch_day(day)  # očekuje list[UnifiedRow] ili list[list]
                else:
                    rows = mod.fetch_day(day)  # type: ignore  # ako je fetch_day u podmodulu
            except Exception as e:
                log.error("fetch_failed", source=name, date=str(day), error=str(e))
                typer.echo(f"[ERR] {name} {day}: {e}")
                continue

            if not rows:
                continue

            # dozvoli i UnifiedRow i već spremne list-ove
            first = rows[0]
            if hasattr(first, "as_row"):
                rows = [r.as_row() for r in rows]  # type: ignore
            all_rows.extend(rows)  # type: ignore

    if all_rows:
        append_rows(all_rows)
        typer.echo(f"OK — upisano {len(all_rows)} redova u 'Unified'.")
    else:
        typer.echo("Nema novih redova za upis.")


if __name__ == "__main__":
    app()
