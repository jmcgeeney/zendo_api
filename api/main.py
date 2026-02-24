from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import date, timedelta

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from router import router
from config import settings

log = logging.getLogger(__name__)


def _run_etl_chain(target_date: date) -> None:
    """Run the full ETL pipeline for *target_date* in dependency order.

    Step 1 — temperature   (independent source fetch)
    Step 2 — irradiance    (independent source fetch)
    Step 3 — consumption   (depends on temperature)
    Step 4 — production    (depends on irradiance + temperature)
    Step 5 — pearson       (depends on consumption + production)
    """
    from etl.consumption import run as run_consumption
    from etl.irradiance import run as run_irradiance
    from etl.pearson import run as run_pearson
    from etl.production import run as run_production
    from etl.temperature import run as run_temperature

    # -- Step 1: temperature ---------------------------------------------
    try:
        run_temperature(target_date=target_date)
    except Exception as exc:
        log.error("Temperature ETL failed for %s: %s", target_date, exc)
        return  # consumption, production, and pearson all depend on this

    # -- Step 2: irradiance ----------------------------------------------
    try:
        run_irradiance(target_date=target_date)
    except Exception as exc:
        log.error("Irradiance ETL failed for %s: %s", target_date, exc)
        return  # production and pearson depend on this

    # -- Step 3: consumption (depends on temperature) --------------------
    try:
        run_consumption(target_date=target_date)
    except Exception as exc:
        log.error("Consumption ETL failed for %s: %s", target_date, exc)
        return  # pearson depends on this

    # -- Step 4: production (depends on irradiance + temperature) --------
    try:
        run_production(target_date=target_date)
    except Exception as exc:
        log.error("Production ETL failed for %s: %s", target_date, exc)
        return  # pearson depends on this

    # -- Step 5: pearson (depends on consumption + production) -----------
    try:
        run_pearson(target_date=target_date)
    except Exception as exc:
        log.error("Pearson ETL failed for %s: %s", target_date, exc)


def _run_backfill() -> None:
    """Run ETL for every day from BACKFILL_START_DATE up to and including today."""
    try:
        start = date.fromisoformat(settings.BACKFILL_START_DATE)
    except ValueError:
        log.error(
            "Invalid BACKFILL_START_DATE %r — expected YYYY-MM-DD. Skipping backfill.",
            settings.BACKFILL_START_DATE,
        )
        return

    today = date.today()
    if start > today:
        log.warning(
            "BACKFILL_START_DATE %s is in the future — nothing to backfill.", start
        )
        return

    total_days = (today - start).days + 1
    log.info(
        "Starting backfill from %s to %s (%d day(s)).", start, today, total_days
    )

    current = start
    while current <= today:
        log.info("Backfilling %s ...", current)
        _run_etl_chain(current)
        current += timedelta(days=1)

    log.info("Backfill complete.")


def _scheduled_job() -> None:
    """ETL job that runs every 15 minutes — processes today's date."""
    _run_etl_chain(date.today())


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Backfill runs in a thread so it doesn't block the event loop
    await asyncio.to_thread(_run_backfill)

    scheduler = BackgroundScheduler(executors={"default": ThreadPoolExecutor(1)})
    scheduler.add_job(_scheduled_job, "interval", minutes=15, id="etl_15min")
    scheduler.start()
    log.info("APScheduler started — ETL job runs every 15 minutes.")

    yield

    scheduler.shutdown(wait=False)
    log.info("APScheduler stopped.")


def create_app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://localhost:3000"],
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.include_router(router, prefix="/api")
    return app
