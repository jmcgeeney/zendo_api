from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import date, timedelta

from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.router import router
from api.config import settings
from etl.orchestration import run_etl_chain

log = logging.getLogger(__name__)

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
        run_etl_chain(current)
        current += timedelta(days=1)

    log.info("Backfill complete.")


def _scheduled_job() -> None:
    """ETL job that runs every 15 minutes — processes today's date."""
    run_etl_chain(date.today())


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
