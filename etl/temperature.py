from __future__ import annotations

import logging
import sys
from datetime import date, datetime, time, timedelta
from typing import NamedTuple
from api.clients.openweather import OpenWeatherClient, OpenWeatherError
from api.clients.weather_simulator import WeatherSimulatorClient
from api.config import settings
from api.db.client import DatabaseClient

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl.temperature")

# ---------------------------------------------------------------------------
# Lazy imports — these depend on ``api/`` being on PYTHONPATH.  We defer them
# to ``run()`` so that import errors are surfaced with a helpful message.
# ---------------------------------------------------------------------------


class _Location(NamedTuple):
    latitude: float
    longitude: float


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_TIME_SLICE = "15m"          # 15-minute intervals → 96 readings per day
_INTERVAL_MINUTES = 15       # must match _TIME_SLICE


def _timestamps_for_day(day: date) -> list[datetime]:
    """Return the 96 UTC-naive datetime objects for a 15-min grid over *day*."""
    start = datetime.combine(day, time(0, 0, 0))
    return [start + timedelta(minutes=_INTERVAL_MINUTES * i) for i in range(96)]


# ---------------------------------------------------------------------------
# Core ETL logic
# ---------------------------------------------------------------------------


def run(target_date: date | None = None) -> None:
    if not settings.OPENWEATHER_API_KEY:
        raise RuntimeError(
            "OPENWEATHER_API_KEY is not set. "
            "Export the variable before running this job."
        )

    target_date = target_date or date.today()
    log.info("Running temperature ETL for %s", target_date.isoformat())

    db_client = DatabaseClient()
    weather_client = OpenWeatherClient(api_key=settings.OPENWEATHER_API_KEY)
    sim_client = WeatherSimulatorClient(openweather=weather_client)

    # ------------------------------------------------------------------
    # Fetch customers and deduplicate by location so we only call the
    # weather API once per unique (lat, lon) pair.
    # ------------------------------------------------------------------
    customers = db_client.list_customers()

    timestamps = _timestamps_for_day(target_date)
    total_upserted = 0

    for c in customers:
        try:
            temperatures = sim_client.get_temperature_series(
                lat=c.latitude,
                lon=c.longitude,
                start_date=target_date,
                end_date=target_date,
                time_slice=_TIME_SLICE,
            )
        except OpenWeatherError as exc:
            log.error(
                "  OpenWeather error for (%.4f, %.4f) — skipping.  "
                "HTTP %s: %s",
                c.latitude,
                c.longitude,
                exc.status_code,
                exc.message,
            )
            continue
        except Exception as exc:  # noqa: BLE001
            log.error(
                "  Unexpected error for (%.4f, %.4f) — skipping.  %s",
                c.latitude,
                c.longitude,
                exc,
            )
            continue

        if len(temperatures) != len(timestamps):
            log.warning(
                "  Expected %d readings but got %d for (%.4f, %.4f) — skipping.",
                len(timestamps),
                len(temperatures),
                c.latitude,
                c.longitude,
            )
            continue

        rows = [
            {
                "latitude": c.latitude,
                "longitude": c.longitude,
                "timestamp": ts,
                "temperature": temp,
            }
            for ts, temp in zip(timestamps, temperatures)
        ]

        db_client.upsert_temperature_bulk(rows)
        total_upserted += len(rows)
        log.info("  Upserted %d rows.", len(rows))

    log.info(
        "ETL complete — %d total row(s) upserted for %s.",
        total_upserted,
        target_date.isoformat(),
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and store today's simulated temperature series for all customers."
    )
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Target date (default: today)",
        default=None,
    )
    args = parser.parse_args()

    target: date | None = None
    if args.date:
        try:
            target = date.fromisoformat(args.date)
        except ValueError:
            print(f"Invalid date: {args.date!r}. Expected YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)

    run(target_date=target)
