"""ETL job: fetch solar irradiance from the OpenWeather Solar API and store
the resulting time series in the ``irradiance`` table.

Irradiance is stored per unique customer location at 15-minute resolution
(96 readings per day).  The ``cloudy_sky.ghi`` (Global Horizontal Irradiance)
value is used as it accounts for actual cloud cover.

Run from the repository root::

    PYTHONPATH=api python -m etl.irradiance
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timezone

from api.clients.openweather import OpenWeatherClient, OpenWeatherError
from api.config import settings
from api.db.client import DatabaseClient
from lib.time_util import interval_minutes, intervals_per_day
from lib.types import TimeInterval

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl.irradiance")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _current_interval_cutoff(time_interval: TimeInterval) -> datetime:
    """Return the start of the current interval (UTC, timezone-aware)."""
    now = datetime.now(tz=timezone.utc)
    floored_minute = (now.minute // interval_minutes(time_interval)) * interval_minutes(time_interval)
    return now.replace(minute=floored_minute, second=0, microsecond=0)


def _parse_intervals(response: dict) -> list[tuple[datetime, float]]:
    """Extract (timestamp, ghi) pairs from the OpenWeather solar response.

    Uses ``cloudy_sky.ghi`` (W/m²) — Global Horizontal Irradiance adjusted
    for cloud cover.
    """
    intervals = response.get("intervals", [])
    date_str = response['date']
    tz_str = response['tz']
    results: list[tuple[datetime, float]] = []
    for entry in intervals:
        time_str: str = entry["start"]
        # The API returns ISO-8601 strings without timezone info — treat as UTC.
        ts = datetime.fromisoformat(f"{date_str}T{time_str}{tz_str}")
        ghi: float = entry["avg_irradiance"]["cloudy_sky"]["ghi"]
        results.append((ts, ghi))
    return results


# ---------------------------------------------------------------------------
# Core ETL logic
# ---------------------------------------------------------------------------


def run(target_date: date | None = None, time_interval: TimeInterval = "15m") -> None:
    if not settings.OPENWEATHER_API_KEY:
        raise RuntimeError(
            "OPENWEATHER_API_KEY is not set. "
            "Export the variable before running this job."
        )

    target_date = target_date or date.today()
    log.info("Running irradiance ETL for %s", target_date.isoformat())

    is_today = target_date == date.today()
    cutoff = _current_interval_cutoff(time_interval) if is_today else None
    if cutoff is not None:
        log.info("Today's run — limiting to intervals through %s UTC.", cutoff.strftime("%H:%M"))

    db_client = DatabaseClient()
    ow_client = OpenWeatherClient(api_key=settings.OPENWEATHER_API_KEY)

    customers = db_client.list_customers()
    if not customers:
        log.warning("No customers found — nothing to do.")
        return

    # Deduplicate by location so we only make one API call per unique (lat, lon).
    seen: set[tuple[float, float]] = set()
    total_upserted = 0

    try:
        for c in customers:
            loc = (c.latitude, c.longitude)
            if loc in seen:
                log.debug("  Skipping duplicate location (%.4f, %.4f).", *loc)
                continue
            seen.add(loc)

            log.info(
                "  Fetching irradiance for customer %r (%.4f, %.4f) …",
                c.name,
                c.latitude,
                c.longitude,
            )

            try:
                response = ow_client.get_solar_irradiance(
                    lat=c.latitude,
                    lon=c.longitude,
                    day=target_date,
                    interval=time_interval,
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

            readings = _parse_intervals(response)

            if cutoff is not None:
                readings = [(ts, ghi) for ts, ghi in readings if ts <= cutoff]
                log.info(
                    "  Trimmed to %d reading(s) through current interval (%s UTC).",
                    len(readings),
                    cutoff.strftime("%H:%M"),
                )
            elif len(readings) != intervals_per_day(time_interval):
                log.warning(
                    "  Expected %d readings but got %d for (%.4f, %.4f) — skipping.",
                    intervals_per_day(time_interval),
                    len(readings),
                    c.latitude,
                    c.longitude,
                )
                continue

            if not readings:
                log.warning(
                    "  No readings available for (%.4f, %.4f) up to current interval — skipping.",
                    c.latitude,
                    c.longitude,
                )
                continue

            rows = [
                {
                    "latitude": c.latitude,
                    "longitude": c.longitude,
                    "timestamp": ts,
                    "irradiance": ghi,
                }
                for ts, ghi in readings
            ]

            db_client.upsert_irradiance_bulk(rows)
            total_upserted += len(rows)
            log.info("  Upserted %d rows.", len(rows))

    finally:
        ow_client.close()

    log.info(
        "ETL complete — %d total row(s) upserted for %s.",
        total_upserted,
        target_date.isoformat(),
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and store solar irradiance for all customer locations."
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

    try:
        run(target_date=target)
    except Exception as exc:
        log.exception("Irradiance ETL failed: %s", exc)
        sys.exit(1)
