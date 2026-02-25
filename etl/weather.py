"""ETL: fetch historical weather from OpenWeatherMap Time Machine and persist it.

For each unique customer location the job:
  1. Finds the last-stored weather timestamp (defaults to BACKFILL_START_DATE).
  2. Builds a 15-minute timestamp grid from that point up to *now*.
  3. Deduplicates to hourly timestamps for API efficiency (the Time Machine
     endpoint returns hourly resolution), then fans the data out to all four
     15-minute slots within each hour.
  4. Upserts every row into the ``weather`` table.

Usage
-----
    python etl/weather.py              # from now back to last stored point
    python etl/weather.py --start 2026-02-20   # override start date
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timedelta, timezone
from typing import NamedTuple

from api.clients.openweather import OpenWeatherClient, OpenWeatherError
from api.config import settings
from api.db.client import DatabaseClient
from lib.time_util import day_window, interval_minutes
from lib.types import TimeInterval

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl.weather")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _Location(NamedTuple):
    latitude: float
    longitude: float


def _floor_to_hour(dt: datetime) -> datetime:
    """Truncate a datetime to the whole hour."""
    return dt.replace(minute=0, second=0, microsecond=0)


def _build_timestamps(start: datetime, end: datetime, time_interval: TimeInterval) -> list[datetime]:
    """Return all 15-minute-aligned UTC-naive datetimes in [start, end]."""
    timestamps: list[datetime] = []
    current = start
    while current <= end:
        timestamps.append(current)
        current += timedelta(minutes=interval_minutes(time_interval))
    return timestamps


def _parse_weather_row(
    lat: float,
    lon: float,
    timestamp: datetime,
    data: dict,
) -> dict:
    """Map a Time Machine ``data[0]`` payload to a ``weather`` table row."""
    weather_desc = ""
    weather_list = data.get("weather", [])
    if weather_list:
        weather_desc = weather_list[0].get("description", "")

    return {
        "latitude": lat,
        "longitude": lon,
        "timestamp": timestamp,
        "temperature": float(data["temp"]),
        "feels_like": float(data["feels_like"]),
        "pressure": int(data["pressure"]),
        "humidity": int(data["humidity"]),
        "dew_point": float(data["dew_point"]),
        "uvi": float(data.get("uvi", 0.0)),
        "clouds": int(data.get("clouds", 0)),
        "visibility": int(data.get("visibility", 0)),
        "wind_speed": float(data["wind_speed"]),
        "wind_degree": int(data["wind_deg"]),
        "description": weather_desc,
    }


# ---------------------------------------------------------------------------
# Core ETL logic
# ---------------------------------------------------------------------------


def run(target_date: date | None = None, time_interval: TimeInterval = "15m") -> None:
    if not settings.OPENWEATHER_API_KEY:
        raise RuntimeError(
            "OPENWEATHER_API_KEY is not set. "
            "Export the variable before running this job."
        )

    db_client = DatabaseClient()
    weather_client = OpenWeatherClient(api_key=settings.OPENWEATHER_API_KEY)

    customers = db_client.list_customers()
    if not customers:
        log.warning("No customers found — nothing to do.")
        return

    # Deduplicate by location so each (lat, lon) pair is only fetched once.
    locations: dict[_Location, None] = {
        _Location(c.latitude, c.longitude): None for c in customers
    }

    backfill_start = datetime.combine(
        date.fromisoformat(settings.BACKFILL_START_DATE),
        datetime.min.time(),
    )

    total_upserted = 0

    for loc in locations:
        lat, lon = loc.latitude, loc.longitude
        log.info("Processing location (%.4f, %.4f)", lat, lon)

        # ------------------------------------------------------------------
        # Step 1 — determine start of fetch window
        # ------------------------------------------------------------------
        if target_date:
            start_dt, end_dt = day_window(target_date, limit_to_now=True)
        else:
            last_ts = db_client.get_last_weather_timestamp(lat, lon)
            if last_ts is not None:
                # Resume from the next 15-minute slot after the last stored row.
                start_dt = last_ts + timedelta(minutes=interval_minutes(time_interval))
                log.info("  Resuming from %s", start_dt.isoformat())
            else:
                start_dt = backfill_start
                log.info("  No existing data — backfilling from %s", start_dt.isoformat())
            
            end_dt = datetime.now(tz=timezone.utc).replace(tzinfo=None)

        if start_dt >= end_dt:
            log.info("  Already up-to-date. Skipping.")
            continue

        # ------------------------------------------------------------------
        # Step 2 — build the timestamp grid
        # ------------------------------------------------------------------
        timestamps = _build_timestamps(start_dt, end_dt, time_interval)
        log.info("  %d timestamps to fill (%.1f hours)", len(timestamps), len(timestamps) / 4)

        # Deduplicate to unique hours so we make one API call per hour.
        unique_hours: dict[datetime, list[datetime]] = {}
        for ts in timestamps:
            hour = _floor_to_hour(ts)
            unique_hours.setdefault(hour, []).append(ts)

        # ------------------------------------------------------------------
        # Step 3 — fetch from Time Machine and store
        # ------------------------------------------------------------------
        rows: list[dict] = []

        for hour_ts, slot_timestamps in sorted(unique_hours.items()): # TODO: limit total calls by only fetching on published intervals
            try:
                response = weather_client.get_timemachine(
                    lat=lat,
                    lon=lon,
                    dt=hour_ts.replace(tzinfo=timezone.utc),
                )
            except OpenWeatherError as exc:
                log.error(
                    "  OpenWeather error for %s — skipping hour.  HTTP %s: %s",
                    hour_ts.isoformat(),
                    exc.status_code,
                    exc.message,
                )
                continue
            except Exception as exc:  # noqa: BLE001
                log.error(
                    "  Unexpected error for %s — skipping hour.  %s",
                    hour_ts.isoformat(),
                    exc,
                )
                continue

            data_points = response.get("data", [])
            if not data_points:
                log.warning("  No data returned for %s — skipping hour.", hour_ts.isoformat())
                continue

            # Fan the single hourly reading out to all 15-min slots.
            for slot_ts in slot_timestamps:
                rows.append(_parse_weather_row(lat, lon, slot_ts, data_points[0]))

        if rows:
            db_client.upsert_weather_bulk(rows)
            total_upserted += len(rows)
            log.info("  Upserted %d row(s).", len(rows))
        else:
            log.info("  No rows to upsert.")

    log.info("ETL complete — %d total row(s) upserted.", total_upserted)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Fetch and store weather time series for all customer locations."
    )
    parser.add_argument(
        "--start",
        metavar="YYYY-MM-DD",
        help="Override the start date instead of resuming from the last stored point.",
        default=None,
    )
    args = parser.parse_args()

    start: date | None = None
    if args.start:
        try:
            start = date.fromisoformat(args.start)
        except ValueError:
            print(f"Invalid date: {args.start!r}. Expected YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)

    run(start_override=start)
