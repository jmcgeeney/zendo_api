from __future__ import annotations

import logging
import sys
from datetime import date, datetime, time, timedelta

from api.db.client import DatabaseClient
from api.simulators.solar import SolarSimulator
from lib.time_util import day_window, interval_hours, timestamps_for_day
from lib.types import TimeInterval

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl.production")


def _simulator_for_customer(interval_hours: float) -> SolarSimulator:
    """Return a SolarSimulator configured for *customer_id*.

    All customers share the same defaults for now.  Replace this function
    with per-customer config loading when that data is available.
    """
    return SolarSimulator(
        installed_capacity_kw=500.0,
        performance_ratio=0.80,
        temp_coefficient=-0.004,
        jitter=3.0,
    )

# ---------------------------------------------------------------------------
# Core ETL logic
# ---------------------------------------------------------------------------


def run(target_date: date | None = None, time_interval: TimeInterval = "15m") -> None:
    target_date = target_date or date.today()
    log.info("Running production ETL for %s", target_date.isoformat())

    db = DatabaseClient()
    customers = db.list_customers()
    if not customers:
        log.warning("No customers found — nothing to do.")
        return

    start_time, end_time = day_window(target_date, limit_to_now=True)
    total_upserted = 0

    for c in customers:
        log.info("  Processing customer %r (id=%d) …", c.name, c.customer_id)

        # ------------------------------------------------------------------
        # Irradiance — required
        # ------------------------------------------------------------------
        irradiance_rows = db.get_irradiance_series(
            lat=c.latitude, lon=c.longitude, start=start_time, end=end_time
        )
        if not irradiance_rows:
            log.warning(
                "  No irradiance data for (%.4f, %.4f) on %s — skipping.",
                c.latitude,
                c.longitude,
                target_date,
            )
            continue

        irradiance = [row.irradiance for row in irradiance_rows]

        # ------------------------------------------------------------------
        # Temperature — from weather table, enables NOCT derating
        # ------------------------------------------------------------------
        weather_rows = db.get_weather_series(
            lat=c.latitude, lon=c.longitude, start=start_time, end=end_time
        )
        temperatures = [row.temperature for row in weather_rows]
        if not temperatures and weather_rows:
            log.warning(
                "  Weather temperature coverage incomplete for (%.4f, %.4f) "
                "— running without temperature derating.",
                c.latitude,
                c.longitude,
            )
        
        if len(irradiance) != len(temperatures):
            log.warning(
                "  Irradiance data length (%d) does not match expected "
                "temperatures length (%d) for customer %d — skipping.",
                len(irradiance),
                len(temperatures),
                c.customer_id,
            )
            continue
        
        timestamps = timestamps_for_day(target_date, time_interval)[:len(irradiance)]

        # ------------------------------------------------------------------
        # Simulate
        # ------------------------------------------------------------------
        simulator = _simulator_for_customer(interval_hours(time_interval))
        power_series = simulator.simulate(irradiance, temperatures)

        # ------------------------------------------------------------------
        # Upsert
        # ------------------------------------------------------------------
        rows = [
            {
                "customer_id": c.customer_id,
                "timestamp": ts,
                "power": power,
            }
            for ts, power in zip(timestamps, power_series)
        ]

        db.upsert_production_bulk(rows)
        total_upserted += len(rows)
        log.info(
            "  Upserted %d rows (temp derating: %s).",
            len(rows),
            "yes" if temperatures is not None else "no",
        )

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
        description="Convert stored irradiance data into solar production estimates."
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
        log.exception("Production ETL failed: %s", exc)
        sys.exit(1)
