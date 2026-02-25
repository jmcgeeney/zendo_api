from __future__ import annotations

import logging
import sys
from datetime import date, datetime, time, timedelta
from typing import Any

from api.simulators.datacenter import DatacenterSimulator
from api.db.client import DatabaseClient
from lib.time_util import day_window, interval_hours
from lib.types import TimeInterval

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl.consumption")


# ---------------------------------------------------------------------------
# Core ETL logic
# ---------------------------------------------------------------------------


def run(target_date: date | None = None, time_interval: TimeInterval = "15m") -> None:
    target_date = target_date or date.today()
    log.info("Running consumption ETL for %s", target_date.isoformat())

    db = DatabaseClient()
    customers = db.list_customers()

    if not customers:
        log.warning("No customers found in the database — nothing to do.")
        return

    # Time window covering every interval in the target day
    start_time, end_time = day_window(target_date, limit_to_now=True)

    log.info("Processing %d customer(s).", len(customers))

    total_upserted = 0

    for customer in customers:
        log.info(
            "  Customer %d (%s)  loc=(%.4f, %.4f)",
            customer.customer_id,
            customer.name,
            customer.latitude,
            customer.longitude,
        )

        # ------------------------------------------------------------------ #
        # 1. Load weather data for this customer's location                   #
        # ------------------------------------------------------------------ #
        weather_rows = db.get_weather_series(
            lat=customer.latitude,
            lon=customer.longitude,
            start=start_time,
            end=end_time,
        )

        if not weather_rows:
            log.warning(
                "  No weather data for (%.4f, %.4f) on %s — skipping.  "
                "Run etl.weather first.",
                customer.latitude,
                customer.longitude,
                target_date.isoformat(),
            )
            continue

        temperatures = [row.temperature for row in weather_rows]
        timestamps   = [row.timestamp   for row in weather_rows]

        # ------------------------------------------------------------------ #
        # 2. Simulate facility power demand                                   #
        # ------------------------------------------------------------------ #
        simulator = _simulator_for_customer(customer, time_interval)
        loads = simulator.simulate(temperatures)

        # simulate() preserves length, but guard anyway
        if len(loads) != len(timestamps):
            log.error(
                "  Simulator returned %d values for %d timestamps — skipping.",
                len(loads),
                len(timestamps),
            )
            continue

        # ------------------------------------------------------------------ #
        # 3. Persist                                                           #
        # ------------------------------------------------------------------ #
        rows = [
            {
                "customer_id": customer.customer_id,
                "timestamp":   ts,
                "power":       power,
            }
            for ts, power in zip(timestamps, loads)
        ]

        db.upsert_consumption_bulk(rows)
        total_upserted += len(rows)
        log.info(
            "  Upserted %d rows  (%.1f kW – %.1f kW).",
            len(rows),
            min(loads),
            max(loads),
        )

    log.info(
        "ETL complete — %d total row(s) upserted for %s.",
        total_upserted,
        target_date.isoformat(),
    )


def _simulator_for_customer(customer: Any, time_interval: TimeInterval) -> Any:
    return DatacenterSimulator(
        it_load_kw=1000.0,      # 1 MW IT load
        utilisation=0.60,
        pue_base=1.40,
        pue_temp_coeff=0.01,
        temp_setpoint=20.0,         # free-cooling threshold (°C) — realistic for UK climate
        tau_cooling_hours=1.0,
        tau_mass_hours=6.0,
        alpha=0.70,
        interval_hours=interval_hours(time_interval), # Defaults to 15m
        jitter=0.5,
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Simulate datacenter consumption for all customers using "
            "temperature data already stored in the database."
        )
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
