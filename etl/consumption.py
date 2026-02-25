"""ETL job: run the datacenter simulator for each customer using the
weather data already in the database and store the resulting
consumption time series in the ``consumption`` table.

This job depends on :mod:`etl.weather` having been run first to
populate the ``weather`` table for the target date.

Run from the repository root with ``api/`` on the Python path::

    PYTHONPATH=api python -m etl.consumption
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, time
from typing import Any

from api.simulators.datacenter import DatacenterSimulator
from api.db.client import DatabaseClient

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
# Simulator defaults
#
# Every customer is modelled as a datacenter with these parameters.
# Extend _simulator_for_customer() to load per-customer values (e.g. from a
# config file or additional database columns) when that data is available.
# ---------------------------------------------------------------------------

_INTERVAL_HOURS: float = 0.25  # 15-min grid

_DEFAULT_SIMULATOR_PARAMS: dict[str, Any] = {
    "it_load_kw": 1_000.0,      # 1 MW IT load
    "utilisation": 0.60,
    "pue_base": 1.40,
    "pue_temp_coeff": 0.01,
    "temp_setpoint": 10.0,         # free-cooling threshold (°C) — realistic for UK climate
    "tau_cooling_hours": 1.0,
    "tau_mass_hours": 6.0,
    "alpha": 0.70,
    "interval_hours": _INTERVAL_HOURS,
}


# ---------------------------------------------------------------------------
# Core ETL logic
# ---------------------------------------------------------------------------


def run(target_date: date | None = None) -> None:
    target_date = target_date or date.today()
    log.info("Running consumption ETL for %s", target_date.isoformat())

    db = DatabaseClient()
    customers = db.list_customers()

    if not customers:
        log.warning("No customers found in the database — nothing to do.")
        return

    # Time window covering every 15-min slot in the target day
    window_start = datetime.combine(target_date, time(0, 0, 0))
    window_end   = datetime.combine(target_date, time(23, 45, 0))

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
            start=window_start,
            end=window_end,
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
        simulator = _simulator_for_customer(customer)
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


def _simulator_for_customer(customer: Any) -> Any:
    params = {**_DEFAULT_SIMULATOR_PARAMS}
    # Future: params.update(load_customer_dc_config(customer.customer_id))
    return DatacenterSimulator(
        it_load_kw=1000.0,      # 1 MW IT load
        utilisation=0.60,
        pue_base=1.40,
        pue_temp_coeff=0.01,
        temp_setpoint=20.0,         # free-cooling threshold (°C) — realistic for UK climate
        tau_cooling_hours=1.0,
        tau_mass_hours=6.0,
        alpha=0.70,
        interval_hours=_INTERVAL_HOURS,
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
