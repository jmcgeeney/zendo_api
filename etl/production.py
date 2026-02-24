from __future__ import annotations

import logging
import sys
from datetime import date, datetime, time, timedelta

from api.config import settings
from api.db.client import DatabaseClient
from api.simulators.solar import SolarSimulator

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl.production")

# ---------------------------------------------------------------------------
# Simulator defaults
#
# Extend _simulator_for_customer() to load per-customer values (e.g. from a
# config file or additional database columns) when that data is available.
# ---------------------------------------------------------------------------

_INTERVAL_HOURS: float = 0.25          # 15-minute grid
_EXPECTED_READINGS: int = 96           # per day at 15m resolution

_DEFAULT_SIMULATOR_PARAMS = dict(
    installed_capacity_kw=500.0,        # 500 kWp installation
    performance_ratio=0.80,
    temp_coefficient=-0.004,
    noct_delta_t=25.0,
    interval_hours=_INTERVAL_HOURS,
)


def _simulator_for_customer(customer_id: int) -> SolarSimulator:
    """Return a SolarSimulator configured for *customer_id*.

    All customers share the same defaults for now.  Replace this function
    with per-customer config loading when that data is available.
    """
    return SolarSimulator(**_DEFAULT_SIMULATOR_PARAMS)


def _day_window(day: date) -> tuple[datetime, datetime]:
    """Return (start, end) datetime pair covering every 15-min slot in *day*."""
    start = datetime.combine(day, time.min)
    end = start + timedelta(hours=23, minutes=45)
    return start, end


def _timestamps_for_day(day: date) -> list[datetime]:
    start = datetime.combine(day, time.min)
    return [start + timedelta(minutes=15 * i) for i in range(_EXPECTED_READINGS)]


# ---------------------------------------------------------------------------
# Core ETL logic
# ---------------------------------------------------------------------------


def run(target_date: date | None = None) -> None:
    target_date = target_date or date.today()
    log.info("Running production ETL for %s", target_date.isoformat())

    db = DatabaseClient()
    customers = db.list_customers()
    if not customers:
        log.warning("No customers found — nothing to do.")
        return

    start_dt, end_dt = _day_window(target_date)
    timestamps = _timestamps_for_day(target_date)
    total_upserted = 0

    for c in customers:
        log.info("  Processing customer %r (id=%d) …", c.name, c.customer_id)

        # ------------------------------------------------------------------
        # Irradiance — required
        # ------------------------------------------------------------------
        irradiance_rows = db.get_irradiance_series(
            lat=c.latitude, lon=c.longitude, start=start_dt, end=end_dt
        )
        if not irradiance_rows:
            log.warning(
                "  No irradiance data for (%.4f, %.4f) on %s — skipping.",
                c.latitude,
                c.longitude,
                target_date,
            )
            continue

        if len(irradiance_rows) != _EXPECTED_READINGS:
            log.warning(
                "  Expected %d irradiance readings but got %d — skipping.",
                _EXPECTED_READINGS,
                len(irradiance_rows),
            )
            continue

        irradiance = [row.irradiance for row in irradiance_rows]

        # ------------------------------------------------------------------
        # Temperature — optional, enables NOCT derating
        # ------------------------------------------------------------------
        temperature_rows = db.get_temperature_series(
            lat=c.latitude, lon=c.longitude, start=start_dt, end=end_dt
        )
        temperatures: list[float] | None = None
        if temperature_rows and len(temperature_rows) == _EXPECTED_READINGS:
            temperatures = [row.temperature for row in temperature_rows]
        elif temperature_rows:
            log.warning(
                "  Temperature row count (%d) doesn't match irradiance (%d) "
                "— running without temperature derating.",
                len(temperature_rows),
                _EXPECTED_READINGS,
            )

        # ------------------------------------------------------------------
        # Simulate
        # ------------------------------------------------------------------
        simulator = _simulator_for_customer(c.customer_id)
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
