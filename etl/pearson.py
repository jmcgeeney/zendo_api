from __future__ import annotations

import logging
import math
import sys
from datetime import date, datetime, time, timedelta

from api.db.client import DatabaseClient

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl.pearson")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_INTERVAL = timedelta(minutes=15)
_WINDOW = timedelta(hours=24)
_EXPECTED_READINGS = 96  # per day at 15-min resolution


# ---------------------------------------------------------------------------
# Pure-Python Pearson r
# ---------------------------------------------------------------------------


def _pearson(xs: list[float], ys: list[float]) -> float | None:
    """Return the Pearson correlation coefficient for two equal-length lists.

    Returns ``None`` if:
    - fewer than 2 paired values are available, or
    - either series has zero variance (constant → r is undefined).
    """
    n = len(xs)
    if n < 2:
        return None

    sum_x = sum(xs)
    sum_y = sum(ys)
    sum_xy = sum(x * y for x, y in zip(xs, ys))
    sum_x2 = sum(x * x for x in xs)
    sum_y2 = sum(y * y for y in ys)

    num = n * sum_xy - sum_x * sum_y
    den_sq = (n * sum_x2 - sum_x ** 2) * (n * sum_y2 - sum_y ** 2)

    if den_sq <= 0.0:
        return None  # one or both series is constant

    return num / math.sqrt(den_sq)


# ---------------------------------------------------------------------------
# Core ETL logic
# ---------------------------------------------------------------------------


def run(target_date: date | None = None) -> None:
    target_date = target_date or date.today()
    log.info("Running Pearson ETL for %s", target_date.isoformat())

    db = DatabaseClient()
    customers = db.list_customers()
    if not customers:
        log.warning("No customers found — nothing to do.")
        return

    # Fetch window: one full day before target_date through end of target_date.
    # This gives us up to 192 readings to draw the trailing 24h window from.
    fetch_start = datetime.combine(target_date - timedelta(days=1), time.min)
    fetch_end = datetime.combine(target_date, time.min) + timedelta(hours=23, minutes=45)

    # The 96 output timestamps we produce coefficients for.
    day_start = datetime.combine(target_date, time.min)
    output_timestamps = [day_start + _INTERVAL * i for i in range(_EXPECTED_READINGS)]

    total_upserted = 0

    for c in customers:
        log.info("  Processing customer %r (id=%d) …", c.name, c.customer_id)

        # ------------------------------------------------------------------
        # Fetch all series for the two-day window and index by timestamp.
        # ------------------------------------------------------------------
        irr_rows = db.get_irradiance_series(
            lat=c.latitude, lon=c.longitude, start=fetch_start, end=fetch_end
        )
        prod_rows = db.get_production_series(
            customer_id=c.customer_id, start=fetch_start, end=fetch_end
        )
        weather_rows = db.get_weather_series(
            lat=c.latitude, lon=c.longitude, start=fetch_start, end=fetch_end
        )
        cons_rows = db.get_consumption_series(
            customer_id=c.customer_id, start=fetch_start, end=fetch_end
        )

        irr_by_ts: dict[datetime, float] = {r.timestamp: r.irradiance for r in irr_rows}
        prod_by_ts: dict[datetime, float] = {r.timestamp: r.power for r in prod_rows}
        temp_by_ts: dict[datetime, float] = {r.timestamp: r.temperature for r in weather_rows}
        cons_by_ts: dict[datetime, float] = {r.timestamp: r.power for r in cons_rows}

        if not irr_by_ts and not temp_by_ts:
            log.warning(
                "  No source data found for customer %d — skipping.", c.customer_id
            )
            continue

        # ------------------------------------------------------------------
        # For each output timestamp, slice the trailing 24h window and
        # compute both Pearson coefficients.
        # ------------------------------------------------------------------
        output_rows: list[dict] = []

        for ts in output_timestamps:
            window_start = ts - _WINDOW + _INTERVAL  # inclusive lower bound

            # Collect timestamps in [window_start, ts] in order.
            window_ts = [
                window_start + _INTERVAL * i
                for i in range(int(_WINDOW / _INTERVAL))
            ]

            irr_prod_pairs = [
                (irr_by_ts[t], prod_by_ts[t])
                for t in window_ts
                if t in irr_by_ts and t in prod_by_ts
            ]
            temp_cons_pairs = [
                (temp_by_ts[t], cons_by_ts[t])
                for t in window_ts
                if t in temp_by_ts and t in cons_by_ts
            ]

            irr_vals, prod_vals = (
                zip(*irr_prod_pairs) if irr_prod_pairs else ([], [])
            )
            temp_vals, cons_vals = (
                zip(*temp_cons_pairs) if temp_cons_pairs else ([], [])
            )

            output_rows.append(
                {
                    "customer_id": c.customer_id,
                    "timestamp": ts,
                    "solar_irradiance_vs_production": _pearson(
                        list(irr_vals), list(prod_vals)
                    ),
                    "temperature_vs_consumption": _pearson(
                        list(temp_vals), list(cons_vals)
                    ),
                }
            )

        db.upsert_pearson_bulk(output_rows)
        total_upserted += len(output_rows)
        log.info("  Upserted %d rows.", len(output_rows))

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
        description="Compute rolling 24-hour Pearson coefficients for all customers."
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
        log.exception("Pearson ETL failed: %s", exc)
        sys.exit(1)
