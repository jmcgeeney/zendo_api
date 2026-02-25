import logging
import sys
from typing import Optional

from etl.consumption import run as run_consumption
from etl.irradiance import run as run_irradiance
from etl.pearson import run as run_pearson
from etl.production import run as run_production
from etl.weather import run as run_weather

from datetime import date

log = logging.getLogger(__name__)

def run_etl_chain(target_date: date) -> None:
    """Run the full ETL pipeline for *target_date* in dependency order.

    Step 1 — irradiance    (independent source fetch)
    Step 2 — weather       (independent source fetch, not currently used)
    Step 3 — consumption   (depends on weather)
    Step 4 — production    (depends on irradiance)
    Step 5 — pearson       (depends on consumption + production)
    """

    # -- Step 1: irradiance ----------------------------------------------
    try:
        run_irradiance(target_date=target_date)
    except Exception as exc:
        log.error("Irradiance ETL failed for %s: %s", target_date, exc)
        return  # production and pearson depend on this

    # -- Step 2: weather (independent source fetch) ------------------------
    try:
        run_weather(target_date=target_date)
    except Exception as exc:
        log.error("Weather ETL failed for %s: %s", target_date, exc)
        return  # production and pearson depends on this

    # -- Step 3: consumption (depends on weather) ------------------------
    try:
        run_consumption(target_date=target_date)
    except Exception as exc:
        log.error("Consumption ETL failed for %s: %s", target_date, exc)
        return  # pearson depends on this

    # -- Step 3: production (depends on irradiance) ----------------------
    try:
        run_production(target_date=target_date)
    except Exception as exc:
        log.error("Production ETL failed for %s: %s", target_date, exc)
        return  # pearson depends on this

    # -- Step 4: pearson (depends on consumption + production) -----------
    try:
        run_pearson(target_date=target_date)
    except Exception as exc:
        log.error("Pearson ETL failed for %s: %s", target_date, exc)



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Gather/simulate all ETL data for a given date and compute Pearson correlations for all customers."
    )
    parser.add_argument(
        "--date",
        metavar="YYYY-MM-DD",
        help="Target date (default: today)",
        default=None,
    )
    args = parser.parse_args()

    target: Optional[date] = None
    if args.date:
        try:
            target = date.fromisoformat(args.date)
        except ValueError:
            print(f"Invalid date: {args.date!r}. Expected YYYY-MM-DD.", file=sys.stderr)
            sys.exit(1)

    try:
        run_etl_chain(target_date=target)
    except Exception as exc:
        log.exception("ETL failed: %s", exc)
        sys.exit(1)