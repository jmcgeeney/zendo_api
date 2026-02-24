"""Seed script: ensure the customers table contains the canonical set of
customers.

This script is **idempotent** — it uses an INSERT OR IGNORE strategy so it is
safe to run multiple times.  It will only insert a row when no customer with
the given ``customer_id`` already exists; existing rows are left untouched.

Run from the repository root with ``api/`` on the Python path::

    PYTHONPATH=api python -m etl.customers
"""

from __future__ import annotations

import logging
import sys

from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from api.db.models import Customer
from api.db.session import get_session

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("etl.customers")

# ---------------------------------------------------------------------------
# Seed data
# ---------------------------------------------------------------------------

_CUSTOMERS: list[dict] = [
    {
        "customer_id": 1,
        "name": "Big Ben",
        "latitude": 50.5,
        "longitude": 0.1,
    },
]


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------


def run() -> None:
    """Insert any missing customers.  Existing rows are never modified."""
    inserted = 0

    with get_session() as db:
        for customer in _CUSTOMERS:
            stmt = (
                sqlite_insert(Customer)
                .values(**customer)
                .on_conflict_do_nothing(index_elements=["customer_id"])
            )
            result = db.execute(stmt)
            if result.rowcount:
                log.info(
                    "Inserted customer id=%d %r (lat=%.4f, lon=%.4f)",
                    customer["customer_id"],
                    customer["name"],
                    customer["latitude"],
                    customer["longitude"],
                )
                inserted += 1
            else:
                log.info(
                    "Customer id=%d %r already exists — skipped.",
                    customer["customer_id"],
                    customer["name"],
                )

    log.info("Done — %d inserted, %d skipped.", inserted, len(_CUSTOMERS) - inserted)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        log.exception("Customer seed failed: %s", exc)
        sys.exit(1)
