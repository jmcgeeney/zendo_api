#!/usr/bin/env python

from __future__ import annotations

import argparse
import os
from pathlib import Path

from api.config import settings

# Resolve sqlite:///relative.db paths to INSTANCE_DIR/<name>.db so the file
# lands in a predictable, .gitignore-able location.
_INSTANCE_DIR = Path(os.environ.get("INSTANCE_DIR", "/tmp/zendo/instance"))
_INSTANCE_DIR.mkdir(parents=True, exist_ok=True)

_db_url = settings.DATABASE_URL
if _db_url.startswith("sqlite:///") and not _db_url.startswith("sqlite:////"):
    # Relative SQLite URL — rewrite to use instance/ directory.
    _db_file = _db_url.removeprefix("sqlite:///")
    if not Path(_db_file).is_absolute():
        _db_url = f"sqlite:///{_INSTANCE_DIR / _db_file}"
        os.environ["DATABASE_URL"] = _db_url  # propagate before engine is created

from api.db.models import Base  # noqa: E402 — must follow DATABASE_URL env patch
from api.db.session import engine  # noqa: E402 — must follow DATABASE_URL env patch


def init_db(drop: bool = False) -> None:
    db_url = str(engine.url)

    if drop:
        print("Dropping all existing tables …")
        Base.metadata.drop_all(bind=engine)
        print("Done.")

    print(f"Creating tables in: {db_url}")
    Base.metadata.create_all(bind=engine)

    table_names = list(Base.metadata.tables.keys())
    print(f"{len(table_names)} table(s) ready: {', '.join(sorted(table_names))}")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Initialise the Zendo SQLite database.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--drop",
        action="store_true",
        default=False,
        help="Drop all existing tables before creating them (DESTRUCTIVE).",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    init_db(drop=args.drop)
