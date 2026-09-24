#!/usr/bin/env python3
import argparse
import os
import shutil
from pathlib import Path

DEMO_ROOT = Path(__file__).resolve().parents[1]
BACKENDS = DEMO_ROOT / "backends"
SOURCE = DEMO_ROOT / "sources" / "megapot"


LEGACY_SOURCES = (DEMO_ROOT / "sources" / "clickhouse", DEMO_ROOT / "sources" / "sqlite")


def main() -> None:
    parser = argparse.ArgumentParser(description="Select the Megapot Evidence backend")
    parser.add_argument("backend", choices=("sqlite", "clickhouse"))
    args = parser.parse_args()

    if SOURCE.exists():
        shutil.rmtree(SOURCE)
    for legacy_source in LEGACY_SOURCES:
        if legacy_source.exists():
            shutil.rmtree(legacy_source)
    SOURCE.mkdir(parents=True)
    backend = BACKENDS / args.backend
    shutil.copyfile(backend / "connection.template.yaml", SOURCE / "connection.yaml")
    shutil.copyfile(backend / "events.sql", SOURCE / "events.sql")

    if args.backend == "sqlite":
        database_value = os.environ.get("STREAMLING_BLOCKCHAIN_DB")
        if not database_value:
            raise SystemExit("set STREAMLING_BLOCKCHAIN_DB to the generated Streamling events.db")
        database = Path(database_value).expanduser().resolve()
        if not database.is_file():
            raise SystemExit(f"SQLite database does not exist: {database}")
        (SOURCE / "events.db").symlink_to(database)

    print(f"configured Evidence source: {args.backend}")


if __name__ == "__main__":
    main()
