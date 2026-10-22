#!/usr/bin/env python3
"""Add sample_quality and quality_flags columns to the options Iceberg table.

These columns were added to TABLE_DEFS['options'] after the initial migration
and need to be applied to the live table via schema evolution. Existing rows
will return null for both columns.

Already applied: 2026-10-21

Usage:
    python scripts/migrate_options_add_quality_columns.py
"""

from __future__ import annotations

import os

from pyiceberg.catalog import load_catalog
from pyiceberg.types import StringType


def main() -> None:
    cat = load_catalog(
        "glue",
        **{
            "type": "glue",
            "region": os.environ["AWS_REGION"],
            "profile": os.environ["AWS_PROFILE"],
        },
    )
    t = cat.load_table("tractatus.options")
    existing = {f.name for f in t.schema().fields}

    to_add = [
        ("sample_quality", StringType()),
        ("quality_flags", StringType()),
    ]
    pending = [(name, typ) for name, typ in to_add if name not in existing]

    if not pending:
        print("Nothing to do — columns already present.")
        return

    with t.update_schema() as upd:
        for name, typ in pending:
            upd.add_column(name, typ)
            print(f"Added: {name}")

    print("Done.")


if __name__ == "__main__":
    main()
