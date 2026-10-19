#!/usr/bin/env python3
"""Prepare migration: normalize old tickrake parquets to local directory.

Reads old-format parquets, converts columns, normalizes, sorts, and writes
canonical parquet files to an output directory. No S3 or ledger interaction.

For level_one and order_book, multiple intraday snapshots per (symbol, date)
are consolidated into a single file (1 file per symbol per date).

Usage:
    python scripts/migrate_prepare.py --table options --provider schwab -o /tmp/migrate
    python scripts/migrate_prepare.py --table level_one_equities -o /tmp/migrate
    python scripts/migrate_prepare.py --table order_book -o /tmp/migrate --limit 10
    python scripts/migrate_prepare.py --table candles -o /tmp/migrate
    python scripts/migrate_prepare.py --table fundamentals -o /tmp/migrate
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from tractatus.catalog.normalize import normalize_table
from tractatus.catalog.publisher import _sort_table
from tractatus.catalog.schemas import TABLE_DEFS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("migrate_prepare")

# Old options filename: {ROOT}_samples_{DATE}.parquet
_OPTIONS_FILENAME_RE = re.compile(r"^(.+)_samples_(\d{4}-\d{2}-\d{2})\.parquet$")

# Old level_one / order_book filename: {SYMBOL}_{HHMMSSZ}.parquet
_SNAPSHOT_FILENAME_RE = re.compile(r"^(.+)_\d{6}Z\.parquet$")

# Datasets where multiple intraday snapshot files per (symbol, date) must be
# consolidated into a single file before normalization.
_GROUPED_DATASETS = {"level_one_equities", "level_one_futures", "order_book"}

# Data directory names on disk (may differ from table name)
_DATA_DIRS: dict[str, str] = {
    "level_one_equities": "level_one",
    "level_one_futures": "level_one",
}


# ---------------------------------------------------------------------------
# Per-dataset column converters: old parquet → normalizer-compatible Arrow
# ---------------------------------------------------------------------------


def _epoch_ms_to_iso_string(col: pa.Array) -> pa.Array:
    """Convert int64 epoch-milliseconds to UTC ISO 8601 string column."""
    micros = pc.multiply(col.cast(pa.int64()), 1000)
    ts = micros.cast(pa.timestamp("us", tz="UTC"))
    return pc.strftime(ts, format="%Y-%m-%dT%H:%M:%SZ")


def convert_options(table: pa.Table) -> pa.Table:
    """Convert old options parquet to normalizer-compatible format.

    Old parquets vary from 26 to 31 columns. The normalizer's
    validate_input_schema requires all 41 expected columns, so we add
    any missing ones as null here to pass validation.
    """
    from tractatus.catalog.normalize.options import _EXPECTED_INPUT  # noqa: PLC0415

    n = len(table)
    for col_name, col_type in _EXPECTED_INPUT.items():
        if col_name not in table.column_names:
            table = table.append_column(col_name, pa.nulls(n, type=col_type))
        elif col_name == "non_standard" and pa.types.is_string(table.column(col_name).type):
            # Some old parquets have non_standard as string ("true"/"false") instead of bool
            idx = table.column_names.index(col_name)
            col = table.column(col_name)
            bools = pc.equal(col, "true")
            table = table.set_column(idx, col_name, bools)
    return table


def convert_level_one(table: pa.Table) -> pa.Table:
    """Convert old level_one parquet to normalizer-compatible format.

    Old: received_at_ms (int64), quote_time_ms (int64), trade_time_ms (int64)
    New: received_at (string), quote_time (string), trade_time (string)
    """
    renames = {
        "received_at_ms": "received_at",
        "quote_time_ms": "quote_time",
        "trade_time_ms": "trade_time",
    }
    for old_name, new_name in renames.items():
        if old_name in table.column_names:
            idx = table.column_names.index(old_name)
            col = table.column(old_name)
            converted = _epoch_ms_to_iso_string(col)
            table = table.set_column(idx, new_name, converted)
    return table


def convert_order_book(table: pa.Table) -> pa.Table:
    """Convert old order_book parquet to normalizer-compatible format.

    Old: received_at_ms (int64), book_time_ms (int64)
    New: received_at (string), book_time (string)
    """
    renames = {
        "received_at_ms": "received_at",
        "book_time_ms": "book_time",
    }
    for old_name, new_name in renames.items():
        if old_name in table.column_names:
            idx = table.column_names.index(old_name)
            col = table.column(old_name)
            converted = _epoch_ms_to_iso_string(col)
            table = table.set_column(idx, new_name, converted)
    return table


def convert_candles(table: pa.Table) -> pa.Table:
    """Convert old candles parquet — already compatible."""
    return table


def convert_fundamentals(table: pa.Table) -> pa.Table:
    """Convert old fundamentals parquet: rename fetched_at → sampled_at."""
    if "fetched_at" in table.column_names:
        idx = table.column_names.index("fetched_at")
        col = table.column("fetched_at")
        table = table.set_column(idx, "sampled_at", col)
    return table


_CONVERTERS: dict[str, callable] = {
    "options": convert_options,
    "level_one_equities": convert_level_one,
    "level_one_futures": convert_level_one,
    "order_book": convert_order_book,
    "candles": convert_candles,
    "fundamentals": convert_fundamentals,
}


# ---------------------------------------------------------------------------
# File discovery for old parquets
# ---------------------------------------------------------------------------


def _discover_old_parquets(
    data_root: Path,
    table_name: str,
    provider: str | None = None,
    after: date | None = None,
    before: date | None = None,
) -> list[tuple[Path, str, str, date]]:
    """Find old parquet files. Returns (path, provider, root, sample_date)."""
    dir_name = _DATA_DIRS.get(table_name, table_name)
    base = data_root / dir_name

    if not base.is_dir():
        return []

    providers = [provider] if provider else [d.name for d in sorted(base.iterdir()) if d.is_dir()]

    results: list[tuple[Path, str, str, date]] = []

    for prov in providers:
        prov_dir = base / prov
        if not prov_dir.is_dir():
            continue

        for parquet_path in sorted(prov_dir.rglob("*.parquet")):
            parsed = _parse_old_path(parquet_path, prov, table_name, data_root)
            if parsed is None:
                continue
            _, root, sample_dt = parsed
            if after and sample_dt < after:
                continue
            if before and sample_dt >= before:
                continue

            # For level_one, filter equities vs futures by symbol prefix
            if table_name == "level_one_equities" and root.startswith("^"):
                continue
            if table_name == "level_one_futures" and not root.startswith("^"):
                continue

            results.append((parquet_path, prov, root, sample_dt))

    return results


def _parse_old_path(
    path: Path, provider: str, table_name: str, data_root: Path
) -> tuple[str, str, date] | None:
    """Parse an old parquet path into (provider, root, sample_date)."""
    if table_name == "options":
        m = _OPTIONS_FILENAME_RE.match(path.name)
        if not m:
            return None
        root = m.group(1)
        try:
            sample_dt = date.fromisoformat(m.group(2))
        except ValueError:
            return None
        return provider, root, sample_dt

    if table_name in ("level_one_equities", "level_one_futures", "order_book"):
        m = _SNAPSHOT_FILENAME_RE.match(path.name)
        root = m.group(1) if m else path.stem
        rel = path.relative_to(data_root / _DATA_DIRS.get(table_name, table_name) / provider)
        parts = rel.parent.parts
        if len(parts) >= 3:
            try:
                sample_dt = date(int(parts[0]), int(parts[1]), int(parts[2]))
            except ValueError:
                return None
            return provider, root, sample_dt
        return None

    if table_name == "candles":
        rel = path.relative_to(data_root / "candles" / provider)
        parts = rel.parts
        if len(parts) >= 2:
            root = path.stem
            return provider, root, date(1970, 1, 1)
        return None

    if table_name == "fundamentals":
        rel = path.relative_to(data_root / "fundamentals" / provider)
        parts = rel.parts
        if len(parts) >= 3:
            try:
                sample_dt = date(int(parts[0]), int(parts[1]), int(path.stem))
            except ValueError:
                return None
            return provider, "all", sample_dt
        return None

    return None


def _candle_metadata(path: Path, data_root: Path, provider: str) -> tuple[str | None, str | None]:
    """Extract (symbol, interval) for candles from file path."""
    rel = path.relative_to(data_root / "candles" / provider)
    parts = rel.parts
    if len(parts) >= 2:
        return path.stem, parts[0]
    return None, None


def _group_files(
    files: list[tuple[Path, str, str, date]],
) -> dict[tuple[str, str, date], list[tuple[Path, str, str, date]]]:
    """Group files by (provider, symbol, sample_date)."""
    groups: dict[tuple[str, str, date], list[tuple[Path, str, str, date]]] = defaultdict(list)
    for entry in files:
        path, prov, root, sample_dt = entry
        groups[(prov, root, sample_dt)].append(entry)
    return dict(groups)


# ---------------------------------------------------------------------------
# Main preparation logic
# ---------------------------------------------------------------------------


def prepare(
    table_name: str,
    data_root: Path,
    output_dir: Path,
    provider: str | None = None,
    after: date | None = None,
    before: date | None = None,
    limit: int | None = None,
    dry_run: bool = False,
) -> None:
    if table_name not in TABLE_DEFS:
        logger.error("Unknown table: %s", table_name)
        sys.exit(1)

    converter = _CONVERTERS[table_name]
    table_def = TABLE_DEFS[table_name]
    table_output = output_dir / table_name
    table_output.mkdir(parents=True, exist_ok=True)

    logger.info("Discovering old parquets for %s...", table_name)
    files = _discover_old_parquets(
        data_root, table_name, provider=provider, after=after, before=before
    )
    logger.info("Found %d source files", len(files))

    if table_name in _GROUPED_DATASETS:
        groups = _group_files(files)
        if limit:
            groups = dict(list(groups.items())[:limit])

        if dry_run:
            for (prov, root, sample_dt), group_files in groups.items():
                logger.info(
                    "  [dry-run] %s/%s/%s (%d files)",
                    prov,
                    root,
                    sample_dt,
                    len(group_files),
                )
            logger.info(
                "Dry run: %d groups (%d files)",
                len(groups),
                sum(len(v) for v in groups.values()),
            )
            return

        _prepare_grouped(groups, table_name, converter, table_def, table_output)
    else:
        if limit:
            files = files[:limit]

        if dry_run:
            for path, prov, root, sample_dt in files:
                logger.info(
                    "  [dry-run] %s (provider=%s, root=%s, date=%s)",
                    path.name,
                    prov,
                    root,
                    sample_dt,
                )
            logger.info("Dry run: %d files", len(files))
            return

        _prepare_individual(files, table_name, converter, table_def, table_output, data_root)


def _prepare_grouped(
    groups: dict[tuple[str, str, date], list[tuple[Path, str, str, date]]],
    table_name: str,
    converter: callable,
    table_def: object,
    output_dir: Path,
) -> None:
    """Prepare grouped datasets (1 file per symbol per date)."""
    written = 0
    errors = 0
    start = time.monotonic()
    total_rows = 0

    for (prov, root, sample_dt), group_files in groups.items():
        try:
            tables = []
            for path, _, _, _ in group_files:
                arrow_table = pq.read_table(path)
                converted = converter(arrow_table)
                tables.append(converted)

            combined = pa.concat_tables(tables, promote_options="permissive")
            normalized, diagnostics = normalize_table(
                combined, table_name, sample_date=sample_dt
            )
            normalized = _sort_table(normalized, table_def.sort_keys)

            # Use provider/root/date structure for output
            out_dir = output_dir / prov / str(sample_dt.year) / f"{sample_dt.month:02d}" / f"{sample_dt.day:02d}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / f"{root}.parquet"
            pq.write_table(normalized, out_path)
            written += 1
            total_rows += len(normalized)

            if written % 100 == 0:
                elapsed = time.monotonic() - start
                logger.info(
                    "Progress: %d written, %d errors, %d rows (%.1fs)",
                    written,
                    errors,
                    total_rows,
                    elapsed,
                )

        except Exception:
            logger.exception("Failed: %s/%s/%s", prov, root, sample_dt)
            errors += 1

    elapsed = time.monotonic() - start
    logger.info(
        "Complete: %d files written, %d errors, %d total rows (%.1fs)",
        written,
        errors,
        total_rows,
        elapsed,
    )


def _prepare_individual(
    files: list[tuple[Path, str, str, date]],
    table_name: str,
    converter: callable,
    table_def: object,
    output_dir: Path,
    data_root: Path,
) -> None:
    """Prepare datasets that are already 1 file per logical unit."""
    written = 0
    errors = 0
    start = time.monotonic()
    total_rows = 0

    for path, prov, root, sample_dt in files:
        try:
            arrow_table = pq.read_table(path)
            converted = converter(arrow_table)

            kwargs: dict[str, object] = {"sample_date": sample_dt}
            if table_name == "candles":
                symbol, interval = _candle_metadata(path, data_root, prov)
                kwargs["symbol"] = symbol
                kwargs["interval"] = interval

            normalized, diagnostics = normalize_table(converted, table_name, **kwargs)
            normalized = _sort_table(normalized, table_def.sort_keys)

            # Mirror original directory structure
            if table_name == "options":
                out_dir = output_dir / prov / str(sample_dt.year) / f"{sample_dt.month:02d}" / f"{sample_dt.day:02d}"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"{root}_samples_{sample_dt}.parquet"
            elif table_name == "candles":
                symbol, interval = _candle_metadata(path, data_root, prov)
                out_dir = output_dir / prov / (interval or "unknown")
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"{root}.parquet"
            elif table_name == "fundamentals":
                out_dir = output_dir / prov / str(sample_dt.year) / f"{sample_dt.month:02d}"
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"{sample_dt.day:02d}.parquet"
            else:
                out_dir = output_dir / prov
                out_dir.mkdir(parents=True, exist_ok=True)
                out_path = out_dir / f"{root}_{sample_dt}.parquet"

            pq.write_table(normalized, out_path)
            written += 1
            total_rows += len(normalized)

            if written % 100 == 0:
                elapsed = time.monotonic() - start
                logger.info(
                    "Progress: %d written, %d errors, %d rows (%.1fs)",
                    written,
                    errors,
                    total_rows,
                    elapsed,
                )

        except Exception:
            logger.exception("Failed: %s", path)
            errors += 1

    elapsed = time.monotonic() - start
    logger.info(
        "Complete: %d files written, %d errors, %d total rows (%.1fs)",
        written,
        errors,
        total_rows,
        elapsed,
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Prepare migration: normalize old parquets to local directory"
    )
    parser.add_argument("--table", required=True, choices=sorted(_CONVERTERS.keys()))
    parser.add_argument("-o", "--output", required=True, help="Output directory for normalized parquet")
    parser.add_argument("--provider", default=None, help="Filter to specific provider")
    parser.add_argument("--after", default=None, help="sample_date >= YYYY-MM-DD")
    parser.add_argument("--before", default=None, help="sample_date < YYYY-MM-DD")
    parser.add_argument("--limit", type=int, default=None, help="Max files/groups to process")
    parser.add_argument("--dry-run", action="store_true", help="List files without writing")
    parser.add_argument("--data-root", default=str(Path.home() / ".tickrake" / "data"))

    args = parser.parse_args()

    prepare(
        table_name=args.table,
        data_root=Path(args.data_root),
        output_dir=Path(args.output),
        provider=args.provider,
        after=date.fromisoformat(args.after) if args.after else None,
        before=date.fromisoformat(args.before) if args.before else None,
        limit=args.limit,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
