# Lakehouse Migration — 2026-10-18

## Overview

One-time migration of all historical tickrake parquet data from `~/.tickrake/data/` into the Iceberg lakehouse (S3) and MinIO cache. This follows the canonical parquet specifications from `tickrake/docs/notes/lakehouse_parquet_specifications.md`.

## Two-phase approach

### Phase 1: Prepare (`scripts/migrate_prepare.py`)

Reads old-format parquets, converts columns, normalizes, sorts, and writes canonical parquet files to a local staging directory (`/tmp/migrate/`). No S3 or ledger interaction.

Key transformations:
- **level_one, order_book**: Multiple intraday snapshot files per (symbol, date) consolidated into 1 file per symbol per date
- **level_one**: `received_at_ms`/`quote_time_ms`/`trade_time_ms` (int64 epoch-ms) → UTC ISO strings → `timestamp[us, UTC]`
- **order_book**: `received_at_ms`/`book_time_ms` same conversion; `bids_json`/`asks_json` parsed into nested Arrow structs
- **options**: Missing columns added as null (old parquets have 26-31 of 41 expected cols); `non_standard` string→bool cast for recent files
- **candles**: Already compatible
- **fundamentals**: `fetched_at` → `sampled_at`

### Phase 2: Publish (`scripts/migrate_publish.py`)

Reads prepared parquets from local staging and appends to Iceberg tables via `iceberg_table.append()`. Batches files together (default 50) to reduce Glue metadata overhead.

### Phase 3: Cache (`scripts/migrate_cache.py`)

Uploads prepared parquets from local staging to MinIO cache bucket (`tractatus-lakehouse` on `localhost:9000`).

## Data volumes

| Dataset | Provider | Source files | Prepared files | Rows |
|---------|----------|-------------|---------------|------|
| candles | schwab | 139 | 50 | 1.16M |
| candles | ibkr-paper | 6,972 | 6,972 | 4.91M |
| fundamentals | schwab | 1 | 1 | 586 |
| level_one_equities | schwab | 7,649 | 693 | 12.08M |
| level_one_futures | schwab | 3,438 | 92 | 3.46M |
| order_book | schwab | 10,611 | 582 | 15.52M |
| options | schwab | 10,269 | 10,269 | 613.7M |
| options | massive | 513 | 513 | 45.3M |

## Bugs found during migration

1. **`iso_string_to_timestamp`** (`common.py`): `pc.strptime` couldn't parse fractional seconds. Fixed by using `pc.cast(col, timestamp[us, UTC])`.
2. **`_unpack_extra_json`** (`level_one.py`): JSON int/float values for string-typed columns (ask_id, bid_id, last_id). Fixed by adding string coercion.
3. **`convert_options`** (`migrate_prepare.py`): Old parquets have 26-31 columns, normalizer expects 41. Fixed by adding missing columns as null.
4. **`non_standard` column**: Recent options parquets (2026-10-14+) have `non_standard` as string instead of bool. Fixed with `pc.equal(col, "true")` cast.

## Parallel publish doesn't work

Running multiple `migrate_publish.py` processes against the same Glue catalog causes deadlocks. Must publish one table at a time.

## Notes

- Iceberg table properties (compression, row-group-limit) are set at table creation time via `ensure_table()`. PyIceberg enforces these when writing.
- Pre-sorting is done in the prepare step since PyIceberg doesn't physically sort on append.
- The prepared files in `/tmp/migrate/` are NOT deleted after publish — they serve as the source for the MinIO cache upload.
- The postgres ledger was NOT updated during this migration (publish script doesn't write to it). This is acceptable since the ledger tracks ongoing publisher state, not historical migration.
