# Options Iceberg Partition Change + Backfill

## What changed

Added `option_root` (identity) as the first partition field in the options Iceberg table.

**New spec:** `option_root / sampled_at_month`

**Why:** Every dashboard and backtest query filters by root. Putting it first means Iceberg prunes to a single root's files before filtering by month.

**PR:** tractatus #38 (merged 2026-10-28)

---

## Steps to apply

### 1. Drop the options Iceberg table and reset ledger
```bash
tractatus catalog purge --table options
```
This drops the table from `tractatus-iceberg` and resets the ledger. Does **not** touch `tractatus-lakehouse`.

### 2. Recreate the table with the new partition spec
```bash
tractatus catalog init-local
```
Creates all tables from the schema registry. Options table is now partitioned by `option_root / sampled_at_month`.

### 3. Backfill from tractatus-lakehouse
```bash
python scripts/backfill_local_lakehouse_from_cache.py
```
Reads flat parquets from `tractatus-lakehouse/options/schwab/YYYY/MM/DD/` date-by-date and appends to the Iceberg table. Upserts the ledger. Idempotent — safe to rerun or resume with `--after`/`--before`.

---

## Notes

- `tractatus-lakehouse` is the source for backfill — do not drop or modify it
- The backfill script uses `_add_missing_columns` which is idempotent; lakehouse parquets already contain `option_root` and `provider`
- Once backfill completes, the `CacheReader` continues to serve the dashboard from `tractatus-lakehouse`; the Iceberg table is for future `LakehouseReader` backtest queries
