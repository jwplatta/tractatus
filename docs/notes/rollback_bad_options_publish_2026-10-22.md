# Rollback: Bad Options Publish (2026-10-22)

## What Happened

The `tractatus catalog publish` command was run for options samples on 2026-10-19 before the publisher had grouping logic. At the time, the publisher appended each individual CSV snapshot as a separate Iceberg file, producing 34,732 tiny parquet files (~25KB each) instead of one file per ticker per day. This also triggered two other bugs:

- Columns like `open`/`high`/`low`/`close` that are entirely null in a CSV were inferred as `pa.null()` type by pyarrow, which the schema validator rejected
- `sample_quality` and `quality_flags` were being added twice (once by `align_to_schema`, once by `validate_options`), causing a duplicate column error

All 34,732 files failed on the first run. After fixes were applied they committed successfully but produced the wrong file layout.

## Rollback Steps

### 1. Delete Oct 19 rows from Iceberg (logical delete)

```python
from pyiceberg.catalog import load_catalog

cat = load_catalog('glue', **{'type':'glue','region':'us-east-1','profile':'tickrake'})
t = cat.load_table('tractatus.options')
t.delete("sampled_at >= '2026-10-19T00:00:00+00:00' AND sampled_at < '2026-10-20T00:00:00+00:00'")
```

This creates a delete snapshot but does not remove the underlying S3 files.

### 2. Physically delete the parquet files (VACUUM via Athena)

PyIceberg does not implement `expire_snapshots` yet. Use AWS Athena instead:

1. Go to AWS Console → Athena → Query editor
2. Set a query result location (we used `s3://tractatus-lakehouse/athena-results/`)
3. Run:

```sql
VACUUM tractatus.options;
```

This expired the old snapshots and physically deleted the 34,732 orphaned parquet files from S3. Took ~10 minutes. Confirmed success by checking the snapshot summary returned to 707 data files.

### 3. Reset the ledger

```sql
UPDATE catalog_files
SET status = 'discovered', error = NULL, committed_at = NULL
WHERE sample_date = '2026-10-19' AND table_name = 'options';
```

### 4. Re-publish with the fixed publisher

```bash
pip install -e .
tractatus catalog publish --table options --after 2026-10-19 --before 2026-10-20
```

## Fix Applied

See PR #24. The publisher now groups all CSVs for `(provider, root, sample_date)` into a single Arrow table before normalizing, validating, and appending to Iceberg — one parquet file per ticker per day.
