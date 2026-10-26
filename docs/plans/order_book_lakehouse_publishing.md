---
date_created: 2026-10-26
description: Plan for validating and operationalizing the order book publishing pipeline — from Tickrake staging MinIO to local Iceberg lakehouse and MinIO cache.
pull_request: ~
---

# Plan: Order Book Lakehouse Publishing

## Context

Most of the order book pipeline is already implemented. The goal is to audit what's there,
identify any gaps or bugs, validate against real Tickrake NDJSON output, and produce a
daily publish script analogous to `publish_daily_options.sh`.

`published_artifacts` already has 582 order_book/schwab rows (2026-09-18 → 2026-10-16),
so the pipeline has been exercised before. This is a validation and operationalization effort,
not a build-from-scratch one.

---

## What's Already Done

| Component | Status | Notes |
|---|---|---|
| Iceberg schema | ✅ | `schemas/order_book.py` — monthly partition on `sample_date`, sort on `[provider, received_at, book_time]` |
| Normalizer | ✅ | `normalize/order_book.py` — parses nested bids/asks JSON, converts timestamps, strips metadata |
| Normalizer tests | ✅ | `TestNormalizeOrderBook` in `test_catalog_normalize_tables.py` |
| Discovery path parser | ✅ | `_parse_symbol_ndjson` handles `order_book/{provider}/{YYYY}/{MM}/{DD}/{symbol}.ndjson` |
| Publisher grouping | ✅ | Groups by `(provider, symbol, sample_date)`, handles NDJSON input |
| Cache key generation | ✅ | `order_book/{provider}/{YYYY}/{MM}/{DD}/{symbol}.parquet` |
| CLI discover/publish | ✅ | `tractatus catalog discover/publish --table order_book` already works |
| Daily publish script | ❌ | Needs to be written |
| Snapshot expiry | ❌ | Not called after bulk publishes — snapshots will accumulate |

---

## Open Questions / Bugs to Verify

### 1. `sampled_at` vs `received_at` in `_TABLE_GRAIN`

In `publisher.py`, `_TABLE_GRAIN` maps `order_book` to `("sampled_at", ...)` as the date
column used for Iceberg scan filtering in `cache_artifacts`. But the order_book schema has
no `sampled_at` column — it has `received_at` and `book_time`.

**Check:** Confirm whether `cache_artifacts` actually uses this column name and whether it
causes a silent failure or a hard error when caching order_book artifacts. The correct
column is probably `received_at`.

### 2. Normalizer contract vs live Tickrake NDJSON

The normalizer expects:
- `received_at`, `symbol`, `service`, `book_time` as top-level string fields
- `bids_json`, `asks_json` as JSON-encoded strings
- `job_type`, `provider` as metadata fields (stripped before normalization)

**Check:** Pull a sample NDJSON file from the Tickrake staging MinIO bucket and confirm
the field names match exactly. Tickrake may have evolved since the normalizer was written.

### 3. `sample_date` column population

The normalizer adds `sample_date` as a date literal from the file path parameter. Confirm
the publisher passes this correctly for order_book files (it should — the discovery parser
extracts the date from the path).

---

## Implementation Steps

### Step 1: Verify `_TABLE_GRAIN` date column

Read `publisher.py` `_TABLE_GRAIN` dict. If it says `sampled_at` for `order_book`, change
it to `received_at`. Write a test or spot-check that `cache_artifacts --table order_book`
doesn't error.

### Step 2: Validate normalizer against live data

Pull a sample file from tickrake-staging MinIO:

```bash
# List available order_book files
mc ls local/tickrake-staging/order_book/schwab/ --recursive | head -20

# Download one sample
mc cp local/tickrake-staging/order_book/schwab/YYYY/MM/DD/SYMBOL.ndjson /tmp/sample_ob.ndjson
```

Run the normalizer manually against it in a Python REPL:

```python
import pyarrow as pa
from tractatus.catalog.normalize.order_book import normalize_order_book

with open("/tmp/sample_ob.ndjson") as f:
    lines = [json.loads(l) for l in f]

table = pa.Table.from_pylist(lines)
result, diagnostics = normalize_order_book(table, provider="schwab", sample_date=date.today())
print(result.schema)
print(result.num_rows)
```

If field names don't match, update the normalizer (or add an alias layer) and add a fixture
test with the actual live field names.

### Step 3: End-to-end publish smoke test

Pick one recent date with known order_book data in tickrake-staging:

```bash
tractatus catalog discover --table order_book --after 2026-10-23 --before 2026-10-24
tractatus catalog status --table order_book
tractatus catalog publish --table order_book --after 2026-10-23 --before 2026-10-24 --cache
tractatus catalog status --table order_book
```

Verify `published_artifacts` gets new rows and the cache parquet is readable.

### Step 4: Write `scripts/publish_daily_order_book.sh`

Mirror `publish_daily_options.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

DATE="${1:-$(date +%Y-%m-%d)}"
NEXT_DATE=$(date -v+1d -j -f "%Y-%m-%d" "$DATE" +%Y-%m-%d 2>/dev/null \
  || date -d "$DATE + 1 day" +%Y-%m-%d)

echo "==> Discovering order_book for $DATE..."
uv run tractatus catalog discover \
  --table order_book \
  --after "$DATE" \
  --before "$NEXT_DATE"

echo "==> Publishing and caching order_book for $DATE..."
uv run tractatus catalog publish \
  --table order_book \
  --after "$DATE" \
  --before "$NEXT_DATE" \
  --cache

echo "==> Done: $DATE"
```

### Step 5: Snapshot expiry

After any bulk publish, call:

```python
table.maintenance.expire_snapshots().older_than(dt=datetime.now(UTC)).commit()
```

Check whether the publisher already does this for order_book or whether it needs to be
added to the publish script as an explicit `tractatus catalog` command.

---

## Done Criteria

- [ ] `_TABLE_GRAIN` date column for order_book confirmed correct (or fixed)
- [ ] Normalizer validated against a live Tickrake NDJSON sample with no field-name mismatches
- [ ] End-to-end discover → publish → cache succeeds for at least one date
- [ ] `published_artifacts` rows written with correct `row_count` and `cached=1`
- [ ] `scripts/publish_daily_order_book.sh` written and smoke-tested
- [ ] Snapshot expiry confirmed handled
