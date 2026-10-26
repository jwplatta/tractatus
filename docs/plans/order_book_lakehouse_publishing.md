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

## Resolved Questions

### 1. `sampled_at` vs `received_at` — intentionally different names, `_TABLE_GRAIN` was buggy

The ingestion-time column is named differently across table types by design:

- **`sampled_at`** (options, fundamentals, candles) — when the *sample* was taken: Schwab's
  feed time or batch fetch time. The data was pulled/fetched at this moment.
- **`received_at`** (order_book, level_one) — when Tickrake's streaming collector *received*
  the event off the Schwab WebSocket. Stamped by `Time.now.utc` in Tickrake — it is a
  Tickrake concept, not a Schwab field. The Schwab feed does not provide this timestamp.

Both answer "when did our system have this data?" for backtest alignment, named accurately
for their context. A backtest joining across tables uses whichever column is present.

`_TABLE_GRAIN` in `publisher.py` incorrectly mapped `order_book` to `"sampled_at"`.
**Fixed:** changed to `"received_at"` and updated `cache_artifacts` to derive `sample_date`
from `date_col` (the `_TABLE_GRAIN` date column) rather than hardcoding `"sampled_at"`.

Decision: keep `received_at` as the canonical column name in the Iceberg schema for
order_book and level_one. It is more accurate than `sampled_at` for streaming data where
Tickrake stamps receipt time itself. No schema changes needed.

### 2. Normalizer contract matches live Tickrake output ✅

Validated against a live example JSON from `tmp/example.json` and confirmed against
`schwab_rb/lib/schwab_rb/stream/fields.rb` (BookBid, BookAsk, BookExchange constants):

- Top-level fields match: `received_at`, `symbol`, `service`, `book_time`, `bids_json`, `asks_json`, `job_type`, `provider`
- Numeric key mapping in nested arrays is correct:
  - Bid/ask level: `"0"` = price, `"1"` = total_volume, `"2"` = num_bids/asks, `"3"` = venues array
  - Venue: `"0"` = exchange name, `"1"` = volume, `"2"` = sequence_id
- Tickrake writes raw Schwab payload directly to JSON without remapping keys — normalizer handles this correctly

### 3. `sample_date` population ✅

Discovery parser extracts date from `order_book/{provider}/{YYYY}/{MM}/{DD}/{symbol}.ndjson`
path and passes it to the normalizer. Works correctly.

---

## Implementation Steps

### Step 1: `_TABLE_GRAIN` fix ✅ DONE

Fixed in `publisher.py` — `order_book` now maps to `"received_at"`. Also fixed
`cache_artifacts` to use `date_col` from `_TABLE_GRAIN` instead of hardcoding `"sampled_at"`
so all tables derive `sample_date` correctly.

### Step 2: Normalizer validated ✅ DONE

Confirmed against live example and `schwab_rb` field constants. No changes needed.

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
