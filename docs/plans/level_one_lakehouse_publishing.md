---
date_created: 2026-10-27
description: Plan for converting level_one raw data to NDJSON, fixing pipeline bugs, and publishing equities and futures to the local Iceberg lakehouse.
pull_request: ~
---

# Plan: Level One Lakehouse Publishing

## Context

Level_one (equities + futures) data needs to be published to the local Iceberg lakehouse the same way order_book and options were. The raw data lives in `~/.tickrake/data/level_one/schwab/YYYY/MM/DD/` in two formats:

- **Sep 18 – Oct 16**: Multiple time-stamped parquet files per symbol per day (`SYMBOL_HHMMSSz.parquet`), with `*_ms` integer timestamps. Contains equities (IWM, QQQ, SPY) AND futures (`^ES` confirmed in at least Oct 01; check earlier dates).
- **Oct 19 onwards**: One NDJSON file per symbol per day (`SYMBOL.ndjson`), already in the correct normalizer format with ISO string timestamps. Both equities and futures.

The existing normalizer and Iceberg schemas are already implemented. The work is: convert the old parquet data to NDJSON, fix two `_TABLE_GRAIN` bugs, and run the publish pipeline.

## Equities/Futures Split

Keep it. The two tables have genuinely different schemas (equities: 63 cols, futures: 35 cols), different Schwab API field numbering, and different semantic fields. The `^` prefix discriminator is reliable and already implemented in discovery.

## Files to Modify

- `src/tractatus/catalog/publisher.py` — fix `_TABLE_GRAIN`
- `scripts/convert_level_one_parquet_to_ndjson.py` — new conversion script

## Step 1: Fix `_TABLE_GRAIN` (`publisher.py`)

Both level_one entries map to `"sampled_at"` but the schema uses `received_at`. Fix:

```python
"level_one_equities": ("received_at", ("provider", "symbol", "sample_date")),
"level_one_futures":  ("received_at", ("provider", "symbol", "sample_date")),
```

## Step 2: Conversion Script (`scripts/convert_level_one_parquet_to_ndjson.py`)

Converts Sep 18 – Oct 16 parquet files to NDJSON matching the normalizer input contract. Follow the pattern in `scripts/convert_order_book_parquet_to_ndjson.py`.

**Raw parquet schema (13 columns):**
`received_at_ms` (int64 ms), `symbol`, `service`, `quote_time_ms` (int64 ms), `trade_time_ms` (int64 ms), `bid`, `ask`, `last`, `bid_size`, `ask_size`, `volume`, `mark`, `extra_json`

**NDJSON output format per line:**
```json
{
  "job_type": "level_one",
  "provider": "schwab",
  "symbol": "SPY",
  "service": "LEVELONE_EQUITIES",
  "received_at": "2026-09-18T14:30:07Z",
  "quote_time": "2026-09-18T14:30:07Z",
  "trade_time": "2026-09-18T14:30:05Z",
  "bid": 499.50,
  "ask": 499.55,
  "last": 499.52,
  "mark": 499.53,
  "bid_size": 100,
  "ask_size": 200,
  "volume": 1234567,
  "extra_json": "{...}"
}
```

**Logic:**
- Group `SYMBOL_HHMMSSz.parquet` files by symbol per day directory
- Convert `received_at_ms`, `quote_time_ms`, `trade_time_ms` (int64 ms) → ISO8601 UTC strings
- Rename to `received_at`, `quote_time`, `trade_time`
- Add `job_type: "level_one"` and `provider: "schwab"`
- Write all rows for the symbol to `SYMBOL.ndjson` in the same directory
- Skip if `SYMBOL.ndjson` already exists (idempotent)
- Support `--dry-run`, `--date YYYY-MM-DD`, `--data-root` flags
- Do not delete parquet files

**Oct 19+ NDJSON files are already correct** — same field names and ISO string format. No changes needed to those files.

## Step 3: Validations

No new validations. The normalizer already validates the input schema contract and raises `ValueError` on failure. Level_one has no options-style quality issues (no strikes, expirations). Add validations only if data quality problems surface during the smoke test.

## Step 4: Smoke Test

After converting parquet → NDJSON and staging to `tickrake-staging` MinIO:

```bash
# Equities
uv run tractatus catalog discover --table level_one_equities --after 2026-09-18 --before 2026-10-27 --verbose
uv run tractatus catalog publish --table level_one_equities --after 2026-09-18 --before 2026-10-27 --cache --verbose

# Futures (full range — futures parquet data exists from at least Oct 01)
uv run tractatus catalog discover --table level_one_futures --after 2026-09-18 --before 2026-10-27 --verbose
uv run tractatus catalog publish --table level_one_futures --after 2026-09-18 --before 2026-10-27 --cache --verbose
```

Spot-check a cached parquet: `received_at` as `timestamp[us, tz=UTC]`, `bid/ask/last/mark` as `double`, `extra_json` fields unpacked correctly into individual columns.

## Done Criteria

- [ ] `_TABLE_GRAIN` fixed for `level_one_equities` and `level_one_futures`
- [ ] Conversion script dry-run shows correct grouping and timestamp conversion
- [ ] NDJSON files written to `~/.tickrake/data/level_one/schwab/` for Sep 18 – Oct 16
- [ ] End-to-end discover → publish → cache succeeds for equities
- [ ] End-to-end discover → publish → cache succeeds for futures
- [ ] Cache parquet schema verified — correct types throughout
- [ ] `make check` passes
