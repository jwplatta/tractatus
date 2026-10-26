# Local Iceberg Migration — Working Context

Last updated: 2026-10-24

---

## What We're Doing and Why

Stop publishing to AWS Glue + S3 Glacier IR during development. Stand up a fully local Iceberg
catalog using PostgreSQL + MinIO, validate everything works, then flip to AWS when confident.

---

## Current State (as of 2026-10-24)

### Infrastructure
- `ICEBERG_CATALOG_TYPE=sql` in `.env` — zero risk of touching AWS
- `tractatus catalog init-local` has been run — `tractatus-iceberg` bucket + all 6 Iceberg tables created in PostgreSQL
- `tickrake-historical` MinIO bucket deleted — data is fully covered by `tractatus-lakehouse`

### Data in tractatus-iceberg (local Iceberg warehouse)
- Options: all historical data from 2025-12-18 through 2026-10-23 has been appended
  - Historical (up to Oct 16): backfilled via `scripts/backfill_local_lakehouse_from_cache.py`
  - Oct 19–23: reset ledger to `discovered`, republished via CSV pipeline
  - Snapshots expired after backfill (was 11,331 → 1)
- **Gap: 2023-09-26 → 2025-10-10** — purchased (non-Schwab) dataset, SPXW only, one parquet per day
- **Gap: 2025-10-11 → 2025-12-17** — no data; this is a known collection gap between the purchased dataset ending and Schwab scraping resuming
- Other tables (level_one, candles, order_book, fundamentals): NOT YET published to local Iceberg

### Data in tractatus-lakehouse (cache bucket)
- Options cache parquets exist for 2025-12-18 → 2026-10-22 but with OLD schema (missing `provider`,
  `option_root`, `underlying_symbol` columns)
- Need to regenerate via `tractatus catalog cache` to get full-schema parquets

### SQLite ledger (~/.tractatus/catalog_ledger.db)
- `catalog_files`: source CSV grain, `symbol` column (was `root`, renamed in-place)
- `published_artifacts`: artifact grain, `symbol` column (was `root`, renamed in-place)
- `published_artifacts` may be partially populated from backfill script (upserts were running
  but silently failing — not fully diagnosed)

---

## What's Been Fixed in This Session (on main, not a PR)

- `root` → `symbol` rename throughout: `FileRecord`, `DiscoveredFile`, ledger SQL, publisher, cli, tests
- `_ROOT_TO_UNDERLYING` mapping corrected: SPX, NDX, RUT (both roots each), XSP→$XSP (was wrong $SPX),
  VIX/VIXW→$VIX, DJX/DJXW→$DJX; removed hallucinated NANOS entry
- Options sort keys: removed `provider` (constant per file), `sampled_at` first for snapshot queries
- `--cache` flag added to `catalog publish`
- `_TABLE_GRAIN` fixed: `root`→`option_root`/`symbol`, `sample_date` derived from `sampled_at`
- `_build_cache_key` fixed: uses `option_root` for options, `symbol` for others
- `cache_artifacts` rewritten to scan one day at a time (was loading entire table into memory)
- tqdm progress added to `cache_artifacts`
- `CATALOG_DATABASE_URL` renamed to `LEDGER_DATABASE_URL` throughout
- `scripts/backfill_local_lakehouse_from_cache.py` written and run — appends historical cache
  parquets to local Iceberg, upserts `published_artifacts`, expires snapshots at end
- PyIceberg snapshot expiry API confirmed: `table.maintenance.expire_snapshots().older_than(dt=datetime.now(UTC)).commit()`

---

## In Progress Right Now

Running:
```
tractatus catalog cache --table options --before 2026-10-17
```
This reads from `tractatus-iceberg` (local Iceberg), writes full-schema parquets to
`tractatus-lakehouse`, and upserts `published_artifacts`. Streams one day at a time with tqdm.

Oct 19–22 still needs to be cached separately (after `--before 2026-10-17` run completes):
```
tractatus catalog cache --table options --after 2026-10-19 --before 2026-10-23
```

---

## Next Steps (in order)

1. **Complete `catalog cache` runs** — historical + Oct 19–22 (see above)
2. **Validate** — spot check row counts per date in Iceberg, verify `option_root`/`underlying_symbol`
   correctness, confirm `published_artifacts` is fully populated
3. **Other tables** — publish level_one, candles, order_book, fundamentals to local Iceberg
4. **AWS transition (future)** — flip `ICEBERG_CATALOG_TYPE=glue`

---

## Key Decisions Made

- `symbol` in options schema = contract symbol (e.g. `SPXW 261023C00125000`) — NOT renamed
- `symbol` in all other tables = underlying ticker
- `option_root` = root ticker from filename/contract (e.g. `SPXW`), one Iceberg file per option_root per day
- `underlying_symbol` = actual underlying (e.g. `$SPX`), derived via `_ROOT_TO_UNDERLYING`
- Provider: sort key only (no partition — cardinality too low)
- Options sort keys: `["sampled_at", "expiration_date", "strike", "contract_type", "symbol"]`
- Local Iceberg: PostgreSQL `tractatus` database + `tractatus-iceberg` MinIO bucket
- Cache bucket: `tractatus-lakehouse` (unchanged)
- `catalog cache` streams one day at a time — never loads full table into memory

---

## Files to Know About

| File | Purpose |
|---|---|
| `src/tractatus/catalog/publisher.py` | Core publish + cache logic; `_iceberg_catalog()`, `_TABLE_GRAIN`, `cache_artifacts()` |
| `src/tractatus/catalog/config.py` | CatalogConfig — env vars, `LEDGER_DATABASE_URL`, `catalog_type` |
| `src/tractatus/catalog/ledger.py` | SQLite ledger — `catalog_files` + `published_artifacts` |
| `src/tractatus/catalog/discovery.py` | `DiscoveredFile` dataclass + MinIO key parsers |
| `src/tractatus/catalog/schemas/options.py` | Options Iceberg schema |
| `src/tractatus/catalog/normalize/options.py` | Options normalizer; `_ROOT_TO_UNDERLYING` mapping |
| `src/tractatus/catalog/normalize/__init__.py` | `normalize_table()` dispatcher |
| `src/tractatus/cli.py` | CLI commands including `catalog init-local`, `catalog cache` |
| `scripts/backfill_local_lakehouse_from_cache.py` | Backfill historical cache parquets → local Iceberg |
| `docs/plans/local_iceberg_and_schema_migration.md` | Full implementation plan |
