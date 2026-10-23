# Local Iceberg Migration — Working Context

Last updated: 2026-10-23 (PR #29 in progress, not yet merged)

---

## What We're Doing and Why

We decided to stop publishing to AWS Glue + S3 Glacier IR during development because:
- Every schema mistake costs real money (Glacier IR early deletion fees)
- The publisher had bugs that caused wasted S3 writes
- We need room to iterate on schemas without fear

**The plan:** stand up a fully local Iceberg catalog using PostgreSQL + MinIO, validate everything
works correctly, then flip to AWS Glue + S3 when confident.

---

## Current State of the Codebase (as of 2026-10-23)

### What's been merged and working
- **PR #24** — publisher groups CSVs by (provider, symbol, sample_date). One Iceberg file per ticker per day.
- **PR #25** — --limit applies to groups (tickers), not individual CSV files.
- **PR #26** — `catalog discover` separated from `catalog publish`.
- **PR #27** — `published_artifacts` table in SQLite ledger.
- **PR #28** — `tractatus catalog cache` command (broken — see Known Issues below).

### Data published to AWS lakehouse (Glue + S3)
- Options: Oct 19, 20, 21, 22 2026 — 10,437,596 rows, 123 tickers for Oct 19
- All other dates are in the MinIO `tractatus-lakehouse` cache as parquet files (up to Oct 16)
- **PAUSED** further AWS publishes until local Iceberg is validated

### MinIO buckets
- `tractatus-lakehouse` — hand-crafted parquet paths, cache bucket. Has options data up to Oct 16 + Oct 19-22.
- `tractatus-iceberg` — NEW bucket (`MINIO_ICEBERG_BUCKET` in .env). Local Iceberg warehouse. Iceberg owns layout.

### SQLite ledger (~/.tractatus/catalog_ledger.db)
- `catalog_files` — source CSV grain; columns: `path, table_name, sha256, size_bytes, provider, symbol, sample_date, ...`
- `published_artifacts` — artifact grain; columns: `artifact_key, table_name, provider, symbol, sample_date, ...`
- Both tables previously had `root` column — **renamed to `symbol` in-place via migration script (now deleted)**

---

## In Progress (PR #29 — feat/local-iceberg-catalog)

### Schema changes — ALL DONE
- All 6 Iceberg schemas have `provider` column added + `provider` as first sort key
- Options schema has `option_root` (field_id=44) and `underlying_symbol` (field_id=45) columns
- `symbol` in options = contract symbol (e.g. `SPXW 261023C00125000`) — **NOT renamed to contract_symbol**
- No partition on `provider` — cardinality too low (1-3 values)

### Normalizer changes — ALL DONE
- `normalize_options` parses `option_root` from `symbol` via regex `^([A-Z$]+)`
- `underlying_symbol` derived via `_ROOT_TO_UNDERLYING` mapping in normalize/options.py
- `_ROOT_TO_UNDERLYING`: `SPXW→$SPX`, `NDXP→$NDX`, `RUTW→$RUT`, `XSP→$SPX`, `NANOS→$SPX`
- All other normalizers accept `provider` param + write via `set_literal_column()`
- `normalize_table()` in `__init__.py` accepts `provider` param

### CatalogConfig changes — DONE
- `catalog_type: str` (default `"glue"`, set to `"sql"` for local)
- `iceberg_postgres_url: str | None` — `ICEBERG_POSTGRES_URL` env var
- `iceberg_minio_bucket: str | None` — `MINIO_ICEBERG_BUCKET` env var
- `cache_bucket` env var fixed: `MINIO_CACHE_BUCKET`

### Publisher changes — DONE
- `_iceberg_catalog()` returns SQL catalog (local) or Glue based on `catalog_type`
- SQL catalog connects to PostgreSQL + MinIO: `s3.endpoint`, `s3.path-style-access=true`
- `--cache` flag on `catalog publish` — only writes cache parquet when passed
- `normalize_table()` call passes `provider=provider`

### Ledger changes — DONE
- `root` → `symbol` rename in both `catalog_files` and `published_artifacts` tables
- Migration script ran, confirmed, deleted
- `FileRecord.root` → `FileRecord.symbol` in ledger.py
- `DiscoveredFile.root` → `DiscoveredFile.symbol` in discovery.py
- All callers updated: publisher.py, cli.py, tests

### CLI changes — DONE
- `catalog init-local` command added — creates `tractatus-iceberg` bucket + ensures all 6 Iceberg tables
- `catalog publish --cache` flag added

### Dependencies — DONE
- `pyiceberg[sql-postgres]` added to pyproject.toml

### .env additions — DONE
- `ICEBERG_CATALOG_TYPE=sql`
- `ICEBERG_POSTGRES_URL=postgresql+psycopg2://tractatus_user:tractatus@localhost:5432/tractatus`
- `MINIO_ICEBERG_BUCKET=tractatus-iceberg`

---

## Known Issues / Bugs Outstanding

### catalog cache command (PR #28) is broken
`_TABLE_GRAIN` in `publisher.py` references column names that do not match the actual Iceberg schemas.
This will be fixed after PR #29 merges (schema columns are now `symbol`, `sampled_at`/`sample_date`,
`provider`, `option_root`, `underlying_symbol`).

**TODO after PR #29 merges:**
- Fix `_TABLE_GRAIN` to use correct column names
- Re-test `tractatus catalog cache`

### `option_root → underlying_symbol` mapping is incomplete
Currently only covers index options: `SPXW→$SPX`, `NDXP→$NDX`, `RUTW→$RUT`, `XSP→$SPX`, `NANOS→$SPX`.
Equity options fall back to using `option_root` directly as the underlying (correct for equities).
**TODO:** audit all unique option roots in the dataset to validate no gaps.

---

## Next Steps (in order)

1. **Commit PR #29** — all `make check` tests pass (133/133)
2. **Run `tractatus catalog init-local`** — create `tractatus-iceberg` bucket + Iceberg tables in Postgres
3. **Backfill Part A (Oct 19-22)** — reset ledger to `discovered`, re-publish via CSV pipeline with `--cache`
4. **Backfill Part B (historical, up to Oct 16)** — write `scripts/backfill_local_lakehouse_from_cache.py`:
   read MinIO cache parquets, add missing columns (`provider`, `option_root`, `underlying_symbol`),
   publish to local Iceberg
5. **Fix `_TABLE_GRAIN`** — rewrite to use correct schema column names
6. **Validate** — spot checks, quality checks, `catalog cache` end-to-end
7. **AWS transition (future)** — flip `catalog_type=glue`

---

## Key Decisions Made

- `symbol` in options = contract symbol (e.g. `SPXW 261023C00125000`) — NOT renamed
- `symbol` in all other tables = underlying ticker — consistent
- Provider: sort key only (no partition — cardinality too low)
- Local Iceberg: PostgreSQL `tractatus` database + `tractatus-iceberg` MinIO bucket
- Cache bucket: `tractatus-lakehouse` (unchanged)
- AWS publishes: PAUSED until local validation complete
- `catalog discover` vs `catalog publish`: separated — discovery slow (~6.5 min)
- Backfill source for historical: MinIO cache parquets (not re-reading tickrake-staging CSVs)

---

## Files to Know About

| File | Purpose |
|---|---|
| `src/tractatus/catalog/publisher.py` | Core publish + cache logic; `_iceberg_catalog()` |
| `src/tractatus/catalog/config.py` | CatalogConfig — env vars, catalog_type |
| `src/tractatus/catalog/ledger.py` | SQLite ledger — catalog_files + published_artifacts |
| `src/tractatus/catalog/discovery.py` | `DiscoveredFile` dataclass + MinIO key parsers |
| `src/tractatus/catalog/schemas/options.py` | Options Iceberg schema |
| `src/tractatus/catalog/normalize/options.py` | Options normalizer; `_ROOT_TO_UNDERLYING` mapping |
| `src/tractatus/catalog/normalize/__init__.py` | `normalize_table()` dispatcher |
| `src/tractatus/cli.py` | CLI commands including `catalog init-local` |
| `docs/plans/local_iceberg_and_schema_migration.md` | Full implementation plan |
