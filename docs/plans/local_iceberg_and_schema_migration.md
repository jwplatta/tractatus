# Plan: Local Iceberg Catalog + Schema Migration

## Context

Publishing to AWS Glue + S3 Glacier IR during development is costly and slow to iterate on.
The goal is to stand up a fully local Iceberg catalog (PostgreSQL + MinIO) for development
and validation, then transition to AWS when confident everything is working correctly.

At the same time, several schema issues need fixing:
- `provider` is never written to any Iceberg table — needed for artifact grouping and cache key construction
- Options `symbol` field is the contract symbol — need to rename it and add `option_root` and `underlying_symbol`

---

## Architecture

### Local (development)
- **Catalog:** PyIceberg SQL catalog backed by the existing `tractatus` PostgreSQL database
- **Warehouse:** `tractatus-iceberg` MinIO bucket (`MINIO_ICEBERG_BUCKET` — already in .env) — Iceberg owns the layout
- **Cache:** `tractatus-lakehouse` MinIO bucket — hand-crafted parquet paths, unchanged

### Cloud (production, when ready)
- **Catalog:** AWS Glue
- **Warehouse:** AWS S3 bucket
- **Cache:** `tractatus-lakehouse` MinIO bucket (same as local cache)

### Transition
Flip `CatalogConfig` from `catalog_type=sql` to `catalog_type=glue` and point warehouse at AWS S3.
Cache bucket may require data movement depending on schema/path changes made during local phase.

### catalog cache command
Moves data from the Iceberg warehouse bucket (`tractatus-iceberg`) to the cache bucket
(`tractatus-lakehouse`) — reads parquet files from the Iceberg layout, writes them at
hand-crafted paths in the cache bucket. No Iceberg scan needed; operates directly on MinIO objects.

---

## Schema Changes

### All tables: add `provider` column
- `provider: StringType, required=False`
- Populated by the publisher from the ledger record (already known at publish time)
- Written as a literal column on the Arrow table before normalize
- **Partition spec:** no partition on `provider` — cardinality is too low (1-3 values) and growth is uncertain; most scans would touch all partitions anyway
- **Sort keys:** add `provider` as first sort key on all tables so filtering within a file is cheap

### Options table: symbol fields
- `symbol` (field_id=2) keeps its name — it is the contract symbol (e.g. `SPXW 261023C00125000`). NOT renamed.
- Add `option_root: StringType` — the root ticker (e.g. `SPXW`)
- Add `underlying_symbol: StringType` — the actual underlying (e.g. `$SPX`)

tickrake will be updated to write `option_root` and `underlying_symbol` directly in new CSV samples.
For existing data where these fields are absent from the CSV, the normalizer parses them:
- `option_root` — parsed from `symbol` (prefix before the first space or date digits)
- `underlying_symbol` — looked up via a hardcoded mapping (e.g. `SPXW` → `$SPX`, `NDXP` → `$NDX`,
  `RUTW` → `$RUT`); for equity options where `option_root` == underlying, use `option_root` directly

**TODO:** audit all unique option roots in the dataset and complete the `option_root → underlying_symbol`
mapping before implementing.

### Other tables
- `symbol` already refers to the underlying ticker in level_one, order_book, candles, fundamentals — no rename needed
- Just add `provider` with partition + sort key as above

---

## Implementation Steps

### Step 1: CatalogConfig changes ✅ (MINIO_ICEBERG_BUCKET already in .env)
- Add `catalog_type: str` field (`sql` | `glue`, default `glue`)
- Add `iceberg_postgres_url: str | None` for SQL catalog connection
- `_iceberg_catalog()` in publisher returns SQL catalog or Glue catalog based on `catalog_type`
- SQL catalog URI: `postgresql+psycopg2://tractatus_user:<password>@localhost:5432/tractatus`
- Add `pyiceberg[sql-postgres]` / `sqlalchemy` + `psycopg2` to dependencies if not present

### Step 2: Schema changes
- Add `provider` field + partition + sort key to all 6 table schemas
- Options: keep `symbol` as contract symbol (no rename), add `option_root`, `underlying_symbol`
- Update `normalize_options`: accept `option_root` and `underlying_symbol` from tickrake if present;
  otherwise parse from `symbol` using root extraction logic + underlying mapping
- Update `normalize_table()` signature to accept `provider` param, write as literal column

### Step 3: Publisher changes
- Pass `provider` into `normalize_table()` for all tables
- Tables are now created against local SQL catalog — no Glue/S3 writes

### Step 4: Backfill local lakehouse (two parts)

**Part A — Recent data (Oct 19–22, in tickrake-staging CSVs):**
- Reset ledger records to `discovered` for these dates
- Run `tractatus catalog publish` — goes through full normalize+validate pipeline
- These will have all new columns populated correctly

**Part B — Historical data (up to Oct 16, in `tractatus-lakehouse` MinIO cache as parquet):**
- Read each parquet from `tractatus-lakehouse`
- Add missing columns: `provider` (from path), `option_root` (parsed from `symbol`),
  `underlying_symbol` (from mapping)
- Validate against current schema
- Publish to local Iceberg (`tractatus-iceberg`)
- Upsert `published_artifacts` for each file
- Write a dedicated backfill script for this: `scripts/backfill_local_lakehouse_from_cache.py`

### Step 5: Validate
- Spot check row counts, provider values, contract_symbol/option_root/underlying_symbol correctness
- Run options quality checks
- Verify `catalog cache` (iceberg → cache bucket copy) works end-to-end

### Step 6: Cleanup
- Audit and remove one-off migration scripts (`scripts/migrate_options_add_quality_columns.py`, others)
- Fix `catalog cache` `_TABLE_GRAIN` to use correct schema column names

### Step 7: Transition to AWS (future, when confident)
- Set `catalog_type=glue` in production env
- Point warehouse at AWS S3 bucket
- Run full publish against Glue + AWS S3

---

## Open Questions / TODO

- Complete `option_root → underlying_symbol` mapping (audit unique roots in dataset first)
- Confirmed tickrake CSV field name for contract symbol is `symbol` — NOT renamed in schema ✅
- Migration scripts to audit for removal: `scripts/migrate_options_add_quality_columns.py` + others

---

## Backlog Items
- `docs/backlog/lakehouse_coverage_visualization_app.md`
- `docs/backlog/update_published_files_on_reader_cache_miss.md`
