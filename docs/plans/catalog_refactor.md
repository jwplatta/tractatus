# Tractatus Catalog Refactor: Normalization, Migration & Cache Management

## Context

Tickrake currently writes intermediate parquet files that Tractatus normalizes and publishes to an S3 Iceberg lakehouse. Both systems do parquet generation — wasteful. Going forward, tickrake will only write raw CSV/NDJSON to a `tickrake-staging` MinIO bucket. Tractatus becomes the sole owner of normalization, parquet generation (with proper sort order, row groups, compression), publishing to S3 Iceberg, managing a local MinIO cache, and cache eviction via CLI.

Reference specs: `/Users/jplatta/repos/tickrake/docs/notes/lakehouse_parquet_specifications.md`
Raw contracts: `/Users/jplatta/repos/tickrake/docs/schemas/raw_data_contracts.md`

---

## Phase 1: FileStore — Replace with Raw-Aware Interface

**Goal**: FileStore discovers and reads CSV/NDJSON/parquet files.

**Modify**: `src/tractatus/catalog/file_store.py`

Replace the parquet-only protocol with:
```python
class FileStore(Protocol):
    def list_files(
        self, prefix: str, extensions: list[str], providers: list[str] | None = None
    ) -> list[tuple[str, int]]: ...
    def read_raw(self, key: str) -> bytes: ...
    def read_parquet(self, key: str) -> pa.Table: ...
    def sha256(self, key: str) -> str: ...
```

- Remove `list_parquet` — replace all call sites with `list_files(..., extensions=[".parquet"])` or broader extensions
- `read_raw` returns bytes for CSV/NDJSON
- `read_parquet` stays for parquet files (already Arrow-native)
- Update both `LocalFileStore` and `S3FileStore`

---

## Phase 2: Discovery — New File Patterns

**Goal**: Discovery finds raw staging files.

**Modify**: `src/tractatus/catalog/discovery.py`

Update `DiscoveredFile`:
```python
@dataclass
class DiscoveredFile:
    relative_path: str
    provider: str
    root: str  # symbol or option root
    sample_date: date
    sha256: str
    size_bytes: int
    file_format: str  # "parquet", "csv", "ndjson"
    metadata: dict[str, str]  # e.g. {"sampled_at": "2026-10-18T14:30:00Z"} for options CSV
```

New/updated parsers for raw staging file naming:
- **Options CSV**: `{ROOT}_exp{EXP}_{DATE}_{TIME}.csv` → extract root, sampled_at from filename
- **Level-one NDJSON**: `{SYMBOL}.ndjson` (1 file per symbol per date, no timestamp in name)
- **Order-book NDJSON**: `{SYMBOL}.ndjson` (same pattern)
- **Candles CSV**: `{SYMBOL}.csv`

Existing parquet parsers remain for migration. `discover_files` calls `store.list_files(prefix, extensions=...)`.

---

## Phase 3: Normalizers — Raw Input + Parquet Physical Specs

**Goal**: Normalizers accept raw CSV/NDJSON. Output parquet follows canonical specs (sort order, row groups, compression).

### 3a. `src/tractatus/catalog/normalize/common.py`

Add:
- `csv_to_arrow(data: bytes, schema_hint: dict[str, pa.DataType] | None = None) -> pa.Table`
- `ndjson_to_arrow(data: bytes) -> pa.Table`
- `write_canonical_parquet(table: pa.Table, dest: Path | io.BytesIO, sort_keys: list[str], row_group_size: int, compression: str, compression_level: int) -> None` — sorts, sets row group size, writes with ZSTD

### 3b. `src/tractatus/catalog/normalize/options.py`

- Replace `_EXPECTED_INPUT` (3 cols) with full 40-column validation matching the CSV contract
- Tickrake will add `sampled_at` to the CSV; expect it as a column
- Normalizer expects UTC timestamp strings for `sampled_at`, `quote_at`, `trade_at` — cast directly to `timestamp[us, UTC]`
- Add `normalize_options_csv(data: bytes) -> tuple[pa.Table, list[str]]`
- Parquet write spec: sort by `(expiration_date, sampled_at, strike, contract_type, symbol)`, row_group=50000, ZSTD level 3

### 3c. `src/tractatus/catalog/normalize/level_one.py`

- Update `_EXPECTED_INPUT` to expect UTC timestamps directly:
  - `received_at`: `pa.string()` (UTC ISO string, not epoch-ms)
  - `quote_time`: `pa.string()` (renamed from `quote_time_ms`)
  - `trade_time`: `pa.string()` (renamed from `trade_time_ms`)
- Remove the `epoch_ms_to_timestamp` conversion for these fields — just cast string → `timestamp[us, UTC]`
- Strip `job_type` and `provider` fields from NDJSON input (metadata, not data)
- Add `normalize_level_one_equities_ndjson(data: bytes, sample_date) -> tuple[pa.Table, list[str]]`
- Add `normalize_level_one_futures_ndjson(data: bytes, sample_date) -> tuple[pa.Table, list[str]]`
- Parquet write spec: sort by `(received_at, quote_time, trade_time)`, row_group=100000-200000, ZSTD level 3

### 3d. `src/tractatus/catalog/normalize/order_book.py`

- Same timestamp updates: expect `received_at` and `book_time` as UTC strings, cast directly
- Add `normalize_order_book_ndjson(data: bytes, sample_date) -> tuple[pa.Table, list[str]]`
- Parquet write spec: sort by `(received_at, book_time)`, row_group=25000-50000, ZSTD level 5

### 3e. `src/tractatus/catalog/normalize/candles.py`

- Add `normalize_candles_csv(data: bytes, symbol: str, interval: str) -> tuple[pa.Table, list[str]]`
- Parquet write spec: sort by `(datetime)`, row_group=50000, ZSTD level 3

### 3f. `src/tractatus/catalog/normalize/__init__.py`

Add `normalize_raw(data: bytes, file_format: str, table_name: str, **kwargs) -> tuple[pa.Table, list[str]]` dispatcher.

---

## Phase 4: Ledger — Redesign Schema

**Goal**: Ledger tracks lakehouse publication AND local cache state. Fresh schema, no migration.

**Modify**: `src/tractatus/catalog/ledger.py`

New schema (drop and recreate):
```sql
CREATE TABLE catalog_files (
    path          TEXT NOT NULL,
    table_name    TEXT NOT NULL,
    sha256        TEXT NOT NULL,
    size_bytes    BIGINT NOT NULL,
    provider      TEXT NOT NULL,
    root          TEXT NOT NULL,
    sample_date   TEXT NOT NULL,
    file_format   TEXT NOT NULL,          -- "parquet", "csv", "ndjson"
    -- Lakehouse state
    s3_key        TEXT,
    status        TEXT NOT NULL DEFAULT 'discovered',  -- discovered, committed, error
    error         TEXT,
    discovered_at TEXT NOT NULL,
    committed_at  TEXT,
    -- Cache state
    cache_key     TEXT,                   -- MinIO object path in local cache
    cached_at     TEXT,
    last_accessed_at TEXT,
    -- Bookkeeping
    updated_at    TEXT NOT NULL,
    PRIMARY KEY (path, table_name)
);
```

Remove the `uploaded` status — simplify to `discovered → committed | error`.

Update `FileRecord` dataclass to match.

New methods:
- `mark_cached(path, table_name, cache_key)` — set cache_key + cached_at
- `mark_cache_accessed(path, table_name)` — update last_accessed_at
- `clear_cache_entry(path, table_name)` — null out cache fields
- `cached_files(table_name=None, older_than_days=None, provider=None, root=None) -> list[FileRecord]`
- `cache_summary(table_name=None) -> dict`

Remove `mark_uploaded`, `pending_upload`, `pending_commit` — replaced by simpler `pending(table_name, ...) -> list[FileRecord]` (status='discovered').

---

## Phase 5: Publisher — Dual Publish + Cache CLI

**Goal**: Publisher writes to Iceberg AND local MinIO cache. CLI gains cache commands.

### 5a. `src/tractatus/catalog/config.py`

Add to `CatalogConfig`:
- `cache_bucket: str | None` — local MinIO bucket name (same name as S3 lakehouse bucket)
- Reuse existing `minio_endpoint`/`minio_access_key`/`minio_secret_key`

### 5b. `src/tractatus/catalog/publisher.py`

Rewrite `_append` flow:
1. Check `DiscoveredFile.file_format`
2. If csv/ndjson → `store.read_raw(key)` + `normalize_raw(data, format, table, ...)`
3. If parquet → `store.read_parquet(key)` + `normalize_table(table, ...)`
4. Write canonical parquet using `write_canonical_parquet()` with per-table specs
5. `iceberg_table.append(normalized)`
6. Upload canonical parquet to cache MinIO bucket
7. `ledger.mark_committed(...)` + `ledger.mark_cached(...)`

### 5c. `src/tractatus/cli.py`

Add `cache` subcommand group:
- `catalog cache status [--table]` — cached file count, total bytes
- `catalog cache evict --older-than 30d [--table] [--provider] [--symbol] [--yes]` — delete from MinIO + clear ledger cache entries
- `catalog cache clear --table [--yes]` — full wipe for a table

---

## Phase 6: Migration Scripts

**Goal**: One-off scripts to migrate existing `~/.tickrake/data/` parquets into lakehouse + cache.

**Create** in `scripts/`:
- `scripts/migrate_options.py`
- `scripts/migrate_level_one.py`
- `scripts/migrate_order_book.py`
- `scripts/migrate_candles.py`

**Pattern** (each script):
1. `LocalFileStore(Path.home() / ".tickrake" / "data")` — existing parquets
2. `discover_files(store, table_name=...)` — parquet discovery
3. For each file not already committed in ledger:
   - `store.read_parquet(key)` → Arrow table
   - `normalize_table(table, table_name, ...)` → normalized
   - Write canonical parquet with proper sort/row-groups/compression via `write_canonical_parquet()`
   - `iceberg_table.append(normalized)`
   - Upload to cache MinIO
   - `ledger.mark_committed(...)` + `ledger.mark_cached(...)`
4. CLI flags: `--limit`, `--after`, `--before`, `--provider`, `--dry-run`
5. Idempotent via ledger SHA256 checks

**Note on existing data**: The existing parquets have `received_at_ms` (epoch int64) not `received_at` (UTC string). The migration normalizer path still handles this since `normalize_table()` for parquet input will need to handle the old format. This is a migration-only concern — once data is re-published, the old format is gone.

**Data volumes**:
- Options: 22 GB, 10,782 files (massive, schwab, tickrake)
- Level_one: 944 MB, 11,087 files (schwab)
- Order_book: 2.7 GB, 10,611 files (schwab)
- Candles: 209 MB, 7,111 parquets + 917 CSVs (tickrake, ibkr-paper, schwab)

---

## Canonical Parquet Physical Specs (enforced in `write_canonical_parquet`)

| Dataset | Sort Order | Row Group Size | Compression |
|---------|-----------|---------------|-------------|
| options | `expiration_date, sampled_at, strike, contract_type, symbol` | 50,000 | ZSTD level 3 |
| level_one | `received_at, quote_time, trade_time` | 150,000 | ZSTD level 3 |
| order_book | `received_at, book_time` | 35,000 | ZSTD level 5 |
| candles | `datetime` | 50,000 | ZSTD level 3 |

---

## Phase Dependency Graph

```
Phase 1 (FileStore)  →  Phase 2 (Discovery)  →  Phase 3 (Normalizers)
                                                        ↓
Phase 4 (Ledger)  ─────────────────────────────→  Phase 5 (Publisher+CLI)
                                                        ↓
                                                  Phase 6 (Migration)
```

Phases 1-3 and Phase 4 can proceed in parallel.

---

## Verification

After each phase: `make check` must pass.

End-to-end after Phase 6:
1. `python scripts/migrate_options.py --provider schwab --after 2026-10-01 --limit 5`
2. `catalog status --table options` → committed count increased
3. `catalog cache status --table options` → cached files visible
4. `DataReader().get_parquet("SPXW", date(2026, 10, 1))` → returns from cache
5. `catalog cache evict --older-than 1d --table options --yes` → clears entries
6. Verify parquet physical properties: open a produced file with PyArrow, check sort order, row group size, compression codec

---

## Key Files

- `src/tractatus/catalog/file_store.py`
- `src/tractatus/catalog/discovery.py`
- `src/tractatus/catalog/normalize/common.py`, `options.py`, `level_one.py`, `order_book.py`, `candles.py`, `__init__.py`
- `src/tractatus/catalog/ledger.py`
- `src/tractatus/catalog/publisher.py`
- `src/tractatus/catalog/config.py`
- `src/tractatus/cli.py`
- `scripts/migrate_*.py`
