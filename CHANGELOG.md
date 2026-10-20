# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `tractatus.catalog.validate` module for options sample quality validation.
  - `validate_options()` runs a chain of validators after normalization and before Iceberg append, annotating each row with `sample_quality` (`"degraded"` or null) and `quality_flags` (comma-joined detail flags).
  - `detect_greek_sentinels`: nulls Schwab sentinel value -999.0 in delta, gamma, theta, vega, rho, and volatility; sets flag `greeks_sentinel`.
  - `detect_theoretical_sentinel`: nulls Schwab sentinel value -1.0 in `theoretical_option_value`; sets flag `theoretical_sentinel`.
  - `detect_missing_ohlc`: flags rows where all OHLC columns are null (`ohlc_missing`); values unchanged.
  - `QualityReport` dataclass returned alongside the annotated table with `rows_total`, `rows_degraded`, and `flags_seen`.
- Iceberg options schema extended with `sample_quality` (field_id=42) and `quality_flags` (field_id=43).
- Ledger `catalog_files` table extended with `rows_total`, `rows_degraded`, and `quality_flags` columns to record per-file quality stats at commit time.
- `Ledger.quality_summary()` returns committed files that have at least one degraded row.

### Changed

- Publisher calls `validate_options()` for the `options` table after normalization; passes `QualityReport` to `mark_committed()` and logs a warning when degraded rows are detected.
- `Ledger.mark_committed()` accepts an optional `QualityReport` to populate the new quality columns.



- `tractatus.catalog` module for publishing compacted options Parquet files to an Apache Iceberg lakehouse (AWS Glue catalog + S3).
  - `CatalogConfig` for Iceberg bucket, Glue database/table, data root, and PostgreSQL connection configuration via environment variables.
  - `Ledger` — PostgreSQL-backed publication ledger tracking file discovery, upload, and commit status with idempotent resume-after-failure. Creates the `catalog_files` table automatically on first connection.
  - `discover_files()` — scans configurable options data root for compacted parquet files, computing SHA-256 content identity.
  - `Publisher` — orchestrates discover → S3 upload → Iceberg `add_files()` commit pipeline with error handling and dry-run mode.
  - Iceberg table schema definition for options data (41 fields including contract classification, timing, and value columns, partitioned by expiration_date).
- `normalize` module that reconciles legacy 31-column and current 41-column Parquet files to the canonical Iceberg schema before upload. Casts `expiration_date` strings to dates, adds missing columns, and fills null `trade_at`/`quote_at` from `sampled_at`.
- CLI commands: `tractatus catalog setup`, `tractatus catalog publish`, `tractatus catalog discover`, `tractatus catalog status`, `tractatus catalog reset`, `tractatus catalog purge`, `tractatus catalog sync-ledger`.
- CLI reference in README with examples and environment variable documentation.
- `pyiceberg[glue,s3fs]` and `psycopg[binary]` added as core dependencies.

### Changed

- Publisher uses `table.append()` to let Iceberg handle both S3 writes and metadata commits atomically. Removed manual boto3 S3 uploads, `add_files`, and orphan cleanup logic.
- Publisher loads the Iceberg table directly via `load_table()` instead of auto-creating with `ensure_table()`. Use `tractatus catalog setup` to create tables.
- Files go directly from `discovered` to `committed` in the ledger — no intermediate `uploaded` state in the publish pipeline.

### Fixed

- `expiration_date` type corrected from `StringType` to `DateType` in the Iceberg schema.
- `catalog reset` catches `NoSuchTableError` instead of bare `Exception`.
- `catalog purge` uses `purge_table()` (correct PyIceberg 0.12 API) instead of `drop_table(purge_requested=True)`.

## [0.2.0] - 2026-09-19

### Changed

- Renamed package from `tickrake-client` to `tractatus`.
- Moved all tickrake data access modules under `tractatus.tickrake` subpackage. Import path is now `from tractatus.tickrake import TickrakeClient`.
- Updated intraday index reader to support renamed `option_chains` key (tickrake PR #92), with backward compatibility for `intraday` key.

### Added

- `IntradayClient.fetch_candle_csv()` and `list_candle_frequencies()` for reading candle CSVs from MinIO.

## [0.1.0] - 2026-09-19

### Added

- `TickrakeConfig` with data dir and provider path helpers, configurable via environment variables.
- `CandlesClient` for reading candle CSVs with date range filtering and discovery helpers (`list_symbols`, `list_frequencies`, `list_providers`, `date_range`).
- `LevelOneClient` for reading level one quote parquet snapshots with discovery helpers (`list_symbols`, `list_dates`, `list_providers`).
- `OrderBookClient` for reading order book parquet snapshots with discovery helpers (`list_symbols`, `list_dates`, `list_providers`).
- Options sub-clients:
  - `ArchiveClient` for S3 historical parquet with local cache.
  - `IntradayClient` for MinIO intraday CSV access.
  - `FilesystemClient` for local snapshot scanning and index reading (`list_roots`, `list_sample_dates`, `scan_snapshots`).
- `TickrakeClient` facade exposing all sub-clients.
- Basic test suite covering all dataset clients.
