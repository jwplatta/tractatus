# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). This project uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `tractatus.catalog` module for publishing compacted options Parquet files to an Apache Iceberg lakehouse (AWS Glue catalog + S3).
  - `CatalogConfig` for Iceberg bucket, Glue database/table, data root, and PostgreSQL connection configuration via environment variables.
  - `Ledger` — PostgreSQL-backed publication ledger tracking file discovery, upload, and commit status with idempotent resume-after-failure. Creates the `catalog_files` table automatically on first connection.
  - `discover_files()` — scans configurable options data root for compacted parquet files, computing SHA-256 content identity.
  - `Publisher` — orchestrates discover → S3 upload → Iceberg `add_files()` commit pipeline with error handling and dry-run mode.
  - Iceberg table schema definition for options data (41 fields including contract classification, timing, and value columns, partitioned by expiration_date).
- CLI commands: `tractatus catalog publish` and `tractatus catalog status`.
- `pyiceberg[glue,s3fs]` and `psycopg[binary]` added as core dependencies.

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
