---
type: feature
tags: [options, iceberg, lakehouse, parquet, s3, quant-infra]
title: Publish compacted options Parquet to an Iceberg lakehouse
description: Replace custom S3 option archive indexes and manifests with an idempotent Iceberg table publication flow, starting with options data
created: 2026-10-09
updated: 2026-10-09
status: not-started
priority: high
source: tractatus/options-lakehouse
---

# Summary

Move archived options data toward an Apache Iceberg lakehouse, starting with compacted option-sample Parquet files. Tickrake remains responsible for collecting, validating, and compacting source snapshots into Parquet, then may delete the source samples after verifying the compacted output. Iceberg publishing is owned by an isolated Tractatus module and scheduled by quant-infra; Tickrake should not depend on Iceberg or the publisher. The publisher discovers completed Parquet files under the configured `~/.tickrake/data` options directory, uploads new files to S3, and commits them to an Iceberg table registered in AWS Glue. A persistent SQLite ledger in Tractatus tracks discovery and publication progress so runs are idempotent and can resume after failures. Tractatus will provide researchers one API over local cached Parquet and archived Iceberg data, using Iceberg metadata to identify archived files and avoiding S3 reads when the needed data is already local.

## Requirements

- **Options-first migration**:
  - Start with compacted historical options Parquet; leave other dataset types and intraday publication out of scope until this flow is proven.
  - Preserve the existing local options Parquet layout under `~/.tickrake/data` as the research cache and publisher input.
- **Tickrake producer boundary**:
  - Tickrake continues collecting source snapshots, validating compaction, and writing compacted Parquet.
  - Tickrake does not create Iceberg metadata, update Glue, or require publisher success to consider compaction complete.
  - Source snapshots are deleted only after Tickrake validates that the compacted Parquet contains the expected data.
- **Tractatus publisher**:
  - Add an isolated publishing module and explicit schemas for the options Iceberg table(s).
    - I think the module should be called something like `DataCatalog`
  - Discover completed Parquet files from a configurable options data root without requiring a Tickrake-to-publisher queue or Iceberg dependency in Tickrake.
  - Upload the Parquet object to S3 before committing its reference in Iceberg; a file is query-visible only after the Iceberg commit.
  - Keep a persistent SQLite ledger for discovered files, content identity, publication status, attempts/errors, and recovery. Reconcile ledger state with Iceberg so reruns safely handle failures between upload and commit and never create duplicate table entries.
  - Make upload keys deterministic and ensure incomplete or changing local files are not published.
- **Catalog and storage**:
  - Use AWS Glue Data Catalog for Iceberg metadata and S3 for table data and Iceberg metadata files.
    - Do I need to create a new bucket for this?
  - Keep the bucket configurable; determine whether to use a new bucket or a dedicated prefix in the existing archive bucket during implementation.
    - We will create a new bucket
  - Keep active Iceberg metadata in an immediately accessible storage class; define any archival lifecycle policy so it does not expire or transition files still required by the table.
- **Scheduled operation**:
  - Add a quant-infra publisher job that runs once daily after options compaction completes.
  - Mount the Tickrake data directory read-only for discovery and local research reads, and persist the Tractatus SQLite ledger across container runs.
  - Provide logs and a manual rerun path for recovery; do not require restarting or changing collection jobs to retry publication.
- **Research access and local-cache behavior**:
  - Keep a single Tractatus researcher-facing API.
  - Query local Parquet under `~/.tickrake/data` and archived Iceberg data through DuckDB as separate sources, combining results without duplicates.
    - This local parquet cache/file dir should be an env var or a config value, we should not hardcode `~/.tickrake/data`
  - Use Iceberg table metadata to resolve archived files; download/cache only required objects when they are not available locally, rather than scanning or querying all S3 data for every research request.
  - Preserve query filters for provider, option root, and sample date.
- **Validation and migration**:
  - Pilot with options data in an isolated development bucket or prefix before switching researcher reads.
  - Validate row counts, schemas, keys, date coverage, and local-versus-archived query results.
  - Backfill existing compacted Parquet without rewriting data files where supported; avoid retaining duplicate archive copies after validation and cutover.
  - Retire the custom options index/manifest read path only after Tractatus reads and recovery have been validated against Iceberg.

## Dependencies & Resources

- Tractatus package: `src/tractatus/tickrake/options/archive.py`, `src/tractatus/tickrake/client.py`, and `src/tractatus/tickrake/config.py`
- Tickrake options compaction and validation: `/Users/jplatta/repos/tickrake/lib/tickrake/maintenance/option_samples/compactor.rb`, `source_sample_cleaner.rb`, and `local_artifact_manager.rb`
- Existing Tickrake archive/index behavior: `/Users/jplatta/repos/tickrake/lib/tickrake/maintenance/option_samples/artifact_archiver.rb`, `manifest_writer.rb`, and `/Users/jplatta/repos/tickrake/docs/index_publishing.md`
- quant-infra scheduled service and mount configuration: `/Users/jplatta/repos/quant-infra/services/tickrake/compose.yml`, `/Users/jplatta/repos/quant-infra/deploy/prod.yml`, and `/Users/jplatta/repos/quant-infra/Makefile`
- Apache Iceberg AWS integrations: https://iceberg.apache.org/docs/latest/aws/
- PyIceberg documentation: https://py.iceberg.apache.org/
  - In particular, we want to review which dependencies we actually need to install with pyIceberg
- DuckDB Iceberg extension: https://duckdb.org/docs/current/core_extensions/iceberg/overview
