---
type: feature
tags: [options, iceberg, schema, publisher, parquet]
title: Correct the options Iceberg schema and make publication commit-safe
description: Define the canonical options schema in Tractatus, normalize legacy Parquet, rebuild the new table with correct types, and avoid orphan uploads on failed commits
created: 2026-10-15
updated: 2026-10-15
status: completed
priority: high
source: tractatus/catalog
---

# Summary

The current options Iceberg table schema does not match the intended long-term contract: `expiration_date` is stored as a string, and newer option fields are absent from older Parquet samples. The table and publisher are still early enough to correct the contract and rewrite/rebuild the table rather than preserving an incorrect type. Tractatus owns the canonical schema and compatibility normalization; Tickrake will continue emitting its existing Parquet contract and will not be changed to emit an Iceberg-specific schema. The publisher must validate and prepare data before upload and handle the separation between S3 object upload and Iceberg metadata commit so failed commits do not leave unmanaged objects behind.

## Requirements

- Define the canonical options Iceberg schema in the Tractatus library. Use a date type for `expiration_date`; do not retain the current string type as the target contract.
- Normalize source Parquet by column name to the canonical schema before publication:
  - Cast legacy string `expiration_date` values to dates, with clear validation for invalid values.
  - Add canonical fields missing from older files with the appropriate types and null/default semantics.
  - For records where `trade_at` or `quote_at` is absent or null, populate it from that record's `sampled_at` datetime.
  - Preserve values in fields that are present and valid; report incompatible or malformed inputs with file-level diagnostics.
- Plan and implement a migration/rebuild of the existing early-stage table so its actual schema and all retained data match the canonical schema. Rewriting is acceptable; do not rely on changing only the Glue catalog declaration while leaving incompatible Parquet files in place.
- Before uploading, validate that each file can be normalized to the canonical schema and is eligible for the intended Iceberg operation.
- Make publication failure-safe across S3 upload and Iceberg metadata commit. Track the exact objects created by a batch; on commit failure, clean up only newly uploaded objects that are confirmed not referenced by a successful Iceberg snapshot. If cleanup cannot be proven safe, retain explicit ledger state for retry/reconciliation and report the objects rather than silently treating them as published.
- Keep ledger state consistent with actual Iceberg snapshots across success, upload failure, commit failure, and retry. Reruns must be idempotent and must not duplicate table entries or delete data referenced by Iceberg.
- Add tests for legacy and current file schemas, date conversion and invalid dates, missing fields, `trade_at`/`quote_at` fallback, failed commits, safe orphan cleanup, and retry/reconciliation behavior.
- Document the migration procedure and operational recovery behavior.

## Dependencies & Resources

- Existing lakehouse design/backlog: `publish_options_parquet_to_iceberg_lakehouse.md`
- Current schema mismatch observed during publishing: table schema has 41 fields; older source Parquet had 31, including `expiration_date` as a date while the table expected a string. Newer fields absent from older samples include `trade_at`, `quote_at`, `time_value`, `net_change`, `percent_change`, `settlement_type`, `exercise_type`, `expiration_type`, `multiplier`, and `non_standard`.
- Tractatus schema and publisher implementation: `src/tractatus/catalog/schema.py`, `src/tractatus/catalog/publisher.py`, and `src/tractatus/catalog/ledger.py` (confirm current paths during implementation).
- Apache Iceberg schema evolution: https://iceberg.apache.org/docs/latest/evolution/
- PyIceberg API and `add_files` behavior: https://py.iceberg.apache.org/api/
