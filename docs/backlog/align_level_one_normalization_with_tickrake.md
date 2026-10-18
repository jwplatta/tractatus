---
type: feature
tags: [level-one, normalization, tickrake, iceberg]
title: Align Level One normalization with Tickrake output
description: Normalize Tickrake Level One Parquet without duplicate null fields or losing unrecognized feed data
created: 2026-10-17
updated: 2026-10-17
status: not-started
priority: high
source: tractatus/level-one-normalization
---

# Summary

Tractatus normalizes Tickrake Level One Parquet into equities and futures Iceberg tables, but parts of its JSON field maps still expect values that Tickrake has already extracted into top-level columns. This produces redundant null columns, while the current JSON unpacking drops unrecognized feed keys. Tickrake writes a `service` string column into both Level One and order book Parquet files, and Tractatus schemas retain it. However, Level One discovery currently classifies files by filename shape (a leading `^`), before reading Parquet, rather than using this service value; this can route files incorrectly. Align the normalizer and discovery behavior with the producer contract while retaining the existing conversion of top-level epoch-millisecond fields to UTC timestamps.

## Requirements

- Treat Tickrake's top-level fields as authoritative when the producer has already extracted a value from the Schwab stream. Do not recreate duplicate values from `extra_json` for those fields.
  - In particular, remove or otherwise resolve redundant mappings for equities keys `33`, `34`, and `35` and futures keys `10`, `11`, and `24`, which Tickrake extracts to top-level `mark`, `quote_time_ms`, and `trade_time_ms` fields.
  - Keep the canonical top-level timestamp conversion: `received_at_ms`, `quote_time_ms`, and `trade_time_ms` must become timezone-aware UTC `received_at`, `quote_time`, and `trade_time` values.
- Define and implement a loss policy for `extra_json` keys not mapped to canonical columns. Preserve them in a raw or extensible representation, or explicitly reject/report them with actionable diagnostics; do not silently discard them during normalization.
- Route Level One files using reliable producer metadata or an unambiguous directory/filename contract, rather than assuming a leading `^` alone identifies futures. Tickrake already writes `service` in each Level One Parquet row and Tractatus already retains that column in its Iceberg schemas, but the current discovery phase runs before Parquet contents are read; adapt the flow or use an equally reliable routing hint. Support each service type that Tickrake actually writes, and clearly reject unsupported service types rather than normalizing them under the wrong schema. Order book Parquet also carries `service`, which Tractatus retains; keep that field available in normalized output.
- Keep the normalized output aligned with the registered Iceberg schema. If this requires schema evolution, document the compatible migration/rebuild steps before changing an existing table contract.
- Add fixtures representative of Tickrake's emitted equities and futures Parquet, including populated top-level values and `extra_json`, and test:
  - values are retained once in their canonical columns, with no misleading duplicate null columns;
  - millisecond timestamps are converted to UTC timestamps correctly;
  - unknown JSON keys follow the chosen preservation or explicit-diagnostic policy;
  - service identification routes files to the correct normalizer and fails clearly for unsupported inputs.

## Dependencies & Resources

- Producer field extraction and Parquet contract in Tickrake's Level One compaction job; confirm the current contract at implementation time.
- Tractatus normalizer: `src/tractatus/catalog/normalize/level_one.py`.
- Tractatus field maps and schemas: `src/tractatus/catalog/schemas/level_one_equities.py` and `src/tractatus/catalog/schemas/level_one_futures.py`.
- File classification: `src/tractatus/catalog/discovery.py`.
- Existing Level One normalization tests: `tests/test_catalog_normalize_tables.py`.
