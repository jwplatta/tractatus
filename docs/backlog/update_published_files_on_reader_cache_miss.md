---
type: feature
tags: [published-files, reader, cache]
title: Update published_artifacts on reader cache miss
description: When reader fetches from lakehouse and writes to MinIO cache, upsert published_artifacts
created: 2026-10-22
updated: 2026-10-22
status: not-started
priority: medium
source: claude/tractatus
---

# Summary

When the reader refactor (DuckDB httpfs with MinIO) is complete, the reader will pull data
from the Iceberg lakehouse on a cache miss and write the result to the `tractatus-lakehouse`
MinIO bucket. At that point it should also upsert the corresponding `published_artifacts` record
with `cached=true`, `cached_at`, `size_bytes`, and `row_count` so the table stays current
without requiring a manual reconcile run.

## Requirements

- After a successful cache write to MinIO, call `ledger.upsert_published()` with the
  artifact metadata
- `cached` set to `true`, `cached_at` set to current UTC timestamp
- `size_bytes` from the MinIO object size
- `row_count` from the Arrow table row count
- Must not block or fail the read if the ledger write fails

## Dependencies & Resources

- Depends on: `docs/backlog/refactor_reader_duckdb_httpfs_minio.md`
- Depends on: `published_artifacts` table existing in the ledger (implemented in publisher PR)
- See: `docs/plans/lakehouse_backfill_and_visualization.md`
