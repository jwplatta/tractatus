---
type: feature
tags: [published-artifacts, visualization, streamlit]
title: Lakehouse coverage visualization app
description: Local Streamlit app showing which tickers and dates are available in the lakehouse and cache
created: 2026-10-22
updated: 2026-10-22
status: not-started
priority: medium
source: claude/tractatus
---

# Summary

A local Streamlit app that reads from `published_artifacts` in the ledger SQLite DB and
visualizes what data is available in the lakehouse and MinIO cache. No S3 or Iceberg reads
at render time — all data comes from the ledger.

## Requirements

- Calendar heatmap per ticker showing which dates have data
- Filter by `table_name` (options, candles, etc.) and `provider`
- Cache status indicator — cached (in MinIO) vs lakehouse-only
- Gap detection — highlight missing trading days within a covered date range
- Must read only from `published_artifacts` — no S3, no Iceberg, no network calls at render time

## Dependencies & Resources

- Depends on: `published_artifacts` table populated (PR #27)
- Depends on: `scripts/backfill_published_artifacts.py` having been run
- See: `docs/plans/lakehouse_backfill_and_visualization.md`
