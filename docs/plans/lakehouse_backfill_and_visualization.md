# Plan: published_artifacts Table, Backfill, and Coverage Visualization

## Context

The `catalog_files` ledger tracks source CSV files at the grain of the tickrake staging bucket.
That's the right grain for the publisher pipeline, but the wrong grain for answering "what data
do we have in the lakehouse?" — which is naturally at the grain of published parquet artifacts
(one per ticker per day).

A separate `published_artifacts` table tracks what is actually in the lakehouse and the MinIO cache.

---

## published_artifacts Schema

| column | type | description |
|---|---|---|
| `id` | integer PK | |
| `table_name` | text | e.g. `options`, `candles` |
| `provider` | text | e.g. `schwab`, `massive` |
| `root` | text | ticker symbol |
| `sample_date` | text | YYYY-MM-DD |
| `committed_at` | text | when published to Iceberg |
| `cache_key` | text | MinIO path, null until cached |
| `row_count` | integer | null until cached or explicitly read |
| `size_bytes` | integer | null until cached |

---

## Backfill Strategy

### Step 1 — Seed from ~/.tickrake/data directory (free, no S3)
Walk `~/.tickrake/data/options/schwab/` and `~/.tickrake/data/options/massive/` — the path
structure encodes `provider`, `root`, and `sample_date` directly. Write one `published_artifacts`
record per (provider, root, sample_date) group with `cache_key`, `row_count`, `size_bytes`
all null. This gives full ticker+date coverage without touching S3.

### Step 2 — Lazy fill on read (once reader refactor is done)
When the reader pulls a ticker+date from the lakehouse to MinIO cache, update the
`published_artifacts` record with `cache_key`, `row_count`, `size_bytes`. Over time the table
fills in naturally as data gets used.

### Going forward — publisher writes to published_artifacts at commit time
After every `iceberg_table.append()`, the publisher writes a `published_artifacts` record with
`committed_at` and whatever metadata is available from the Iceberg snapshot summary.

---

## Visualization App

A simple local Streamlit app reads only from `published_artifacts` (no S3, no Iceberg):
- Calendar heatmap per ticker — which dates have data
- Filter by `table_name`, `provider`
- Cache status indicator — cached vs lakehouse-only
- Gap detection — missing trading days within a covered range

---

## Decisions

- **Providers in scope:** `schwab` and `massive` only
- **Same DB:** `published_artifacts` lives in `~/.tractatus/catalog_ledger.db` alongside
  `catalog_files`. `_ensure_schema` uses `CREATE TABLE IF NOT EXISTS` so it is safe.
- **Massive path structure:** identical to schwab (`provider/root/YYYY/MM/DD/`)
- **Publisher write timing:** write to `published_artifacts` immediately after
  `iceberg_table.append()` succeeds, before `mark_committed`. No atomic transaction needed —
  exception before this line means we never write, which is the correct behavior.

## Implementation Steps

1. **Migration** — add `published_artifacts` table to `ledger.py` via `_ensure_schema` using
   `CREATE TABLE IF NOT EXISTS`. No constraints beyond `artifact_key` as PK. All other
   columns nullable.

2. **Ledger methods** — add `record_published` and `upsert_published` to `Ledger`.

3. **Publisher** — after each successful `iceberg_table.append()`, call `record_published`
   with the group metadata. Write fails silently logged — must not block the publish.

4. **Backfill script** — walk MinIO `tractatus-lakehouse` bucket under `options/` for
   `schwab` and `massive` providers. Parse `artifact_key`, `provider`, `root`, `sample_date`
   from the object key. Set `cached=true`, `size_bytes` from object metadata. Upsert into
   `published_artifacts`.

5. **Reconcile command** — `tractatus catalog reconcile --table options` diffs
   `published_artifacts` against the MinIO bucket and reports: in DB but not in MinIO, in MinIO
   but not in DB.

6. **Lazy reader update** — tracked in backlog (see below). When the reader pulls data from
   the lakehouse to MinIO cache, upsert the `published_artifacts` record with `cached=true`,
   `cached_at`, `size_bytes`, `row_count`.

## Backlog

- `backfill_published_artifacts_on_reader_cache_miss.md` — when reader fetches from lakehouse
  and writes to MinIO, upsert `published_artifacts` so the table stays current without a manual
  reconcile run.
