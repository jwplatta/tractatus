# Tractatus

Shared computation and data-access library for trading research. Tractatus is part of the quant platform — see the `quant-platform` skill for platform-wide context, cross-repository boundaries, and data flow. It owns normalization, canonical parquet generation, Iceberg lakehouse publishing, local MinIO cache management, and the research-facing historical data interface.

## Skills

Pull relevant skills before starting work:

```bash
skillex pull quant-platform --agent claude   # platform architecture, ownership boundaries, cross-repo work
skillex pull project-backlog --agent claude  # create and manage backlog items in docs/backlog/
```

Use `skillex list` to discover other available skills.

## Commands

```bash
# Run all checks (lint, format, typecheck, tests)
make check

# Individual checks
make lint        # ruff check src tests
make format      # ruff format --check src tests
make typecheck   # mypy src
make test        # pytest tests/ -q

# Auto-fix lint and format issues
make fix
```

## Contributing

Follow `CONTRIBUTING.md` for workflow, commit conventions, changelog requirements, and PR guidelines. These are guidelines — the user may explicitly ask to bypass specific ones. When they do, proceed without friction.

Key points:
- Run `make check` before every commit or PR. All four checks must pass.
- Use conventional commits (`feat:`, `fix:`, `chore:`, etc.).
- Stage specific files by name — never `git add .` or `git add -A` or a directory path.
- Update `CHANGELOG.md` with every PR.
- Keep changes targeted; do not mix unrelated work into the same branch.
- When a question is asked, answer the question. Do not make assumptions about what to do next.

## Architecture

```
src/tractatus/
  catalog/             # Lakehouse publishing pipeline
    config.py          # CatalogConfig (env-var-backed; requires ICEBERG_BUCKET)
    discovery.py       # File discovery from MinIO staging bucket
    file_store.py      # FileStore protocol: LocalFileStore + S3FileStore
    ledger.py          # Postgres/SQLite publication ledger (discovered → committed)
    publisher.py       # Normalize → sort → Iceberg append → MinIO cache
    normalize/         # Per-dataset normalizers (CSV/NDJSON/parquet → Arrow)
      common.py        # Shared helpers: timestamp casting, write_canonical_parquet
      options.py
      level_one.py
      order_book.py
      candles.py
      fundamentals.py
      economic_events.py
    schemas/           # PyIceberg schemas, partition specs, table properties
      options.py
      level_one_equities.py
      level_one_futures.py
      order_book.py
      candles.py
      fundamentals.py
      economic_events.py
  reader/              # Research-facing historical data interface
    config.py          # ReaderConfig (env-var-backed)
    reader.py          # DataReader: 3-tier resolution (local cache → MinIO → Iceberg)
    iceberg_store.py   # Read-only PyIceberg Glue catalog access
    minio_store.py     # MinIO boto3 client wrapper
  tickrake/            # Legacy access layer for old tickrake local filesystem data
    options/
      queries.py       # Options query helpers
  calc/                # Pure computation modules (no I/O, no UI dependencies)
    gex.py             # Gamma exposure
    gex_term_structure.py
    vol.py             # Volatility (IV, RV, spreads, VIX correlation)
    ma.py              # Moving averages (SMA, VWAP)
    oi.py              # Open interest matrix
    oi_zscore.py
    iv_zscore.py
    fixed_strike_vol.py
  cli.py               # `catalog` CLI: publish, discover, status, cache, reset, purge
  config.py            # TractatusConfig (AWS region/profile, data root)
  logging.py           # Structured logging helpers
```

## Data flow

```
tickrake-staging (MinIO)
        │  raw CSV/NDJSON
        ▼
  discovery.py  ──→  ledger (discovered)
        │
        ▼
  publisher.py
    normalize → sort → write_canonical_parquet
        │
        ├──→  Iceberg append (S3 / Glue catalog)  ──→  ledger (committed)
        └──→  MinIO cache (tractatus-lakehouse)    ──→  ledger (cached)

DataReader (reader/)
    1. local ~/.tractatus/cache/
    2. MinIO cache (tractatus-lakehouse)
    3. Iceberg scan (S3 Glue)
```

## Gotchas

- Iceberg table appends go through `table.append()` — never raw boto3 S3 uploads. S3 Glacier IR charges apply even after deletion; minimize churn.
- The Glue catalog and S3 bucket are in **us-east-1**. Using the wrong region silently returns "database not found."
- mypy is strict — all new code needs full type annotations.
- `ruff` is the linter/formatter (not black/flake8); line length is 100.
- Calc modules must remain pure — no I/O, no Streamlit. They take DataFrames in and return DataFrames/values out.
- `scripts/` contains one-off migration and verification scripts. They are not part of the production library.

## Docs conventions

All markdown files added to `docs/` must include front matter:

```markdown
---
date_created: YYYY-MM-DD
description: One sentence describing what this document covers.
pull_request: https://github.com/jwplatta/tractatus/pull/<N>  # omit if not tied to a PR
---
```

## UI

The Tractatus coverage app and any future dashboards use a shared role-based color palette with light and dark mode variants. See [`docs/notes/ui_color_palette.md`](docs/notes/ui_color_palette.md) before adding colors to any UI component. Do not introduce one-off colors — extend the palette note instead.

## Backlog

Use the `project-backlog` skill to create and manage work items in `docs/backlog/`. Items are committed markdown files — not ephemeral notes.

```bash
python3 .claude/skills/project-backlog/scripts/search.py --status not-started --priority high
```
