# Tractatus

Shared computation and data-access library for trading research. Provides tickrake data access, options calculations (GEX, vol, OI, z-scores), and market analysis utilities.

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

## Pre-commit / pre-merge requirement

Always run `make check` before committing or opening a PR. All four checks must pass.

## Architecture

```
src/tractatus/
  tickrake/          # Data access layer for tickrake (options, candles, order book)
    client.py        # TickrakeClient facade
    config.py        # TickrakeConfig (env-var-backed)
    filesystem.py    # Local filesystem discovery
    intraday.py      # S3/MinIO intraday snapshot access
    archive.py       # Historical archive access
  calc/              # Pure computation modules (no UI dependencies)
    gex.py           # Gamma exposure calculations
    gex_term_structure.py  # GEX term structure
    vol.py           # Volatility (IV, RV, spreads, VIX correlation)
    ma.py            # Moving averages (SMA, VWAP)
    oi.py            # Open interest matrix
    oi_zscore.py     # OI z-score calculation
    iv_zscore.py     # IV z-score calculation
    fixed_strike_vol.py  # Fixed-strike IV matrix
```

## Gotchas

- This package is **read-only** against tickrake data. Never write, modify, or delete files under `~/.tickrake/`.
- `TickrakeConfig` uses `data_dir` (not `options_dir`). Derived paths like `provider_options_dir(provider)` build from there.
- mypy is strict — all new code needs type annotations.
- `ruff` is the linter/formatter (not black/flake8); line length is 100.
- Calc modules must remain pure (no Streamlit, no I/O). They take DataFrames in and return DataFrames/values out.
