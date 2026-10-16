# tractatus

[![Tests](https://github.com/jwplatta/tractatus/actions/workflows/ci.yml/badge.svg)](https://github.com/jwplatta/tractatus/actions/workflows/ci.yml)

Tractatus Research — data access, calculations, and utilities for trading research. Built on data collected by [tickrake](https://github.com/jwplatta/tickrake).

## Installation

```bash
# As a git dependency
uv add "tractatus @ git+ssh://git@github.com/jwplatta/tractatus.git@main"
```

## Tickrake data access

The `tractatus.tickrake` module reads from `~/.tickrake/data/` by default. Override with the `TICKRAKE_DATA_DIR` environment variable.

| Dataset | Format | Source |
|---|---|---|
| **Options chains** | CSV (intraday), Parquet (historical) | MinIO, S3, local filesystem |
| **Candles** | CSV | Local filesystem |
| **Level one quotes** | Parquet | Local filesystem |
| **Order book** | Parquet | Local filesystem |

For MinIO and S3 access, set the relevant environment variables:

```bash
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_BUCKET="tickrake"
export MINIO_ACCESS_KEY="..."
export MINIO_SECRET_KEY="..."
export S3_BUCKET="your-tickrake-bucket"
export S3_REGION="us-east-1"
```

### Usage

```python
from tractatus.tickrake import TickrakeClient

client = TickrakeClient()
```

#### Candles

```python
client.candles.list_providers()  # ['ibkr-paper', 'schwab', ...]
client.candles.list_frequencies("schwab")  # ['1min', '5min', '30min', 'day']
client.candles.list_symbols("schwab", frequency="5min")  # ['SPX', 'SPY', ...]

from datetime import date

df = client.candles.read("SPX", "5min", provider="schwab")
df = client.candles.read("SPX", "5min", start=date(2026, 1, 1), end=date(2026, 6, 1))
client.candles.date_range("SPX", "5min")  # (datetime, datetime)
```

#### Level one quotes

```python
client.level_one.list_symbols("schwab")  # ['IWM', 'QQQ', 'SPY']
client.level_one.list_dates("schwab", symbol="SPY")  # [date(2026, 9, 18)]
df = client.level_one.read("SPY", date(2026, 9, 18))
```

#### Order book

```python
client.order_book.list_symbols("schwab")
client.order_book.list_dates("schwab", symbol="IWM")
df = client.order_book.read("IWM", date(2026, 9, 18))
```

#### Options

```python
client.options_filesystem.list_roots("schwab")  # ['AAPL', 'SPXW', ...]
client.options_filesystem.list_sample_dates("SPXW", "schwab")

# Scan local intraday snapshots
snapshots = client.options_filesystem.scan_snapshots("SPXW", date(2026, 9, 18))

# Get historical parquet (downloads from S3 if not cached locally)
path = client.options_archive.get_parquet_path("SPXW", date(2026, 9, 17))

# Fetch current intraday data from MinIO
expirations = client.options_intraday.list_expirations("SPXW")
df = client.options_intraday.fetch_csv(uri, dtypes={...})
```

## CLI

Tractatus includes a command-line interface for project scaffolding, data access, and Iceberg catalog management.

```bash
tractatus --help
```

### Project scaffolding

```bash
# Create a new research project with dependencies, MLflow, and tests
tractatus init research --name my-study

# Validate environment and run smoke backtest
tractatus verify

# Start JupyterLab
tractatus notebook
```

### Data access

```bash
# Pull a historical options parquet file into TICKRAKE_DATA_DIR
tractatus data pull SPXW 2026-06-25 --provider schwab
```

### Configuration

```bash
# Create ~/.tractatus/config.toml
tractatus config init

# Show resolved settings
tractatus config show --effective
```

### Iceberg catalog

Manages the publication pipeline: discover local Parquet files, normalize and upload to S3, and commit to Iceberg tables via AWS Glue.

```bash
# Create Iceberg tables from the schema registry (run once)
tractatus catalog setup

# Check ledger status
tractatus catalog status --table options

# Discover files without uploading
tractatus catalog discover --table options --provider schwab

# Full publish pipeline: discover → normalize → upload → commit
tractatus catalog publish --table options
tractatus catalog publish --table options --provider schwab --after 2026-06-01 --before 2026-07-01

# Partial reset scoped by date (deletes Iceberg rows, S3 files, and ledger records)
tractatus catalog reset --table options --after 2026-06-01 --before 2026-07-01

# Full purge for table rebuilds (drops table, deletes all S3 data, resets ledger)
tractatus catalog purge --table options

# Seed a new ledger from an existing Iceberg table
tractatus catalog sync-ledger --table options
```

#### Environment variables

| Variable | Required | Description |
|---|---|---|
| `ICEBERG_BUCKET` | Yes | S3 bucket for Iceberg data and metadata |
| `GLUE_DATABASE` | No | Glue database name (default: `tractatus`) |
| `CATALOG_DATABASE_URL` | No | Ledger database (default: `~/.tractatus/catalog_ledger.db`). Set to a `postgresql://` URL for production. |
| `S3_STORAGE_CLASS` | No | S3 storage class for uploads (default: `GLACIER_IR`) |

## Development

```bash
uv sync
uv run pytest
uv run ruff check .
uv run mypy src
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for workflow details.
