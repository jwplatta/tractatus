"""Configuration for the unified data reader."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from tractatus.config import TractatusConfig

_CACHE_DIR_DEFAULT = Path.home() / ".tractatus" / "cache"


@dataclass(frozen=True)
class ReaderConfig:
    """Connection settings for MinIO (intraday + historical) and Iceberg lakehouse."""

    # MinIO
    minio_endpoint: str
    minio_intraday_bucket: str
    minio_historical_bucket: str
    minio_access_key: str
    minio_secret_key: str

    # Iceberg / lakehouse
    iceberg_bucket: str
    glue_database: str
    aws_region: str
    aws_profile: str | None

    # Local cache for downloaded parquets (DuckDB needs local files)
    cache_dir: Path

    @classmethod
    def load(cls, tractatus_config: TractatusConfig | None = None) -> ReaderConfig:
        tc = tractatus_config or TractatusConfig.load()
        return cls(
            minio_endpoint=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_intraday_bucket=os.environ.get("MINIO_INTRADAY_BUCKET", "tickrake"),
            minio_historical_bucket=os.environ.get(
                "MINIO_HISTORICAL_BUCKET", "tickrake-historical"
            ),
            minio_access_key=os.environ.get("MINIO_ACCESS_KEY", ""),
            minio_secret_key=os.environ.get("MINIO_SECRET_KEY", ""),
            iceberg_bucket=os.environ.get("ICEBERG_BUCKET", ""),
            glue_database=os.environ.get("GLUE_DATABASE", "tractatus"),
            aws_region=tc.aws_region,
            aws_profile=tc.aws_profile,
            cache_dir=Path(os.environ.get("TRACTATUS_CACHE_DIR", str(_CACHE_DIR_DEFAULT))),
        )
