"""Configuration for the unified data reader."""

from __future__ import annotations

import os
from dataclasses import dataclass

from tractatus.config import TractatusConfig


@dataclass(frozen=True)
class ReaderConfig:
    """Connection settings for MinIO and Iceberg lakehouse."""

    # MinIO
    minio_endpoint: str
    minio_intraday_bucket: str
    minio_lakehouse_bucket: str  # tractatus-lakehouse — pre-exported parquets
    minio_access_key: str
    minio_secret_key: str

    # Iceberg catalog
    catalog_type: str  # "sql" or "glue"
    iceberg_warehouse: str  # s3://... URI
    iceberg_postgres_url: str | None  # required when catalog_type="sql"
    glue_database: str  # namespace for Glue catalog
    aws_region: str
    aws_profile: str | None

    @classmethod
    def load(cls, tractatus_config: TractatusConfig | None = None) -> ReaderConfig:
        tc = tractatus_config or TractatusConfig.load()
        catalog_type = os.environ.get("ICEBERG_CATALOG_TYPE", "glue")
        if catalog_type == "sql":
            minio_iceberg_bucket = os.environ.get("MINIO_ICEBERG_BUCKET", "tractatus-iceberg")
            iceberg_warehouse = f"s3://{minio_iceberg_bucket}"
        else:
            iceberg_bucket = os.environ.get("ICEBERG_BUCKET", "tractatus-iceberg")
            iceberg_warehouse = f"s3://{iceberg_bucket}"
        return cls(
            minio_endpoint=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_intraday_bucket=os.environ.get("MINIO_INTRADAY_BUCKET", "tickrake"),
            minio_lakehouse_bucket=os.environ.get("MINIO_LAKEHOUSE_BUCKET", "tractatus-lakehouse"),
            minio_access_key=os.environ.get("MINIO_ACCESS_KEY", ""),
            minio_secret_key=os.environ.get("MINIO_SECRET_KEY", ""),
            catalog_type=catalog_type,
            iceberg_warehouse=iceberg_warehouse,
            iceberg_postgres_url=os.environ.get("ICEBERG_POSTGRES_URL"),
            glue_database=os.environ.get("GLUE_DATABASE", "tractatus"),
            aws_region=tc.aws_region,
            aws_profile=tc.aws_profile,
        )
