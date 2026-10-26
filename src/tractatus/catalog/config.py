"""Configuration for the Iceberg catalog publisher."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from tractatus.config import TractatusConfig

_DEFAULTS: dict[str, str] = {
    "catalog_type": "glue",
    "glue_database": "tractatus",
    "ledger_database_url": str(Path.home() / ".tractatus" / "catalog_ledger.db"),
    "s3_storage_class": "GLACIER_IR",
}

_ENV_NAMES: dict[str, str] = {
    "catalog_type": "ICEBERG_CATALOG_TYPE",
    "iceberg_bucket": "ICEBERG_BUCKET",
    "iceberg_postgres_url": "ICEBERG_POSTGRES_URL",
    "iceberg_minio_bucket": "MINIO_ICEBERG_BUCKET",
    "glue_database": "GLUE_DATABASE",
    "ledger_database_url": "LEDGER_DATABASE_URL",
    "s3_storage_class": "S3_STORAGE_CLASS",
    "minio_endpoint": "MINIO_ENDPOINT",
    "minio_bucket": "MINIO_BUCKET",
    "minio_access_key": "MINIO_ACCESS_KEY",
    "minio_secret_key": "MINIO_SECRET_KEY",
    "cache_bucket": "MINIO_CACHE_BUCKET",
}


@dataclass(frozen=True)
class CatalogConfig:
    """Resolved configuration for Iceberg catalog operations."""

    catalog_type: str  # "glue" | "sql"
    iceberg_warehouse: str
    glue_database: str
    aws_region: str
    aws_profile: str | None
    data_root: Path
    database_url: str
    s3_storage_class: str
    # Glue catalog
    iceberg_bucket: str | None = None
    # SQL catalog (local PostgreSQL + MinIO)
    iceberg_postgres_url: str | None = None
    iceberg_minio_bucket: str | None = None
    # MinIO (staging + cache)
    minio_endpoint: str | None = None
    minio_bucket: str | None = None
    minio_access_key: str | None = None
    minio_secret_key: str | None = None
    cache_bucket: str | None = None

    @classmethod
    def load(cls, tractatus_config: TractatusConfig | None = None) -> CatalogConfig:
        """Build catalog config from environment, defaults, and TractatusConfig."""
        tc = tractatus_config or TractatusConfig.load()

        values: dict[str, str | None] = dict(_DEFAULTS)
        values.update(
            {
                key: os.environ[env_name]
                for key, env_name in _ENV_NAMES.items()
                if env_name in os.environ
            }
        )

        catalog_type = values.get("catalog_type") or _DEFAULTS["catalog_type"]

        if catalog_type == "glue":
            iceberg_bucket = values.get("iceberg_bucket")
            if not iceberg_bucket:
                raise ValueError(
                    "Missing required configuration: ICEBERG_BUCKET environment variable"
                )
            iceberg_warehouse = f"s3://{iceberg_bucket}"
        else:
            iceberg_bucket = None
            iceberg_minio_bucket = values.get("iceberg_minio_bucket")
            if not iceberg_minio_bucket:
                raise ValueError(
                    "Missing required configuration: MINIO_ICEBERG_BUCKET environment variable"
                )
            iceberg_warehouse = f"s3://{iceberg_minio_bucket}"

        return cls(
            catalog_type=catalog_type,
            iceberg_bucket=iceberg_bucket,
            iceberg_warehouse=iceberg_warehouse,
            iceberg_postgres_url=values.get("iceberg_postgres_url"),
            iceberg_minio_bucket=values.get("iceberg_minio_bucket"),
            glue_database=values.get("glue_database") or _DEFAULTS["glue_database"],
            aws_region=tc.aws_region,
            aws_profile=tc.aws_profile,
            data_root=tc.tickrake_data_dir,
            database_url=values.get("ledger_database_url") or _DEFAULTS["ledger_database_url"],
            s3_storage_class=values.get("s3_storage_class") or _DEFAULTS["s3_storage_class"],
            minio_endpoint=values.get("minio_endpoint"),
            minio_bucket=values.get("minio_bucket"),
            minio_access_key=values.get("minio_access_key"),
            minio_secret_key=values.get("minio_secret_key"),
            cache_bucket=values.get("cache_bucket"),
        )
