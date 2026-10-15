"""Configuration for the Iceberg catalog publisher."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from tractatus.config import TractatusConfig

_DEFAULTS: dict[str, str] = {
    "glue_database": "tractatus",
    "catalog_database_url": str(Path.home() / ".tractatus" / "catalog_ledger.db"),
}

_ENV_NAMES: dict[str, str] = {
    "iceberg_bucket": "ICEBERG_BUCKET",
    "glue_database": "GLUE_DATABASE",
    "catalog_database_url": "CATALOG_DATABASE_URL",
}


@dataclass(frozen=True)
class CatalogConfig:
    """Resolved configuration for Iceberg catalog operations."""

    iceberg_bucket: str
    iceberg_warehouse: str
    glue_database: str
    aws_region: str
    aws_profile: str | None
    data_root: Path
    database_url: str

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

        iceberg_bucket = values.get("iceberg_bucket")
        if not iceberg_bucket:
            raise ValueError("Missing required configuration: ICEBERG_BUCKET environment variable")

        return cls(
            iceberg_bucket=iceberg_bucket,
            iceberg_warehouse=f"s3://{iceberg_bucket}",
            glue_database=values.get("glue_database") or _DEFAULTS["glue_database"],
            aws_region=tc.aws_region,
            aws_profile=tc.aws_profile,
            data_root=tc.tickrake_data_dir,
            database_url=values.get("catalog_database_url") or _DEFAULTS["catalog_database_url"],
        )
