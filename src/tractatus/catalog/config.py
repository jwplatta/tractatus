"""Configuration for the Iceberg catalog publisher."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from tractatus.config import TractatusConfig

_DEFAULTS: dict[str, str] = {
    "glue_database": "tractatus",
    "glue_table": "options",
    "catalog_database_url": "postgresql://tractatus_user:tractatus@localhost:5432/tractatus",
}

_ENV_NAMES: dict[str, str] = {
    "iceberg_bucket": "ICEBERG_BUCKET",
    "glue_database": "GLUE_DATABASE",
    "glue_table": "GLUE_TABLE",
    "catalog_database_url": "CATALOG_DATABASE_URL",
    "options_data_root": "OPTIONS_DATA_ROOT",
}


@dataclass(frozen=True)
class CatalogConfig:
    """Resolved configuration for Iceberg catalog operations."""

    iceberg_bucket: str
    iceberg_warehouse: str
    glue_database: str
    glue_table: str
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

        data_root_str = values.get("options_data_root")
        data_root = (
            Path(data_root_str).expanduser() if data_root_str else tc.tickrake_data_dir / "options"
        )

        return cls(
            iceberg_bucket=iceberg_bucket,
            iceberg_warehouse=f"s3://{iceberg_bucket}/warehouse",
            glue_database=values.get("glue_database") or _DEFAULTS["glue_database"],
            glue_table=values.get("glue_table") or _DEFAULTS["glue_table"],
            aws_region=tc.aws_region,
            aws_profile=tc.aws_profile,
            data_root=data_root,
            database_url=values.get("catalog_database_url") or _DEFAULTS["catalog_database_url"],
        )
