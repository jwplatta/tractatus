"""PyIceberg catalog reader for the lakehouse."""

from __future__ import annotations

import logging

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog

from tractatus.reader.config import ReaderConfig

logger = logging.getLogger(__name__)


class IcebergStore:
    """Read-only access to Iceberg tables via SQL or Glue catalog."""

    def __init__(self, cfg: ReaderConfig) -> None:
        self._cfg = cfg
        self._catalog: Catalog | None = None

    @property
    def catalog(self) -> Catalog:
        if self._catalog is None:
            self._catalog = self._load_catalog()
        return self._catalog

    def _load_catalog(self) -> Catalog:
        if self._cfg.catalog_type == "sql":
            if not self._cfg.iceberg_postgres_url:
                raise ValueError("catalog_type=sql requires ICEBERG_POSTGRES_URL to be set")
            props: dict[str, str] = {
                "type": "sql",
                "uri": self._cfg.iceberg_postgres_url,
                "warehouse": self._cfg.iceberg_warehouse,
            }
            if self._cfg.minio_endpoint:
                props["s3.endpoint"] = self._cfg.minio_endpoint
                props["s3.path-style-access"] = "true"
            if self._cfg.minio_access_key:
                props["s3.access-key-id"] = self._cfg.minio_access_key
            if self._cfg.minio_secret_key:
                props["s3.secret-access-key"] = self._cfg.minio_secret_key
            return load_catalog("sql", **props)

        props = {
            "type": "glue",
            "glue.region": self._cfg.aws_region,
            "s3.region": self._cfg.aws_region,
            "warehouse": self._cfg.iceberg_warehouse,
        }
        if self._cfg.aws_profile:
            props["glue.profile-name"] = self._cfg.aws_profile
            props["s3.profile-name"] = self._cfg.aws_profile
        return load_catalog("glue", **props)

    def scan(
        self,
        table_name: str,
        row_filter: str | None = None,
        selected_fields: tuple[str, ...] | None = None,
    ) -> pa.Table:
        """Scan an Iceberg table and return an Arrow table."""
        fqn = f"{self._cfg.glue_database}.{table_name}"
        table = self.catalog.load_table(fqn)
        kwargs: dict[str, object] = {}
        if row_filter:
            kwargs["row_filter"] = row_filter
        if selected_fields:
            kwargs["selected_fields"] = selected_fields
        scan = table.scan(**kwargs)  # type: ignore[arg-type]
        return scan.to_arrow()

    def scan_to_dataframe(
        self,
        table_name: str,
        row_filter: str | None = None,
        selected_fields: tuple[str, ...] | None = None,
    ) -> pd.DataFrame:
        """Scan an Iceberg table and return a pandas DataFrame."""
        arrow = self.scan(table_name, row_filter, selected_fields)
        return arrow.to_pandas()  # type: ignore[no-any-return]
