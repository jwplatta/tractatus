"""PyIceberg Glue catalog reader for lakehouse fallback."""

from __future__ import annotations

import logging

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog

from tractatus.reader.config import ReaderConfig

logger = logging.getLogger(__name__)


class IcebergStore:
    """Read-only access to Iceberg tables via Glue catalog."""

    def __init__(self, cfg: ReaderConfig) -> None:
        self._cfg = cfg
        self._catalog: Catalog | None = None

    @property
    def catalog(self) -> Catalog:
        if self._catalog is None:
            props: dict[str, str] = {
                "type": "glue",
                "glue.region": self._cfg.aws_region,
                "s3.region": self._cfg.aws_region,
                "warehouse": f"s3://{self._cfg.iceberg_bucket}",
            }
            if self._cfg.aws_profile:
                props["glue.profile-name"] = self._cfg.aws_profile
                props["s3.profile-name"] = self._cfg.aws_profile
            self._catalog = load_catalog("glue", **props)
        return self._catalog

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
