"""Unified DuckDB reader for local parquet and Iceberg-archived options data."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from tractatus.catalog.config import CatalogConfig
from tractatus.tickrake.options.queries import _normalize_df


class CatalogReader:
    """Query options data from local parquet or the Iceberg archive.

    Checks local parquet first. Falls back to the Iceberg table via DuckDB's
    iceberg extension when the local file is not available.

    Parameters
    ----------
    config:
        Catalog configuration with data_root and Iceberg connection details.
    conn:
        Optional pre-configured DuckDB connection. Creates an in-memory
        connection if not provided.
    """

    def __init__(
        self,
        config: CatalogConfig,
        conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self._config = config
        self._conn = conn if conn is not None else duckdb.connect(":memory:")
        self._glue_attached = False

    def _local_parquet(self, root: str, sample_date: date, provider: str = "schwab") -> Path | None:
        """Return the local parquet path if it exists on disk."""
        path = (
            self._config.data_root
            / provider
            / f"{sample_date:%Y}"
            / f"{sample_date:%m}"
            / f"{sample_date:%d}"
            / f"{root}_samples_{sample_date.isoformat()}.parquet"
        )
        return path if path.exists() else None

    def _ensure_iceberg(self) -> None:
        """Attach the Glue catalog via DuckDB's iceberg extension once."""
        if self._glue_attached:
            return
        self._conn.execute("INSTALL iceberg; LOAD iceberg;")
        self._conn.execute("INSTALL httpfs; LOAD httpfs;")

        self._conn.execute(f"SET s3_region = '{self._config.aws_region}'")
        if self._config.aws_profile:
            self._conn.execute(f"SET s3_profile = '{self._config.aws_profile}'")

        self._conn.execute("ATTACH '' AS glue_catalog (TYPE ICEBERG, ENDPOINT_TYPE 'glue')")
        self._glue_attached = True

    def _iceberg_table_ref(self) -> str:
        """Return the fully qualified Iceberg table reference for DuckDB."""
        return f"glue_catalog.{self._config.glue_database}.{self._config.glue_table}"

    def _query(
        self,
        root: str,
        sample_date: date,
        where: str,
        params: list[Any],
        provider: str = "schwab",
    ) -> pd.DataFrame:
        """Run a query against local parquet or Iceberg, returning a DataFrame."""
        local = self._local_parquet(root, sample_date, provider)
        if local is not None:
            source = f"read_parquet('{local}')"
        else:
            self._ensure_iceberg()
            source = self._iceberg_table_ref()

        query = f"SELECT * FROM {source} WHERE {where}"
        df = self._conn.execute(query, params).df()
        return _normalize_df(df)

    def load_expiry(
        self,
        root: str,
        sample_date: date,
        expiry: date,
        provider: str = "schwab",
    ) -> pd.DataFrame:
        """Load all snapshots for an expiry on a sample date."""
        return self._query(
            root,
            sample_date,
            "expiration_date = ? ORDER BY sampled_at",
            [expiry.isoformat()],
            provider,
        )

    def load_snapshot(
        self,
        root: str,
        sample_date: date,
        expiry: date,
        sampled_at: datetime,
        provider: str = "schwab",
    ) -> pd.DataFrame:
        """Load a single snapshot for one expiry and timestamp."""
        return self._query(
            root,
            sample_date,
            "expiration_date = ? AND CAST(sampled_at AS TIMESTAMPTZ) = CAST(? AS TIMESTAMPTZ)",
            [expiry.isoformat(), sampled_at.isoformat()],
            provider,
        )

    def list_expirations(
        self,
        root: str,
        sample_date: date,
        min_date: date | None = None,
        provider: str = "schwab",
    ) -> list[date]:
        """Return distinct expiration dates from a sample date's data."""
        local = self._local_parquet(root, sample_date, provider)
        if local is not None:
            source = f"read_parquet('{local}')"
        else:
            self._ensure_iceberg()
            source = self._iceberg_table_ref()

        if min_date is not None:
            query = (
                f"SELECT DISTINCT expiration_date FROM {source} "
                f"WHERE expiration_date >= ? ORDER BY expiration_date"
            )
            rows = self._conn.execute(query, [min_date.isoformat()]).fetchall()
        else:
            query = f"SELECT DISTINCT expiration_date FROM {source} ORDER BY expiration_date"
            rows = self._conn.execute(query).fetchall()

        return [date.fromisoformat(str(r[0])) for r in rows]
