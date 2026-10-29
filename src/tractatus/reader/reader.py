"""CacheReader — reads market data from the tractatus-lakehouse MinIO cache bucket."""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any
from urllib.parse import urlparse

import duckdb
import pandas as pd

from tractatus.reader.config import ReaderConfig
from tractatus.reader.minio_store import MinIOStore
from tractatus.tickrake.options.queries import OPTIONS_DTYPES, OptionsQueryClient

logger = logging.getLogger(__name__)

_DATE_DIR_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})")


@dataclass(frozen=True)
class OptionSource:
    """A discovered historical options parquet with its identity attributes."""

    sample_date: date
    uri: str
    revision: str  # "v1:{size_bytes}" — changes when the file is replaced
    size_bytes: int


class CacheReader:
    """Read market data from the tractatus-lakehouse MinIO cache bucket.

    Options parquets live at:
        tractatus-lakehouse/options/{provider}/YYYY/MM/DD/{root}_samples_{date}.parquet

    DuckDB reads s3:// URIs directly via httpfs — no local downloads.
    Intraday data is read from the intraday MinIO bucket.
    """

    def __init__(
        self,
        cfg: ReaderConfig | None = None,
        conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self._cfg = cfg or ReaderConfig.load()
        self._minio = MinIOStore(self._cfg)
        self._conn = self._configure_conn(conn or duckdb.connect(":memory:"), self._cfg)
        self._query = OptionsQueryClient(self._conn)

    @property
    def options_query(self) -> OptionsQueryClient:
        return self._query

    # ------------------------------------------------------------------
    # Historical options — tractatus-lakehouse cache, s3:// URIs only
    # ------------------------------------------------------------------

    def list_option_roots(self, provider: str = "schwab") -> list[str]:
        """List available option roots from tractatus-lakehouse key listing."""
        prefix = f"options/{provider}/"
        keys = self._minio.list_keys(self._minio.lakehouse_bucket, prefix)
        roots: set[str] = set()
        for key in keys:
            filename = key.rsplit("/", 1)[-1]
            if filename.endswith(".parquet") and "_samples_" in filename:
                root = filename.split("_samples_")[0]
                if root:
                    roots.add(root)
        return sorted(roots)

    def list_option_sample_dates(self, root: str, provider: str = "schwab") -> list[date]:
        """List available sample dates for a root from tractatus-lakehouse key listing."""
        prefix = f"options/{provider}/"
        keys = self._minio.list_keys(self._minio.lakehouse_bucket, prefix)
        dates: set[date] = set()
        for key in keys:
            if f"/{root}_samples_" not in key:
                continue
            m = _DATE_DIR_RE.search(key)
            if m:
                dates.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
        return sorted(dates)

    def get_option_parquet(
        self,
        root: str,
        sample_date: date,
        provider: str = "schwab",
    ) -> str:
        """Return an s3:// URI for a historical options parquet from tractatus-lakehouse.

        Raises FileNotFoundError if the parquet is not in the cache.
        DuckDB reads the returned URI directly via httpfs — no local download.
        """
        key = self._lakehouse_parquet_key(root, sample_date, provider)
        if self._minio.head_object(self._minio.lakehouse_bucket, key):
            return f"s3://{self._minio.lakehouse_bucket}/{key}"
        raise FileNotFoundError(
            f"No options parquet in cache for {root}/{sample_date} (provider={provider})"
        )

    def ensure_option_parquets(
        self,
        root: str,
        after: date,
        before: date,
        provider: str = "schwab",
    ) -> list[str]:
        """Return s3:// URIs for all available dates in [after, before]."""
        uris: list[str] = []
        for d in self.list_option_sample_dates(root, provider):
            if after <= d <= before:
                try:
                    uris.append(self.get_option_parquet(root, d, provider))
                except FileNotFoundError:
                    logger.warning("No parquet for %s/%s — skipping", root, d)
        return uris

    def discover_option_sources(
        self,
        roots: list[str],
        after: date,
        before: date,
        provider: str = "schwab",
    ) -> dict[str, list[OptionSource]]:
        """List the lakehouse once and return {root: [OptionSource]} for all requested roots.

        Performs a single S3 listing under options/{provider}/ and partitions
        results by root, filtering to the [after, before] date range.
        Roots with no available files are omitted from the result.
        """
        if not roots:
            return {}
        prefix = f"options/{provider}/"
        wanted = set(roots)
        inventory: dict[str, list[OptionSource]] = {}
        for key, size in self._minio.list_objects(self._minio.lakehouse_bucket, prefix):
            filename = key.rsplit("/", 1)[-1]
            if not filename.endswith(".parquet") or "_samples_" not in filename:
                continue
            root = filename.split("_samples_")[0]
            if root not in wanted:
                continue
            m = _DATE_DIR_RE.search(key)
            if not m:
                continue
            sample_date = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
            if not (after <= sample_date <= before):
                continue
            source = OptionSource(
                sample_date=sample_date,
                uri=f"s3://{self._minio.lakehouse_bucket}/{key}",
                revision=f"v1:{size}",
                size_bytes=size,
            )
            inventory.setdefault(root, []).append(source)
        for sources in inventory.values():
            sources.sort(key=lambda s: s.sample_date)
        return inventory

    def list_option_sources(
        self,
        root: str,
        after: date,
        before: date,
        provider: str = "schwab",
    ) -> list[OptionSource]:
        """Return OptionSources for root in [after, before], sorted by sample_date."""
        return self.discover_option_sources([root], after, before, provider).get(root, [])

    # ------------------------------------------------------------------
    # Query context — DuckDB reads s3:// URIs directly via httpfs
    # ------------------------------------------------------------------

    @contextmanager
    def query_connection(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Context manager that yields the configured DuckDB connection."""
        yield self._conn

    def option_daily_snapshots(
        self,
        source: OptionSource,
        target_dte: int,
        conn: duckdb.DuckDBPyConnection,
    ) -> pd.DataFrame:
        """Load all option rows for the expiry closest to target_dte from source.

        Queries source.uri directly via DuckDB httpfs. Returns rows ordered by
        sampled_at and strike. Returns an empty DataFrame if no rows match.
        """
        sample_iso = source.sample_date.isoformat()
        uri = source.uri
        df = conn.execute(f"""
            WITH candidates AS (
                SELECT *,
                    ABS(DATEDIFF('day', DATE '{sample_iso}',
                        CAST(expiration_date AS DATE)) - {target_dte}) AS _dte_diff
                FROM read_parquet('{uri}', union_by_name=true)
            ),
            closest_exp AS (
                SELECT expiration_date
                FROM candidates
                ORDER BY _dte_diff, expiration_date
                LIMIT 1
            )
            SELECT c.* EXCLUDE (_dte_diff)
            FROM candidates c
            JOIN closest_exp e ON c.expiration_date = e.expiration_date
            ORDER BY sampled_at, strike
        """).df()
        df = df.astype({col: dtype for col, dtype in OPTIONS_DTYPES.items() if col in df.columns})
        if "expiration_date" in df.columns:
            df["expiration_date"] = pd.to_datetime(df["expiration_date"])
        if "contract_type" in df.columns:
            df["contract_type"] = df["contract_type"].str.upper()
        return df

    # ------------------------------------------------------------------
    # Intraday (live) — reads from intraday MinIO bucket
    # ------------------------------------------------------------------

    def list_roots(self, provider: str = "schwab") -> list[str]:
        """List option roots available in the intraday MinIO index."""
        prefix = f"intraday/{provider}/"
        keys = self._minio.list_keys(self._minio.intraday_bucket, prefix)
        roots: list[str] = []
        for key in keys:
            if key.endswith(".json"):
                stem = key[len(prefix) :].removesuffix(".json")
                if stem and "/" not in stem:
                    roots.append(stem)
        return sorted(roots)

    def fetch_intraday_index(self, root: str, provider: str = "schwab") -> dict[str, Any]:
        """Fetch the intraday index JSON for a root."""
        key = f"intraday/{provider}/{root}.json"
        data = self._minio.get_object(self._minio.intraday_bucket, key)
        return json.loads(data)  # type: ignore[no-any-return]

    def intraday_updated_at(self, root: str, provider: str = "schwab") -> datetime | None:
        """Return the updated_at timestamp from the intraday index, or None."""
        try:
            index = self.fetch_intraday_index(root, provider)
        except Exception:
            return None
        raw = index.get("updated_at")
        if not raw:
            return None
        return datetime.fromisoformat(str(raw).replace("Z", "+00:00"))

    def list_intraday_expirations(self, root: str, provider: str = "schwab") -> list[date]:
        """Return sorted expiration dates from the intraday index."""
        index = self.fetch_intraday_index(root, provider)
        files = self._intraday_latest_files(index)
        return sorted({date.fromisoformat(str(f["expiration_date"])) for f in files})

    def latest_snapshots(
        self,
        root: str,
        start_exp: date,
        end_exp: date,
        provider: str = "schwab",
    ) -> dict[date, str]:
        """Return {expiry: s3_uri} for expirations in [start_exp, end_exp]."""
        result = self._list_latest_uris(root, start_exp, end_exp, provider)
        if not result:
            index = self.fetch_intraday_index(root, provider)
            files = self._intraday_latest_files(index)
            for f in files:
                exp = date.fromisoformat(str(f["expiration_date"]))
                if start_exp <= exp <= end_exp:
                    result[exp] = str(f["uri"])
        return dict(sorted(result.items()))

    def series_snapshots(
        self,
        root: str,
        start_exp: date,
        end_exp: date,
        provider: str = "schwab",
    ) -> list[dict[str, Any]]:
        """Return chronological series entries for expirations in [start_exp, end_exp]."""
        index = self.fetch_intraday_index(root, provider)
        series: list[dict[str, Any]] = index.get("option_chains", {}).get("series", [])
        return [
            entry
            for entry in series
            if start_exp <= date.fromisoformat(str(entry["expiration_date"])) <= end_exp
        ]

    def load_intraday_snapshot(
        self, uri: str, dtypes: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        """Load a single options snapshot CSV from an s3:// URI in the intraday bucket."""
        parsed = urlparse(uri)
        key = parsed.path.lstrip("/")
        effective_dtypes = dtypes or OPTIONS_DTYPES
        df = self._minio.get_csv(self._minio.intraday_bucket, key, effective_dtypes)
        if "expiration_date" in df.columns:
            df["expiration_date"] = pd.to_datetime(df["expiration_date"])
        return df

    # ------------------------------------------------------------------
    # Candles — intraday bucket only for now
    # ------------------------------------------------------------------

    def candles(
        self,
        provider: str,
        symbols: list[str],
        frequency: str = "1min",
        after: date | None = None,
        before: date | None = None,
    ) -> pd.DataFrame:
        """Load candle data for one or more symbols from the intraday bucket."""
        frames: list[pd.DataFrame] = []
        for symbol in symbols:
            df = self._load_intraday_candles(symbol, frequency, provider)
            if df is None:
                raise FileNotFoundError(f"No candle data for {symbol}/{frequency}/{provider}")
            if after is not None and "datetime" in df.columns:
                tz = df["datetime"].dt.tz
                after_ts = pd.Timestamp(after, tz="UTC") if tz is not None else pd.Timestamp(after)
                df = df[df["datetime"] >= after_ts]
            if before is not None and "datetime" in df.columns:
                tz = df["datetime"].dt.tz
                before_ts = (
                    pd.Timestamp(before, tz="UTC") if tz is not None else pd.Timestamp(before)
                )
                df = df[df["datetime"] <= before_ts]
            frames.append(df.reset_index(drop=True))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _lakehouse_parquet_key(root: str, sample_date: date, provider: str) -> str:
        return (
            f"options/{provider}"
            f"/{sample_date.year:04d}/{sample_date.month:02d}/{sample_date.day:02d}"
            f"/{root}_samples_{sample_date.isoformat()}.parquet"
        )

    @staticmethod
    def _configure_conn(
        conn: duckdb.DuckDBPyConnection, cfg: ReaderConfig
    ) -> duckdb.DuckDBPyConnection:
        """Configure DuckDB httpfs for direct s3:// reads from MinIO."""
        endpoint = cfg.minio_endpoint.removeprefix("http://").removeprefix("https://")
        use_ssl = cfg.minio_endpoint.startswith("https://")
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"SET s3_endpoint='{endpoint}'")
        conn.execute("SET s3_url_style='path'")
        conn.execute(f"SET s3_use_ssl={str(use_ssl).lower()}")
        conn.execute(f"SET s3_access_key_id='{cfg.minio_access_key}'")
        conn.execute(f"SET s3_secret_access_key='{cfg.minio_secret_key}'")
        return conn

    @staticmethod
    def _intraday_latest_files(index: dict[str, Any]) -> list[dict[str, Any]]:
        option_chains: dict[str, Any] = index.get("option_chains", {})
        latest: dict[str, Any] = option_chains.get("latest", {})
        return (  # type: ignore[no-any-return]
            latest.get("files")
            or option_chains.get("files")
            or index.get("intraday", {}).get("files", [])
        )

    def _list_latest_uris(
        self,
        root: str,
        start_exp: date,
        end_exp: date,
        provider: str,
    ) -> dict[date, str]:
        prefix = f"intraday/{provider}/options/latest/{root}_exp"
        try:
            keys = self._minio.list_keys(self._minio.intraday_bucket, prefix)
        except Exception as exc:
            logger.debug("_list_latest_uris(%s): %s", root, exc)
            return {}
        result: dict[date, str] = {}
        bucket = self._minio.intraday_bucket
        for key in keys:
            stem = key.rsplit("/", 1)[-1].removesuffix(".csv")
            parts = stem.split("_exp", 1)
            if len(parts) != 2:
                continue
            try:
                exp = date.fromisoformat(parts[1])
            except ValueError:
                continue
            if start_exp <= exp <= end_exp:
                result[exp] = f"s3://{bucket}/{key}"
        return result

    def _load_intraday_candles(
        self, symbol: str, frequency: str, provider: str
    ) -> pd.DataFrame | None:
        key = f"intraday/{provider}/candles/{frequency}/{symbol}.csv"
        try:
            df = self._minio.get_csv(self._minio.intraday_bucket, key)
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"])
            return df
        except Exception:
            return None
