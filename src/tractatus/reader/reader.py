"""DataReader — unified facade for reading market data from MinIO and Iceberg."""

from __future__ import annotations

import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import duckdb
import pandas as pd
import pyarrow.parquet as pq

from tractatus.reader.config import ReaderConfig
from tractatus.reader.iceberg_store import IcebergStore
from tractatus.reader.minio_store import MinIOStore
from tractatus.tickrake.options.queries import OPTIONS_DTYPES, OptionsQueryClient

logger = logging.getLogger(__name__)

_DATE_DIR_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})")


class DataReader:
    """Read market data from MinIO (intraday + historical) with Iceberg fallback."""

    def __init__(
        self,
        cfg: ReaderConfig | None = None,
        query_conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self._cfg = cfg or ReaderConfig.load()
        self._minio = MinIOStore(self._cfg)
        self._iceberg = IcebergStore(self._cfg)
        self._query = OptionsQueryClient(query_conn)

    @property
    def options_query(self) -> OptionsQueryClient:
        return self._query

    # ------------------------------------------------------------------
    # Options — intraday (tickrake bucket)
    # ------------------------------------------------------------------

    def list_roots(self, provider: str = "schwab") -> list[str]:
        """List option roots available in the intraday MinIO index."""
        prefix = f"intraday/{provider}/"
        try:
            keys = self._minio.list_keys(self._minio.intraday_bucket, prefix)
        except Exception as exc:
            logger.debug("list_roots(%s): %s", provider, exc)
            return []
        roots: list[str] = []
        for key in keys:
            if key.endswith(".json"):
                stem = key[len(prefix) :].removesuffix(".json")
                if stem and "/" not in stem:
                    roots.append(stem)
        return sorted(roots)

    def fetch_intraday_index(self, root: str, provider: str = "schwab") -> dict[str, Any]:
        """Fetch the intraday index JSON for a root from MinIO."""
        key = f"intraday/{provider}/{root}.json"
        return self._minio.get_json(self._minio.intraday_bucket, key)

    def intraday_updated_at(self, root: str, provider: str = "schwab") -> datetime | None:
        """Return the updated_at timestamp from the intraday index, or None."""
        index = self.fetch_intraday_index(root, provider)
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
        """Return {expiry: s3_uri} for expirations in [start_exp, end_exp].

        Tries the latest/ prefix listing first, falls back to the index JSON.
        """
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
    # Options — historical (tickrake-historical bucket, Iceberg fallback)
    # ------------------------------------------------------------------

    def list_sample_dates(self, root: str, provider: str = "schwab") -> list[date]:
        """List available sample dates for a root from the historical manifest."""
        manifest = self._minio.get_json(self._minio.historical_bucket, "manifests/options.json")
        dates: set[date] = set()
        for f in manifest.get("files", []):
            key: str = f.get("key", "")
            if key.startswith(f"options/{provider}/") and f"/{root}_samples_" in key:
                m = _DATE_DIR_RE.search(key)
                if m:
                    dates.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
        if not dates:
            dates = self._list_sample_dates_from_keys(root, provider)
        return sorted(dates)

    def get_parquet(
        self,
        root: str,
        sample_date: date,
        provider: str = "schwab",
    ) -> Path | None:
        """Resolve a historical options parquet to a local cached path.

        Resolution order:
        1. Local cache
        2. tickrake-historical MinIO bucket
        3. Iceberg lakehouse (with cache-through to MinIO + local)
        """
        local_path = self._local_parquet_path(root, sample_date, provider)
        if local_path.exists():
            return local_path

        minio_key = self._historical_parquet_key(root, sample_date, provider)
        if self._try_download_from_minio(minio_key, local_path):
            return local_path

        if self._try_fetch_from_iceberg(root, sample_date, provider, local_path):
            self._cache_to_minio(minio_key, local_path)
            return local_path

        return None

    def ensure_parquet_range(
        self,
        root: str,
        start_date: date,
        end_date: date,
        provider: str = "schwab",
    ) -> str:
        """Ensure parquets for a date range are cached locally. Returns a glob pattern."""
        dates = self.list_sample_dates(root, provider)
        for d in dates:
            if start_date <= d <= end_date:
                self.get_parquet(root, d, provider)
        cache_base = self._cfg.cache_dir / "options" / provider
        return str(cache_base / "**" / f"{root}_samples_*.parquet")

    # ------------------------------------------------------------------
    # Candles
    # ------------------------------------------------------------------

    def load_candles(
        self,
        symbol: str,
        frequency: str,
        provider: str = "schwab",
        start: date | None = None,
        end: date | None = None,
    ) -> pd.DataFrame:
        """Load candle data, trying intraday bucket first then historical."""
        df = self._try_load_intraday_candles(symbol, frequency, provider)
        if df is None:
            df = self._try_load_historical_candles(symbol, frequency, provider)
        if df is None:
            df = self._try_load_iceberg_candles(symbol, frequency, provider)
        if df is None:
            raise FileNotFoundError(f"No candle data for {symbol}/{frequency}/{provider}")

        if "datetime" in df.columns:
            tz = df["datetime"].dt.tz
            if start is not None:
                start_ts = pd.Timestamp(start, tz="UTC") if tz is not None else pd.Timestamp(start)
                df = df[df["datetime"] >= start_ts]
            if end is not None:
                end_ts = pd.Timestamp(end, tz="UTC") if tz is not None else pd.Timestamp(end)
                df = df[df["datetime"] <= end_ts]
        return df.reset_index(drop=True)

    def list_candle_frequencies(self, symbol: str, provider: str = "schwab") -> list[str]:
        """Return candle frequencies available for a symbol from the intraday index."""
        index = self.fetch_intraday_index(symbol, provider)
        candles = index.get("candles", {})
        files: list[dict[str, Any]] = candles.get("files", [])
        return sorted({str(f["frequency"]) for f in files})

    # ------------------------------------------------------------------
    # Internal — intraday helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _intraday_latest_files(index: dict[str, Any]) -> list[dict[str, Any]]:
        option_chains: dict[str, Any] = index.get("option_chains", {})
        latest: dict[str, Any] = option_chains.get("latest", {})
        files: list[dict[str, Any]] = (
            latest.get("files")
            or option_chains.get("files")
            or index.get("intraday", {}).get("files", [])
        )
        return files

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

    # ------------------------------------------------------------------
    # Internal — historical helpers
    # ------------------------------------------------------------------

    def _local_parquet_path(self, root: str, sample_date: date, provider: str) -> Path:
        return (
            self._cfg.cache_dir
            / "options"
            / provider
            / f"{sample_date.year:04d}"
            / f"{sample_date.month:02d}"
            / f"{sample_date.day:02d}"
            / f"{root}_samples_{sample_date.isoformat()}.parquet"
        )

    @staticmethod
    def _historical_parquet_key(root: str, sample_date: date, provider: str) -> str:
        return (
            f"options/{provider}"
            f"/{sample_date.year:04d}/{sample_date.month:02d}/{sample_date.day:02d}"
            f"/{root}_samples_{sample_date.isoformat()}.parquet"
        )

    def _try_download_from_minio(self, key: str, local_path: Path) -> bool:
        try:
            if not self._minio.head_object(self._minio.historical_bucket, key):
                return False
            self._minio.download_to_local(self._minio.historical_bucket, key, local_path)
            return True
        except Exception as exc:
            logger.debug("_try_download_from_minio(%s): %s", key, exc)
            return False

    def _try_fetch_from_iceberg(
        self,
        root: str,
        sample_date: date,
        provider: str,
        local_path: Path,
    ) -> bool:
        if not self._cfg.iceberg_bucket:
            return False
        try:
            row_filter = (
                f"symbol == '{root}'"
                f" AND provider == '{provider}'"
                f" AND sampled_at >= '{sample_date.isoformat()}T00:00:00+00:00'"
                f" AND sampled_at <= '{sample_date.isoformat()}T23:59:59+00:00'"
            )
            arrow_table = self._iceberg.scan("options", row_filter=row_filter)
            if arrow_table.num_rows == 0:
                return False
            local_path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(arrow_table, local_path)
            return True
        except Exception as exc:
            logger.debug("_try_fetch_from_iceberg(%s, %s): %s", root, sample_date, exc)
            return False

    def _cache_to_minio(self, key: str, local_path: Path) -> None:
        try:
            self._minio.upload_from_local(self._minio.historical_bucket, key, local_path)
        except Exception as exc:
            logger.debug("_cache_to_minio(%s): %s", key, exc)

    def _list_sample_dates_from_keys(self, root: str, provider: str) -> set[date]:
        prefix = f"options/{provider}/"
        try:
            keys = self._minio.list_keys(self._minio.historical_bucket, prefix)
        except Exception:
            return set()
        dates: set[date] = set()
        for key in keys:
            if f"/{root}_samples_" in key:
                m = _DATE_DIR_RE.search(key)
                if m:
                    dates.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
        return dates

    # ------------------------------------------------------------------
    # Internal — candle helpers
    # ------------------------------------------------------------------

    def _try_load_intraday_candles(
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

    def _try_load_historical_candles(
        self, symbol: str, frequency: str, provider: str
    ) -> pd.DataFrame | None:
        """Try loading candle parquet from tickrake-historical bucket."""
        prefix = f"candles/{provider}/{frequency}/"
        try:
            keys = self._minio.list_keys(self._minio.historical_bucket, prefix)
        except Exception:
            return None
        matching = [k for k in keys if f"/{symbol}.parquet" in k]
        if not matching:
            return None
        frames: list[pd.DataFrame] = []
        for key in sorted(matching):
            local = self._cfg.cache_dir / key
            if not local.exists():
                try:
                    self._minio.download_to_local(self._minio.historical_bucket, key, local)
                except Exception:
                    continue
            frames.append(pd.read_parquet(local))
        if not frames:
            return None
        df = pd.concat(frames, ignore_index=True)
        if "datetime" in df.columns:
            df["datetime"] = pd.to_datetime(df["datetime"])
            df = df.sort_values("datetime").reset_index(drop=True)
        return df

    def _try_load_iceberg_candles(
        self, symbol: str, frequency: str, provider: str
    ) -> pd.DataFrame | None:
        if not self._cfg.iceberg_bucket:
            return None
        try:
            row_filter = (
                f"symbol == '{symbol}' AND provider == '{provider}' AND interval == '{frequency}'"
            )
            df = self._iceberg.scan_to_dataframe("candles", row_filter=row_filter)
            if df.empty:
                return None
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"])
                df = df.sort_values("datetime").reset_index(drop=True)
            return df
        except Exception as exc:
            logger.debug("_try_load_iceberg_candles(%s, %s): %s", symbol, frequency, exc)
            return None
