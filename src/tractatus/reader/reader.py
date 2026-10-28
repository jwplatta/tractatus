"""Reader — unified façade for reading market data from MinIO and Iceberg."""

from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta
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


def _next_day(d: date) -> date:
    return d + timedelta(days=1)


class Reader:
    """Read market data from the lakehouse.

    MinIO historical bucket is the authoritative source for options and candles.
    Iceberg (via Glue) is the authoritative source for level_one, order_book, and fundamentals.
    Intraday MinIO bucket serves live/current data.
    """

    def __init__(
        self,
        cfg: ReaderConfig | None = None,
        conn: duckdb.DuckDBPyConnection | None = None,
    ) -> None:
        self._cfg = cfg or ReaderConfig.load()
        self._minio = MinIOStore(self._cfg)
        self._iceberg = IcebergStore(self._cfg)
        self._query = OptionsQueryClient(conn)

    @property
    def options_query(self) -> OptionsQueryClient:
        return self._query

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
    # Historical options — reads from Iceberg lakehouse
    # ------------------------------------------------------------------

    def list_option_sample_dates(self, root: str, provider: str = "schwab") -> list[date]:
        """List available sample dates for a root.

        Checks tractatus-lakehouse (pre-exported parquet cache) first via key listing.
        Falls back to scanning the Iceberg options table if the cache has nothing.
        """
        prefix = f"options/{provider}/"
        try:
            keys = self._minio.list_keys(self._minio.lakehouse_bucket, prefix)
            dates: set[date] = set()
            for key in keys:
                if f"/{root}_samples_" not in key:
                    continue
                m = _DATE_DIR_RE.search(key)
                if m:
                    dates.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
            if dates:
                return sorted(dates)
        except Exception as exc:
            logger.debug("lakehouse key listing failed for %s/%s: %s", root, provider, exc)

        # Fall back to Iceberg scan
        row_filter = f"option_root = '{root}' AND provider = '{provider}'"
        arrow = self._iceberg.scan(
            "options", row_filter=row_filter, selected_fields=("sampled_at",)
        )
        if arrow.num_rows == 0:
            return []
        dates = set()
        for ts in arrow.column("sampled_at").to_pylist():
            if ts is not None:
                dates.add(ts.date() if hasattr(ts, "date") else ts)
        return sorted(dates)

    def get_option_parquet(
        self,
        root: str,
        sample_date: date,
        provider: str = "schwab",
    ) -> Path:
        """Return local path to a historical options parquet for one root/date.

        Checks local filesystem cache, then tractatus-lakehouse, then Iceberg.
        Raises FileNotFoundError if not found in any source.
        """
        local = self._local_parquet_path(root, sample_date, provider)
        if local.exists():
            return local

        # Try tractatus-lakehouse (pre-exported flat parquet)
        key = self._lakehouse_parquet_key(root, sample_date, provider)
        if self._minio.head_object(self._minio.lakehouse_bucket, key):
            local.parent.mkdir(parents=True, exist_ok=True)
            data = self._minio.get_object(self._minio.lakehouse_bucket, key)
            local.write_bytes(data)
            return local

        # Fall back to Iceberg
        row_filter = (
            f"option_root = '{root}' AND provider = '{provider}'"
            f" AND sampled_at >= '{sample_date.isoformat()}'"
            f" AND sampled_at < '{_next_day(sample_date).isoformat()}'"
        )
        arrow = self._iceberg.scan("options", row_filter=row_filter)
        if arrow.num_rows == 0:
            raise FileNotFoundError(
                f"No options data for {root}/{sample_date} (provider={provider})"
            )
        local.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(arrow, local)
        return local

    def ensure_option_parquets(
        self,
        root: str,
        after: date,
        before: date,
        provider: str = "schwab",
    ) -> list[str]:
        """Return local parquet paths for all available dates in [after, before].

        Downloads from Iceberg into local cache as needed.
        """
        paths: list[str] = []
        for d in self.list_option_sample_dates(root, provider):
            if after <= d <= before:
                try:
                    p = self.get_option_parquet(root, d, provider)
                    paths.append(str(p))
                except FileNotFoundError:
                    logger.warning("No Iceberg data for %s/%s — skipping", root, d)
        return paths

    # ------------------------------------------------------------------
    # High-level read methods
    # ------------------------------------------------------------------

    def options(
        self,
        provider: str,
        after: date,
        before: date,
        roots: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load historical options data for one or more roots across a date range.

        Returns a concatenated DataFrame. Uses local parquet cache backed by MinIO.
        """
        if roots is None:
            roots = self._list_option_roots(provider)
        frames: list[pd.DataFrame] = []
        for root in roots:
            paths = self.ensure_option_parquets(root, after, before, provider)
            if not paths:
                continue
            frames.append(pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True))
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def candles(
        self,
        provider: str,
        symbols: list[str],
        frequency: str = "1min",
        after: date | None = None,
        before: date | None = None,
    ) -> pd.DataFrame:
        """Load candle data for one or more symbols.

        Reads from the MinIO historical bucket first, falls back to the intraday bucket.
        Raises FileNotFoundError if no data is found for a symbol.
        """
        frames: list[pd.DataFrame] = []
        for symbol in symbols:
            df = self._load_historical_candles(symbol, frequency, provider)
            if df is None:
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

    def level_one(
        self,
        provider: str,
        after: date,
        before: date,
        symbols: list[str] | None = None,
        table: str = "level_one_equities",
    ) -> pd.DataFrame:
        """Load level-one quote data from Iceberg."""
        return self._iceberg_read(table, provider, after, before, symbols)

    def order_book(
        self,
        provider: str,
        after: date,
        before: date,
        symbols: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load order book data from Iceberg."""
        return self._iceberg_read("order_book", provider, after, before, symbols)

    def fundamentals(
        self,
        provider: str,
        after: date,
        before: date,
        symbols: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load fundamentals data from Iceberg."""
        return self._iceberg_read("fundamentals", provider, after, before, symbols)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _list_option_roots(self, provider: str) -> list[str]:
        arrow = self._iceberg.scan(
            "options",
            row_filter=f"provider = '{provider}'",
            selected_fields=("option_root",),
        )
        if arrow.num_rows == 0:
            return []
        roots: set[str] = set(arrow.column("option_root").to_pylist())
        roots.discard(None)
        return sorted(roots)  # type: ignore[type-var]

    def _iceberg_read(
        self,
        table: str,
        provider: str,
        after: date,
        before: date,
        symbols: list[str] | None,
    ) -> pd.DataFrame:
        parts = [
            f"provider = '{provider}'",
            f"sample_date >= '{after.isoformat()}'",
            f"sample_date <= '{before.isoformat()}'",
        ]
        if symbols:
            quoted = ", ".join(f"'{s}'" for s in symbols)
            parts.append(f"symbol IN ({quoted})")
        row_filter = " AND ".join(parts)
        return self._iceberg.scan_to_dataframe(table, row_filter=row_filter)

    @staticmethod
    def _lakehouse_parquet_key(root: str, sample_date: date, provider: str) -> str:
        return (
            f"options/{provider}"
            f"/{sample_date.year:04d}/{sample_date.month:02d}/{sample_date.day:02d}"
            f"/{root}_samples_{sample_date.isoformat()}.parquet"
        )

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

    def _load_historical_candles(
        self, symbol: str, frequency: str, provider: str
    ) -> pd.DataFrame | None:
        # TODO: read from Iceberg candles table once data is published there
        return None

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
