"""DuckDB-based parquet query client for historical options data."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

# Type alias: callers may pass a glob string or an explicit list of file paths.
ParquetSource = str | list[str] | list[Path]


def _read_parquet_expr(source: ParquetSource) -> str:
    """Return a DuckDB ``read_parquet(...)`` expression for *source*.

    *source* may be a glob string (``'path/**/*.parquet'``) or a list of
    concrete file paths.  DuckDB accepts both forms natively.
    """
    if isinstance(source, str):
        return f"read_parquet('{source}', union_by_name=true)"
    paths = ", ".join(f"'{p}'" for p in source)
    return f"read_parquet([{paths}], union_by_name=true)"


# Columns required by lookback CTEs for partitioning and ordering.
_LOOKBACK_REQUIRED = {"sampled_at", "expiration_date", "strike", "contract_type"}


def _cte_columns(columns: list[str] | None) -> str:
    """Build the SELECT list for a lookback CTE.

    When *columns* is ``None`` returns ``*``.  Otherwise returns the union of
    the requested columns and the columns required for windowing, so the
    CTE can partition/order correctly and the final SELECT can project only
    what the caller asked for.
    """
    if columns is None:
        return "*"
    merged = dict.fromkeys([*columns, *_LOOKBACK_REQUIRED])
    return ", ".join(merged)


OPTIONS_DTYPES: dict[str, Any] = {
    "strike": "float64",
    "open_interest": "float64",
    "gamma": "float64",
    "delta": "float64",
    "theta": "float64",
    "vega": "float64",
    "theoretical_volatility": "float64",
    "underlying_price": "float64",
    "volatility": "float64",
    "mark": "float64",
    "bid": "float64",
    "ask": "float64",
    "last": "float64",
    "last_size": "float64",
    "total_volume": "float64",
}


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Cast option column dtypes, parse expiration_date, uppercase contract_type."""
    df = df.astype({col: dtype for col, dtype in OPTIONS_DTYPES.items() if col in df.columns})
    if "expiration_date" in df.columns:
        df["expiration_date"] = pd.to_datetime(df["expiration_date"])
    if "contract_type" in df.columns:
        df["contract_type"] = df["contract_type"].str.upper()
    return df


class OptionsQueryClient:
    """Run DuckDB queries against archived options parquet files.

    Parameters
    ----------
    conn:
        An existing DuckDB connection to use. Pass a pre-configured persistent
        connection from the application for best performance. When *None* an
        in-memory connection is opened (useful for tests).
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection | None = None) -> None:
        self._conn: duckdb.DuckDBPyConnection = (
            conn if conn is not None else duckdb.connect(":memory:")
        )

    # ------------------------------------------------------------------
    # Point queries
    # ------------------------------------------------------------------

    def list_snapshot_times(self, parquet_path: Path, expiry: date) -> list[datetime]:
        """Return sorted distinct sampled_at datetimes for *expiry* in *parquet_path*."""
        rows = self._conn.execute(
            "SELECT DISTINCT sampled_at FROM read_parquet(?, union_by_name=true)"
            " WHERE expiration_date = ? ORDER BY sampled_at",
            [str(parquet_path), expiry.isoformat()],
        ).fetchall()
        return [datetime.fromisoformat(str(r[0])) for r in rows]

    def load_snapshot(
        self,
        parquet_path: Path,
        expiry: date,
        sampled_at: datetime,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load a single snapshot for one expiry/sampled_at from *parquet_path*.

        Parameters
        ----------
        parquet_path : Path
            Local path to a cached options parquet file (from ``Reader.get_option_parquet``).
        expiry : date
            Expiration date to filter on.
        sampled_at : datetime
            Exact snapshot timestamp to retrieve.
        columns : list[str] | None
            Column names to return.  ``None`` (default) returns all columns.

            >>> q.load_snapshot(path, exp, ts, columns=["strike", "bid", "ask"])
        """
        cols = ", ".join(columns) if columns else "*"
        df = self._conn.execute(
            f"SELECT {cols} FROM read_parquet(?, union_by_name=true)"
            " WHERE expiration_date = ?"
            " AND CAST(sampled_at AS TIMESTAMPTZ) = CAST(? AS TIMESTAMPTZ)",
            [str(parquet_path), expiry.isoformat(), sampled_at.isoformat()],
        ).df()
        return _normalize_df(df)

    def load_expiry(
        self,
        parquet_path: Path,
        expiry: date,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load all snapshots for *expiry* from *parquet_path*, ordered by sampled_at.

        Parameters
        ----------
        parquet_path : Path
            Local path to a cached options parquet file.
        expiry : date
            Expiration date to filter on.
        columns : list[str] | None
            Column names to return.  ``None`` (default) returns all columns.

            >>> q.load_expiry(path, exp, columns=["strike", "volatility", "open_interest"])
        """
        cols = ", ".join(columns) if columns else "*"
        df = self._conn.execute(
            f"SELECT {cols} FROM read_parquet(?) WHERE expiration_date = ? ORDER BY sampled_at",
            [str(parquet_path), expiry.isoformat()],
        ).df()
        return _normalize_df(df)

    # ------------------------------------------------------------------
    # Window / aggregate queries
    # ------------------------------------------------------------------

    def latest_window(
        self,
        parquet_path: Path,
        start_date: date,
        end_date: date,
    ) -> list[tuple[date, datetime]]:
        """Return (expiry, max_sampled_at) for each expiry in [start_date, end_date]."""
        rows = self._conn.execute(
            "SELECT expiration_date, MAX(sampled_at) FROM read_parquet(?, union_by_name=true)"
            " WHERE expiration_date BETWEEN ? AND ?"
            " GROUP BY expiration_date",
            [str(parquet_path), start_date.isoformat(), end_date.isoformat()],
        ).fetchall()
        return [(date.fromisoformat(str(r[0])), datetime.fromisoformat(str(r[1]))) for r in rows]

    def list_expirations(self, parquet_path: Path, min_date: date) -> list[date]:
        """Return distinct expiration dates >= *min_date* from *parquet_path*."""
        rows = self._conn.execute(
            "SELECT DISTINCT expiration_date FROM read_parquet(?, union_by_name=true)"
            " WHERE expiration_date >= ? ORDER BY expiration_date",
            [str(parquet_path), min_date.isoformat()],
        ).fetchall()
        return [date.fromisoformat(str(r[0])) for r in rows]

    # ------------------------------------------------------------------
    # Multi-parquet downsampled lookback queries
    # ------------------------------------------------------------------

    def load_lookback(
        self,
        parquet_glob: ParquetSource,
        expiry_range: tuple[date, date],
        interval_minutes: int,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load time-bucketed downsampled data across parquets filtered by expiry range.

        Parameters
        ----------
        parquet_glob : str | list[str] | list[Path]
            A glob pattern (``'cache/**/*.parquet'``) or an explicit list of file paths.
            Prefer passing a file list from ``Reader.ensure_option_parquets`` to avoid
            scanning unrelated parquets.
        expiry_range : tuple[date, date]
            ``(start, end)`` expiration date range (inclusive).
        interval_minutes : int
            Downsample bucket width in minutes.  Within each bucket the most recent
            snapshot per (expiry, strike, contract_type) is kept.
        columns : list[str] | None
            Column names to return.  ``None`` (default) returns all columns.
            Partition columns (``sampled_at``, ``expiration_date``, ``strike``,
            ``contract_type``) are always read internally but only appear in the
            output when requested or when ``columns`` is ``None``.

            >>> files = reader.ensure_parquet_files("SPXW", start, end)
            >>> q.load_lookback(files, (exp_start, exp_end), 30,
            ...     columns=["strike", "bid", "ask", "volatility"])
        """
        start_str = expiry_range[0].isoformat()
        end_str = expiry_range[1].isoformat()
        src = _read_parquet_expr(parquet_glob)
        # The CTE needs partition/order columns; the final SELECT returns only requested cols.
        cte_cols = _cte_columns(columns)
        final_cols = ", ".join(columns) if columns else "* EXCLUDE (interval_bucket, rn)"
        query = f"""
            WITH bucketed AS (
                SELECT {cte_cols},
                    epoch_ms(
                        CAST(floor(epoch_ms(sampled_at) / ({interval_minutes} * 60000))
                        * ({interval_minutes} * 60000) AS BIGINT)
                    ) AS interval_bucket
                FROM {src}
                WHERE expiration_date BETWEEN '{start_str}' AND '{end_str}'
            ),
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY interval_bucket, expiration_date, strike, contract_type
                        ORDER BY sampled_at DESC
                    ) AS rn
                FROM bucketed
            )
            SELECT {final_cols}
            FROM ranked
            WHERE rn = 1
            ORDER BY sampled_at, expiration_date
        """
        return _normalize_df(self._conn.execute(query).df())

    def load_sample_window(
        self,
        parquet_glob: ParquetSource,
        sample_start: date,
        interval_minutes: int,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load downsampled data across parquets filtered by sample timestamp >= sample_start.

        Parameters
        ----------
        parquet_glob : str | list[str] | list[Path]
            A glob pattern or explicit file list (see ``load_lookback``).
        sample_start : date
            Only include rows with ``sampled_at >= sample_start``.
        interval_minutes : int
            Downsample bucket width in minutes.
        columns : list[str] | None
            Column names to return.  ``None`` (default) returns all columns.

            >>> q.load_sample_window(files, start, 30,
            ...     columns=["strike", "volatility", "underlying_price"])
        """
        start_str = sample_start.isoformat()
        src = _read_parquet_expr(parquet_glob)
        cte_cols = _cte_columns(columns)
        final_cols = ", ".join(columns) if columns else "* EXCLUDE (interval_bucket, rn)"
        query = f"""
            WITH bucketed AS (
                SELECT {cte_cols},
                    epoch_ms(
                        CAST(floor(epoch_ms(sampled_at) / ({interval_minutes} * 60000))
                        * ({interval_minutes} * 60000) AS BIGINT)
                    ) AS interval_bucket
                FROM {src}
                WHERE CAST(sampled_at AS TIMESTAMPTZ) >= TIMESTAMPTZ '{start_str}'
            ),
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY interval_bucket, expiration_date, strike, contract_type
                        ORDER BY sampled_at DESC
                    ) AS rn
                FROM bucketed
            )
            SELECT {final_cols}
            FROM ranked
            WHERE rn = 1
            ORDER BY sampled_at, expiration_date
        """
        return _normalize_df(self._conn.execute(query).df())

    def load_expiry_lookback(
        self,
        parquet_glob: ParquetSource,
        expiry: date,
        interval_minutes: int,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
        """Load time-bucketed downsampled data for a single expiry across parquets.

        Parameters
        ----------
        parquet_glob : str | list[str] | list[Path]
            A glob pattern or explicit file list (see ``load_lookback``).
        expiry : date
            Single expiration date to filter on.
        interval_minutes : int
            Downsample bucket width in minutes.
        columns : list[str] | None
            Column names to return.  ``None`` (default) returns all columns.

            >>> q.load_expiry_lookback(files, exp, 30,
            ...     columns=["strike", "bid", "ask", "delta", "gamma"])
        """
        expiry_str = expiry.isoformat()
        src = _read_parquet_expr(parquet_glob)
        cte_cols = _cte_columns(columns)
        final_cols = ", ".join(columns) if columns else "* EXCLUDE (interval_bucket, rn)"
        query = f"""
            WITH bucketed AS (
                SELECT {cte_cols},
                    epoch_ms(
                        CAST(floor(epoch_ms(sampled_at) / ({interval_minutes} * 60000))
                        * ({interval_minutes} * 60000) AS BIGINT)
                    ) AS interval_bucket
                FROM {src}
                WHERE expiration_date = '{expiry_str}'
            ),
            ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY interval_bucket, strike, contract_type
                        ORDER BY sampled_at DESC
                    ) AS rn
                FROM bucketed
            )
            SELECT {final_cols}
            FROM ranked
            WHERE rn = 1
            ORDER BY sampled_at
        """
        return _normalize_df(self._conn.execute(query).df())
