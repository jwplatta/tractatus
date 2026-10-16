"""Tests for per-table normalization (level_one, order_book, candles, fundamentals)."""

from __future__ import annotations

import datetime
import json

import pyarrow as pa
import pytest

from tractatus.catalog.normalize import normalize_table


class TestNormalizeLevelOne:
    def _make_table(self, n_rows: int = 2) -> pa.Table:
        extra_data = json.dumps(
            {
                "36": 1790349918161,
                "37": 1790349918162,
                "38": 1790349918163,
                "44": 1.61,
                "45": 0.217,
                "assetMainType": "EQUITY",
                "assetSubType": "ETF",
                "cusip": "78462F103",
                "delayed": False,
                "6": "Q",
                "9": 100,
            }
        )
        return pa.table(
            {
                "received_at_ms": pa.array([1000] * n_rows, type=pa.int64()),
                "symbol": pa.array(["SPY"] * n_rows, type=pa.string()),
                "service": pa.array(["LEVELONE_EQUITIES"] * n_rows, type=pa.string()),
                "quote_time_ms": pa.array([2000] * n_rows, type=pa.int64()),
                "trade_time_ms": pa.array([3000] * n_rows, type=pa.int64()),
                "bid": pa.array([400.0] * n_rows, type=pa.float64()),
                "ask": pa.array([401.0] * n_rows, type=pa.float64()),
                "last": pa.array([400.5] * n_rows, type=pa.float64()),
                "mark": pa.array([400.5] * n_rows, type=pa.float64()),
                "bid_size": pa.array([100] * n_rows, type=pa.int64()),
                "ask_size": pa.array([200] * n_rows, type=pa.int64()),
                "volume": pa.array([5000] * n_rows, type=pa.int64()),
                "extra_json": pa.array([extra_data] * n_rows, type=pa.string()),
            }
        )

    def test_promotes_fields(self) -> None:
        table = self._make_table()
        result, diags = normalize_table(table, "level_one", sample_date=datetime.date(2026, 10, 15))
        # Promoted timestamp fields are converted from epoch-ms to timestamptz
        qt = result.column("quote_timestamp")[0].as_py()
        assert isinstance(qt, datetime.datetime)
        assert qt.tzinfo is not None
        assert result.column("implied_volatility")[0].as_py() == pytest.approx(1.61)
        assert result.column("delta")[0].as_py() == pytest.approx(0.217)
        assert result.column("asset_main_type")[0].as_py() == "EQUITY"
        assert result.column("cusip")[0].as_py() == "78462F103"
        assert result.column("delayed")[0].as_py() is False

    def test_builds_extra_struct(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(table, "level_one", sample_date=datetime.date(2026, 10, 15))
        extra = result.column("extra")[0].as_py()
        assert extra["f6"] == "Q"
        assert extra["f9"] == 100

    def test_adds_sample_date(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(table, "level_one", sample_date=datetime.date(2026, 10, 15))
        assert result.column("sample_date")[0].as_py() == datetime.date(2026, 10, 15)

    def test_converts_ms_to_timestamps(self) -> None:
        table = self._make_table()
        result, diags = normalize_table(table, "level_one", sample_date=datetime.date(2026, 10, 15))
        # Source epoch-ms columns are converted and renamed
        assert "received_at_ms" not in result.column_names
        received = result.column("received_at")[0].as_py()
        assert isinstance(received, datetime.datetime)
        assert received.tzinfo is not None

    def test_null_extra_json(self) -> None:
        table = pa.table(
            {
                "received_at_ms": pa.array([1000], type=pa.int64()),
                "symbol": pa.array(["SPY"], type=pa.string()),
                "service": pa.array(["LEVELONE_EQUITIES"], type=pa.string()),
                "quote_time_ms": pa.array([2000], type=pa.int64()),
                "trade_time_ms": pa.array([3000], type=pa.int64()),
                "bid": pa.array([400.0], type=pa.float64()),
                "ask": pa.array([401.0], type=pa.float64()),
                "last": pa.array([400.5], type=pa.float64()),
                "mark": pa.array([400.5], type=pa.float64()),
                "bid_size": pa.array([100], type=pa.int64()),
                "ask_size": pa.array([200], type=pa.int64()),
                "volume": pa.array([5000], type=pa.int64()),
                "extra_json": pa.array([None], type=pa.string()),
            }
        )
        result, _ = normalize_table(table, "level_one", sample_date=datetime.date(2026, 10, 15))
        assert result.column("quote_timestamp")[0].as_py() is None
        assert result.column("extra")[0].as_py() is None


class TestNormalizeOrderBook:
    def _make_table(self) -> pa.Table:
        bids = json.dumps(
            [
                {
                    "0": 400.0,
                    "1": 100,
                    "2": 2,
                    "3": [
                        {"0": "NSDQ", "1": 60, "2": 50520163},
                        {"0": "ARCX", "1": 40, "2": 50520164},
                    ],
                },
            ]
        )
        asks = json.dumps(
            [
                {
                    "0": 401.0,
                    "1": 200,
                    "2": 1,
                    "3": [
                        {"0": "NSDQ", "1": 200, "2": 50520165},
                    ],
                },
            ]
        )
        return pa.table(
            {
                "received_at_ms": pa.array([1000], type=pa.int64()),
                "symbol": pa.array(["SPY"], type=pa.string()),
                "service": pa.array(["NASDAQ_BOOK"], type=pa.string()),
                "book_time_ms": pa.array([2000], type=pa.int64()),
                "bids_json": pa.array([bids], type=pa.string()),
                "asks_json": pa.array([asks], type=pa.string()),
            }
        )

    def test_parses_nested_struct(self) -> None:
        table = self._make_table()
        result, diags = normalize_table(
            table, "order_book", sample_date=datetime.date(2026, 10, 15)
        )
        bids = result.column("bids")[0].as_py()
        assert len(bids) == 1
        assert bids[0]["price"] == pytest.approx(400.0)
        assert bids[0]["size"] == 100
        assert bids[0]["num_orders"] == 2
        assert len(bids[0]["venues"]) == 2
        assert bids[0]["venues"][0]["venue"] == "NSDQ"

    def test_converts_ms_to_timestamps(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(table, "order_book", sample_date=datetime.date(2026, 10, 15))
        assert "received_at_ms" not in result.column_names
        assert "book_time_ms" not in result.column_names
        received = result.column("received_at")[0].as_py()
        assert isinstance(received, datetime.datetime)
        book_time = result.column("book_time")[0].as_py()
        assert isinstance(book_time, datetime.datetime)

    def test_adds_sample_date(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(table, "order_book", sample_date=datetime.date(2026, 10, 15))
        assert result.column("sample_date")[0].as_py() == datetime.date(2026, 10, 15)


class TestNormalizeCandles:
    def _make_table(self) -> pa.Table:
        return pa.table(
            {
                "datetime": pa.array(["2026-02-23T14:31:00Z"], type=pa.string()),
                "open": pa.array([400.0], type=pa.float64()),
                "high": pa.array([401.0], type=pa.float64()),
                "low": pa.array([399.0], type=pa.float64()),
                "close": pa.array([400.5], type=pa.float64()),
                "volume": pa.array([5000], type=pa.int64()),
            }
        )

    def test_injects_symbol_interval(self) -> None:
        table = self._make_table()
        result, diags = normalize_table(table, "candles", symbol="SPX", interval="5min")
        assert result.column("symbol")[0].as_py() == "SPX"
        assert result.column("interval")[0].as_py() == "5min"

    def test_casts_datetime(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(table, "candles", symbol="SPX", interval="5min")
        dt = result.column("datetime")[0].as_py()
        assert dt == datetime.datetime(2026, 2, 23, 14, 31, tzinfo=datetime.UTC)


class TestNormalizeFundamentals:
    def _make_table(self) -> pa.Table:
        return pa.table(
            {
                "symbol": pa.array(["AAPL"], type=pa.string()),
                "sample_date": pa.array(["2026-10-15"], type=pa.string()),
                "asset_type": pa.array(["EQUITY"], type=pa.string()),
                "exchange": pa.array(["XNAS"], type=pa.string()),
                "description": pa.array(["Apple Inc"], type=pa.string()),
                "cusip": pa.array(["037833100"], type=pa.string()),
                "pe_ratio": pa.array([25.0], type=pa.float64()),
                "dividend_date": pa.array(["2026-10-06 00:00:00.0"], type=pa.string()),
                "last_earnings_date": pa.array(["2026-09-30T16:00:00Z"], type=pa.string()),
                "fetched_at": pa.array(["2026-10-15T12:00:00Z"], type=pa.string()),
            }
        )

    def test_casts_date_columns(self) -> None:
        table = self._make_table()
        result, diags = normalize_table(table, "fundamentals")
        assert result.column("sample_date")[0].as_py() == datetime.date(2026, 10, 15)
        assert result.column("dividend_date")[0].as_py() == datetime.date(2026, 10, 6)

    def test_casts_timestamp_columns(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(table, "fundamentals")
        assert result.column("last_earnings_date")[0].as_py() == datetime.datetime(
            2026, 9, 30, 16, 0, tzinfo=datetime.UTC
        )
        assert result.column("fetched_at")[0].as_py() == datetime.datetime(
            2026, 10, 15, 12, 0, tzinfo=datetime.UTC
        )

    def test_adds_missing_columns(self) -> None:
        table = self._make_table()
        result, diags = normalize_table(table, "fundamentals")
        # Should have all 69 columns
        assert len(result.column_names) == 69
        # Missing columns should be null
        assert result.column("htb_quantity")[0].as_py() is None
