"""Tests for per-table normalization (level_one, order_book, candles, fundamentals, economic_events)."""

from __future__ import annotations

import datetime
import json

import pyarrow as pa
import pytest

from tractatus.catalog.normalize import normalize_table


class TestNormalizeLevelOneEquities:
    def _make_table(self, n_rows: int = 2) -> pa.Table:
        extra_data = json.dumps(
            {
                "36": 1790349918161,
                "37": 1790349918162,
                "38": 1790349918163,
                "44": -2.325,
                "45": -0.81456,
                "assetMainType": "EQUITY",
                "assetSubType": "ETF",
                "cusip": "78462F103",
                "delayed": False,
                "6": "Q",
                "9": 100,
                "18": 1.5,
                "unknownKey": "preserved",
            }
        )
        return pa.table(
            {
                "job_type": pa.array(["level_one"] * n_rows, type=pa.string()),
                "provider": pa.array(["schwab"] * n_rows, type=pa.string()),
                "received_at": pa.array(["2026-10-18T14:30:00Z"] * n_rows, type=pa.string()),
                "symbol": pa.array(["SPY"] * n_rows, type=pa.string()),
                "service": pa.array(["LEVELONE_EQUITIES"] * n_rows, type=pa.string()),
                "quote_time": pa.array(["2026-10-18T14:30:00Z"] * n_rows, type=pa.string()),
                "trade_time": pa.array(["2026-10-18T14:29:59Z"] * n_rows, type=pa.string()),
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

    def test_unpacks_fields_with_proper_names(self) -> None:
        table = self._make_table()
        result, diags = normalize_table(
            table, "level_one_equities", sample_date=datetime.date(2026, 10, 15)
        )
        assert result.column("ask_id")[0].as_py() == "Q"
        assert result.column("last_size")[0].as_py() == 100
        assert result.column("net_change")[0].as_py() == pytest.approx(1.5)
        assert result.column("mark_change")[0].as_py() == pytest.approx(-2.325)
        assert result.column("mark_change_percent")[0].as_py() == pytest.approx(-0.81456)
        assert result.column("asset_main_type")[0].as_py() == "EQUITY"
        assert result.column("asset_sub_type")[0].as_py() == "ETF"
        assert result.column("cusip")[0].as_py() == "78462F103"
        assert result.column("delayed")[0].as_py() is False

    def test_converts_timestamp_fields(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(
            table, "level_one_equities", sample_date=datetime.date(2026, 10, 15)
        )
        # Source string columns converted to timestamptz
        received = result.column("received_at")[0].as_py()
        assert isinstance(received, datetime.datetime)
        assert received.tzinfo is not None
        # Extra_json timestamp fields also converted
        rmt = result.column("regular_market_trade_time")[0].as_py()
        assert isinstance(rmt, datetime.datetime)
        assert rmt.tzinfo is not None

    def test_strips_metadata_fields(self) -> None:
        table = self._make_table()
        result, diags = normalize_table(
            table, "level_one_equities", sample_date=datetime.date(2026, 10, 15)
        )
        assert "job_type" not in result.column_names
        assert "provider" not in result.column_names
        assert any("Stripped metadata" in d for d in diags)

    def test_adds_sample_date(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(
            table, "level_one_equities", sample_date=datetime.date(2026, 10, 15)
        )
        assert result.column("sample_date")[0].as_py() == datetime.date(2026, 10, 15)

    def test_preserves_unmapped_keys_in_extra_json(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(
            table, "level_one_equities", sample_date=datetime.date(2026, 10, 15)
        )
        residual = json.loads(result.column("extra_json")[0].as_py())
        assert residual == {"unknownKey": "preserved"}

    def test_no_duplicate_columns_for_tickrake_extracted_fields(self) -> None:
        """Fields 33, 34, 35 are extracted by tickrake to mark, quote_time, trade_time.
        They should NOT produce separate mark_price/quote_time_millis/trade_time_millis columns."""
        table = self._make_table()
        result, _ = normalize_table(
            table, "level_one_equities", sample_date=datetime.date(2026, 10, 15)
        )
        assert "mark_price" not in result.column_names
        assert "quote_time_millis" not in result.column_names
        assert "trade_time_millis" not in result.column_names
        # But the top-level mark and converted timestamps should exist
        assert "mark" in result.column_names
        assert "quote_time" in result.column_names
        assert "trade_time" in result.column_names

    def test_null_extra_json(self) -> None:
        table = pa.table(
            {
                "received_at": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
                "symbol": pa.array(["SPY"], type=pa.string()),
                "service": pa.array(["LEVELONE_EQUITIES"], type=pa.string()),
                "quote_time": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
                "trade_time": pa.array(["2026-10-18T14:29:59Z"], type=pa.string()),
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
        result, _ = normalize_table(
            table, "level_one_equities", sample_date=datetime.date(2026, 10, 15)
        )
        assert result.column("ask_id")[0].as_py() is None
        assert result.column("mark_change")[0].as_py() is None


class TestNormalizeLevelOneFutures:
    def _make_table(self) -> pa.Table:
        extra_data = json.dumps(
            {
                "6": "K",
                "9": 50,
                "19": -36.0,
                "assetMainType": "FUTURE",
                "delayed": False,
                "unknownFuturesKey": 42,
            }
        )
        return pa.table(
            {
                "job_type": pa.array(["level_one"], type=pa.string()),
                "provider": pa.array(["schwab"], type=pa.string()),
                "received_at": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
                "symbol": pa.array(["^ES"], type=pa.string()),
                "service": pa.array(["LEVELONE_FUTURES"], type=pa.string()),
                "quote_time": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
                "trade_time": pa.array(["2026-10-18T14:29:59Z"], type=pa.string()),
                "bid": pa.array([5600.0], type=pa.float64()),
                "ask": pa.array([5601.0], type=pa.float64()),
                "last": pa.array([5600.5], type=pa.float64()),
                "mark": pa.array([5600.5], type=pa.float64()),
                "bid_size": pa.array([100], type=pa.int64()),
                "ask_size": pa.array([200], type=pa.int64()),
                "volume": pa.array([50000], type=pa.int64()),
                "extra_json": pa.array([extra_data], type=pa.string()),
            }
        )

    def test_unpacks_futures_field_names(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(
            table, "level_one_futures", sample_date=datetime.date(2026, 10, 15)
        )
        assert result.column("ask_id")[0].as_py() == "K"
        assert result.column("last_size")[0].as_py() == 50
        assert result.column("net_change")[0].as_py() == pytest.approx(-36.0)
        assert result.column("asset_main_type")[0].as_py() == "FUTURE"
        assert result.column("delayed")[0].as_py() is False

    def test_no_duplicate_columns_for_tickrake_extracted_fields(self) -> None:
        """Fields 10, 11, 24 are extracted by tickrake to quote_time, trade_time, mark."""
        table = self._make_table()
        result, _ = normalize_table(
            table, "level_one_futures", sample_date=datetime.date(2026, 10, 15)
        )
        assert "quote_time_millis" not in result.column_names
        assert "trade_time_millis" not in result.column_names
        assert "mark_price" not in result.column_names

    def test_preserves_unmapped_keys_in_extra_json(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(
            table, "level_one_futures", sample_date=datetime.date(2026, 10, 15)
        )
        residual = json.loads(result.column("extra_json")[0].as_py())
        assert residual == {"unknownFuturesKey": 42}


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
                "job_type": pa.array(["order_book"], type=pa.string()),
                "provider": pa.array(["schwab"], type=pa.string()),
                "received_at": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
                "symbol": pa.array(["SPY"], type=pa.string()),
                "service": pa.array(["NASDAQ_BOOK"], type=pa.string()),
                "book_time": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
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

    def test_converts_string_to_timestamps(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(table, "order_book", sample_date=datetime.date(2026, 10, 15))
        received = result.column("received_at")[0].as_py()
        assert isinstance(received, datetime.datetime)
        assert received.tzinfo is not None
        book_time = result.column("book_time")[0].as_py()
        assert isinstance(book_time, datetime.datetime)
        assert book_time.tzinfo is not None

    def test_strips_metadata_fields(self) -> None:
        table = self._make_table()
        result, diags = normalize_table(
            table, "order_book", sample_date=datetime.date(2026, 10, 15)
        )
        assert "job_type" not in result.column_names
        assert "provider" not in result.column_names

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


class TestNormalizeEconomicEvents:
    def _make_table(self) -> pa.Table:
        return pa.table(
            {
                "event_datetime": pa.array(["2026-10-14T08:30:00Z"], type=pa.string()),
                "event_name": pa.array(["CPI"], type=pa.string()),
                "category": pa.array(["economic"], type=pa.string()),
                "source": pa.array(["bls"], type=pa.string()),
                "actual": pa.array([3.2], type=pa.float64()),
                "estimate": pa.array([3.1], type=pa.float64()),
                "previous": pa.array([3.0], type=pa.float64()),
                "unit": pa.array(["percent"], type=pa.string()),
                "symbol": pa.array([None], type=pa.string()),
                "company_name": pa.array([None], type=pa.string()),
                "time_of_day": pa.array([None], type=pa.string()),
                "release_id": pa.array([53], type=pa.int64()),
                "series_id": pa.array(["CPIAUCSL"], type=pa.string()),
                "sampled_at": pa.array(["2026-10-14T12:00:00Z"], type=pa.string()),
            }
        )

    def test_casts_timestamp_columns(self) -> None:
        table = self._make_table()
        result, diags = normalize_table(table, "economic_events")
        assert result.column("event_datetime")[0].as_py() == datetime.datetime(
            2026, 10, 14, 8, 30, tzinfo=datetime.UTC
        )
        assert result.column("sampled_at")[0].as_py() == datetime.datetime(
            2026, 10, 14, 12, 0, tzinfo=datetime.UTC
        )

    def test_preserves_all_fields(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(table, "economic_events")
        assert result.column("event_name")[0].as_py() == "CPI"
        assert result.column("category")[0].as_py() == "economic"
        assert result.column("source")[0].as_py() == "bls"
        assert result.column("actual")[0].as_py() == pytest.approx(3.2)
        assert result.column("release_id")[0].as_py() == 53

    def test_adds_missing_columns(self) -> None:
        table = self._make_table()
        result, _ = normalize_table(table, "economic_events")
        assert len(result.column_names) == 14


class TestInputSchemaValidation:
    """Tests for schema-on-read validation across all table types."""

    def test_level_one_missing_column_raises(self) -> None:
        table = pa.table(
            {
                "received_at": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
                # "symbol" intentionally missing
                "service": pa.array(["LEVELONE_EQUITIES"], type=pa.string()),
                "quote_time": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
                "trade_time": pa.array(["2026-10-18T14:29:59Z"], type=pa.string()),
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
        with pytest.raises(ValueError, match="Missing required columns: symbol"):
            normalize_table(table, "level_one_equities", sample_date=datetime.date(2026, 10, 15))

    def test_level_one_wrong_type_raises(self) -> None:
        table = pa.table(
            {
                "received_at": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
                "symbol": pa.array(["SPY"], type=pa.string()),
                "service": pa.array(["LEVELONE_EQUITIES"], type=pa.string()),
                "quote_time": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
                "trade_time": pa.array(["2026-10-18T14:29:59Z"], type=pa.string()),
                "bid": pa.array(["400.0"], type=pa.string()),  # wrong type
                "ask": pa.array([401.0], type=pa.float64()),
                "last": pa.array([400.5], type=pa.float64()),
                "mark": pa.array([400.5], type=pa.float64()),
                "bid_size": pa.array([100], type=pa.int64()),
                "ask_size": pa.array([200], type=pa.int64()),
                "volume": pa.array([5000], type=pa.int64()),
                "extra_json": pa.array([None], type=pa.string()),
            }
        )
        with pytest.raises(ValueError, match="Column bid: expected double, got string"):
            normalize_table(table, "level_one_equities", sample_date=datetime.date(2026, 10, 15))

    def test_order_book_missing_column_raises(self) -> None:
        table = pa.table(
            {
                "received_at": pa.array(["2026-10-18T14:30:00Z"], type=pa.string()),
                "symbol": pa.array(["SPY"], type=pa.string()),
                "service": pa.array(["NASDAQ_BOOK"], type=pa.string()),
                # "book_time" intentionally missing
                "bids_json": pa.array(["[]"], type=pa.string()),
                "asks_json": pa.array(["[]"], type=pa.string()),
            }
        )
        with pytest.raises(ValueError, match="Missing required columns: book_time"):
            normalize_table(table, "order_book", sample_date=datetime.date(2026, 10, 15))

    def test_candles_missing_column_raises(self) -> None:
        table = pa.table(
            {
                "datetime": pa.array(["2026-02-23T14:31:00Z"], type=pa.string()),
                "open": pa.array([400.0], type=pa.float64()),
                "high": pa.array([401.0], type=pa.float64()),
                "low": pa.array([399.0], type=pa.float64()),
                # "close" intentionally missing
                "volume": pa.array([5000], type=pa.int64()),
            }
        )
        with pytest.raises(ValueError, match="Missing required columns: close"):
            normalize_table(table, "candles", symbol="SPX", interval="5min")

    def test_fundamentals_missing_column_raises(self) -> None:
        table = pa.table(
            {
                # "symbol" intentionally missing
                "sample_date": pa.array(["2026-10-15"], type=pa.string()),
            }
        )
        with pytest.raises(ValueError, match="Missing required columns: symbol"):
            normalize_table(table, "fundamentals")

    def test_economic_events_missing_column_raises(self) -> None:
        table = pa.table(
            {
                # "event_name" intentionally missing
                "category": pa.array(["economic"], type=pa.string()),
                "source": pa.array(["bls"], type=pa.string()),
                "event_datetime": pa.array(["2026-10-14T08:30:00Z"], type=pa.string()),
                "sampled_at": pa.array(["2026-10-14T12:00:00Z"], type=pa.string()),
            }
        )
        with pytest.raises(ValueError, match="Missing required columns: event_name"):
            normalize_table(table, "economic_events")
