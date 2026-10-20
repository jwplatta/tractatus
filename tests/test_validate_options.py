"""Tests for the options sample quality validator."""

from __future__ import annotations

import pyarrow as pa
import pytest

from tractatus.catalog.validate.options import (
    GREEK_COLUMNS,
    GREEK_SENTINEL,
    THEORETICAL_SENTINEL,
    detect_greek_sentinels,
    detect_missing_ohlc,
    detect_theoretical_sentinel,
    validate_options,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_table(**cols: list) -> pa.Table:
    """Build a minimal Arrow table from keyword column arrays."""
    return pa.table({k: pa.array(v) for k, v in cols.items()})


def _greek_table(values: list[float | None]) -> pa.Table:
    """Table with all six Greek columns set to *values* (same list for each)."""
    return _make_table(**dict.fromkeys(GREEK_COLUMNS, values))


# ---------------------------------------------------------------------------
# detect_greek_sentinels
# ---------------------------------------------------------------------------


class TestDetectGreekSentinels:
    def test_sentinels_are_nulled(self) -> None:
        table = _greek_table([GREEK_SENTINEL, 0.5, GREEK_SENTINEL])
        out, hit = detect_greek_sentinels(table)
        assert hit.to_pylist() == [True, False, True]
        for col in GREEK_COLUMNS:
            vals = out.column(col).to_pylist()
            assert vals[0] is None
            assert vals[1] == pytest.approx(0.5)
            assert vals[2] is None

    def test_no_sentinels_no_change(self) -> None:
        table = _greek_table([0.1, -0.2, 0.3])
        out, hit = detect_greek_sentinels(table)
        assert hit.to_pylist() == [False, False, False]
        for col in GREEK_COLUMNS:
            assert out.column(col).to_pylist() == table.column(col).to_pylist()

    def test_missing_greek_columns_skipped(self) -> None:
        table = _make_table(delta=[GREEK_SENTINEL])
        out, hit = detect_greek_sentinels(table)
        assert hit.to_pylist() == [True]
        assert out.column("delta").to_pylist() == [None]

    def test_empty_table(self) -> None:
        table = _greek_table([])
        out, hit = detect_greek_sentinels(table)
        assert len(hit) == 0
        assert out.num_rows == 0


# ---------------------------------------------------------------------------
# detect_theoretical_sentinel
# ---------------------------------------------------------------------------


class TestDetectTheoreticalSentinel:
    def test_sentinel_nulled(self) -> None:
        table = _make_table(
            theoretical_option_value=[THEORETICAL_SENTINEL, 5.5, THEORETICAL_SENTINEL]
        )
        out, hit = detect_theoretical_sentinel(table)
        assert hit.to_pylist() == [True, False, True]
        vals = out.column("theoretical_option_value").to_pylist()
        assert vals[0] is None
        assert vals[1] == pytest.approx(5.5)
        assert vals[2] is None

    def test_no_sentinel(self) -> None:
        table = _make_table(theoretical_option_value=[3.0, 4.0])
        out, hit = detect_theoretical_sentinel(table)
        assert hit.to_pylist() == [False, False]

    def test_column_absent(self) -> None:
        table = _make_table(mark=[1.0])
        out, hit = detect_theoretical_sentinel(table)
        assert hit.to_pylist() == [False]
        assert out is table


# ---------------------------------------------------------------------------
# detect_missing_ohlc
# ---------------------------------------------------------------------------


class TestDetectMissingOhlc:
    def test_all_null_ohlc_flagged(self) -> None:
        table = _make_table(
            open=[None, 1.0, None],
            high=[None, 2.0, None],
            low=[None, 0.5, None],
            close=[None, 1.5, None],
        )
        out, hit = detect_missing_ohlc(table)
        assert hit.to_pylist() == [True, False, True]
        # Values unchanged
        assert out.column("open").to_pylist() == [None, 1.0, None]

    def test_partial_null_not_flagged(self) -> None:
        table = _make_table(open=[None], high=[2.0], low=[None], close=[None])
        _, hit = detect_missing_ohlc(table)
        assert hit.to_pylist() == [False]

    def test_no_ohlc_columns(self) -> None:
        table = _make_table(mark=[1.0, 2.0])
        out, hit = detect_missing_ohlc(table)
        assert hit.to_pylist() == [False, False]


# ---------------------------------------------------------------------------
# validate_options (integration)
# ---------------------------------------------------------------------------


def _full_table(n: int = 3) -> pa.Table:
    """Minimal table with all validator-relevant columns, all values clean."""
    return _make_table(
        **{col: [0.1] * n for col in GREEK_COLUMNS},
        theoretical_option_value=[2.5] * n,
        open=[100.0] * n,
        high=[105.0] * n,
        low=[99.0] * n,
        close=[102.0] * n,
    )


class TestValidateOptions:
    def test_clean_table_no_flags(self) -> None:
        table = _full_table(3)
        out, report = validate_options(table)
        assert report.rows_total == 3
        assert report.rows_degraded == 0
        assert report.flags_seen == set()
        assert out.column("sample_quality").to_pylist() == [None, None, None]
        assert out.column("quality_flags").to_pylist() == [None, None, None]

    def test_greek_sentinel_produces_degraded(self) -> None:
        table = _full_table(2)
        # Overwrite delta for row 0 with sentinel
        delta = [GREEK_SENTINEL, 0.1]
        table = table.set_column(table.column_names.index("delta"), "delta", pa.array(delta))
        out, report = validate_options(table)
        assert report.rows_degraded == 1
        assert "greeks_sentinel" in report.flags_seen
        sq = out.column("sample_quality").to_pylist()
        qf = out.column("quality_flags").to_pylist()
        assert sq[0] == "degraded"
        assert sq[1] is None
        assert "greeks_sentinel" in qf[0]
        assert qf[1] is None
        # Sentinel nulled
        assert out.column("delta").to_pylist()[0] is None

    def test_multiple_flags_combined(self) -> None:
        # Row 0: greek sentinel + theoretical sentinel + missing ohlc
        table = _make_table(
            **{col: [GREEK_SENTINEL, 0.1] for col in GREEK_COLUMNS},
            theoretical_option_value=[THEORETICAL_SENTINEL, 2.5],
            open=[None, 100.0],
            high=[None, 105.0],
            low=[None, 99.0],
            close=[None, 102.0],
        )
        out, report = validate_options(table)
        assert report.rows_degraded == 1
        assert report.flags_seen == {"greeks_sentinel", "theoretical_sentinel", "ohlc_missing"}
        qf = out.column("quality_flags").to_pylist()[0]
        assert "greeks_sentinel" in qf
        assert "theoretical_sentinel" in qf
        assert "ohlc_missing" in qf

    def test_quality_columns_appended_to_end(self) -> None:
        table = _full_table(1)
        out, _ = validate_options(table)
        assert out.column_names[-2] == "sample_quality"
        assert out.column_names[-1] == "quality_flags"

    def test_quality_report_counts(self) -> None:
        table = _make_table(
            delta=[GREEK_SENTINEL, GREEK_SENTINEL, 0.1, GREEK_SENTINEL],
            gamma=[GREEK_SENTINEL, GREEK_SENTINEL, 0.2, 0.1],
            theta=[0.0] * 4,
            vega=[0.0] * 4,
            rho=[0.0] * 4,
            volatility=[0.0] * 4,
            theoretical_option_value=[2.0] * 4,
            open=[1.0] * 4,
            high=[2.0] * 4,
            low=[0.5] * 4,
            close=[1.5] * 4,
        )
        _, report = validate_options(table)
        assert report.rows_total == 4
        assert report.rows_degraded == 3
