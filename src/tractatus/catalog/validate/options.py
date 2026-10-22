"""Options chain sample validators.

Each validator is a pure function: (pa.Table) -> tuple[pa.Table, pa.Array].
The returned Array is a boolean mask — True means the row triggered this validator.

Sentinel constants are Schwab-specific; define them here so they are easy to update.
"""

from __future__ import annotations

from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc

# ---------------------------------------------------------------------------
# Schwab sentinel constants
# ---------------------------------------------------------------------------

GREEK_SENTINEL: float = -999.0
THEORETICAL_SENTINEL: float = -1.0

GREEK_COLUMNS: tuple[str, ...] = ("delta", "gamma", "theta", "vega", "rho", "volatility")
OHLC_COLUMNS: tuple[str, ...] = ("open", "high", "low", "close")


# ---------------------------------------------------------------------------
# Quality report
# ---------------------------------------------------------------------------


@dataclass
class QualityReport:
    """Aggregate quality stats for a single file."""

    rows_total: int
    rows_degraded: int
    flags_seen: set[str]


# ---------------------------------------------------------------------------
# Validators
# ---------------------------------------------------------------------------


def detect_greek_sentinels(table: pa.Table) -> tuple[pa.Table, pa.Array]:
    """Null out Greek/volatility sentinel values (-999.0) and return a hit mask."""
    n = table.num_rows
    hit: pa.Array = pa.array([False] * n, type=pa.bool_())

    for col_name in GREEK_COLUMNS:
        if col_name not in table.column_names:
            continue
        col = table.column(col_name)
        is_sentinel = pc.equal(col, GREEK_SENTINEL)
        # Nullify sentinel cells
        nulled = pc.if_else(is_sentinel, None, col)
        table = table.set_column(table.column_names.index(col_name), col_name, nulled)
        hit = pc.or_(hit, is_sentinel)

    return table, hit


def detect_theoretical_sentinel(table: pa.Table) -> tuple[pa.Table, pa.Array]:
    """Null out theoretical_option_value sentinel (-1.0) and return a hit mask."""
    col_name = "theoretical_option_value"
    n = table.num_rows
    hit: pa.Array = pa.array([False] * n, type=pa.bool_())

    if col_name not in table.column_names:
        return table, hit

    col = table.column(col_name)
    is_sentinel = pc.equal(col, THEORETICAL_SENTINEL)
    nulled = pc.if_else(is_sentinel, None, col)
    table = table.set_column(table.column_names.index(col_name), col_name, nulled)
    hit = is_sentinel

    return table, hit


def detect_missing_ohlc(table: pa.Table) -> tuple[pa.Table, pa.Array]:
    """Flag rows where all OHLC columns are null. Values are not modified."""
    n = table.num_rows
    all_null: pa.Array = pa.array([True] * n, type=pa.bool_())

    present = [c for c in OHLC_COLUMNS if c in table.column_names]
    if not present:
        return table, pa.array([False] * n, type=pa.bool_())

    for col_name in present:
        all_null = pc.and_(all_null, pc.is_null(table.column(col_name)))

    return table, all_null


# ---------------------------------------------------------------------------
# Validator chain
# ---------------------------------------------------------------------------

_VALIDATORS: list[tuple[str, object]] = [
    ("greeks_sentinel", detect_greek_sentinels),
    ("theoretical_sentinel", detect_theoretical_sentinel),
    ("ohlc_missing", detect_missing_ohlc),
]


def validate_options(table: pa.Table) -> tuple[pa.Table, QualityReport]:
    """Run all validators and append sample_quality / quality_flags columns.

    Returns the annotated table and a QualityReport with aggregate stats.
    ``sample_quality`` is NULL for clean rows, ``"degraded"`` otherwise.
    ``quality_flags`` is NULL for clean rows, comma-joined flag names otherwise.
    """
    n = table.num_rows
    # per-row accumulated flags: list[set[str]]
    row_flags: list[set[str]] = [set() for _ in range(n)]

    for flag_name, validator in _VALIDATORS:
        table, hit_mask = validator(table)  # type: ignore[operator]
        for i, hit in enumerate(hit_mask.to_pylist()):
            if hit:
                row_flags[i].add(flag_name)

    # Build quality columns
    quality_vals: list[str | None] = []
    flags_vals: list[str | None] = []
    for flags in row_flags:
        if flags:
            quality_vals.append("degraded")
            flags_vals.append(",".join(sorted(flags)))
        else:
            quality_vals.append(None)
            flags_vals.append(None)

    for col_name, values in (("sample_quality", quality_vals), ("quality_flags", flags_vals)):
        arr = pa.array(values, type=pa.string())
        if col_name in table.column_names:
            table = table.set_column(table.column_names.index(col_name), col_name, arr)
        else:
            table = table.append_column(pa.field(col_name, pa.string()), arr)

    rows_degraded = sum(1 for f in row_flags if f)
    flags_seen = {f for flags in row_flags for f in flags}

    return table, QualityReport(
        rows_total=n,
        rows_degraded=rows_degraded,
        flags_seen=flags_seen,
    )
