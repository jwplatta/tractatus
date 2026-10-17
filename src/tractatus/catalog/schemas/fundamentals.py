"""Iceberg schema definition for the fundamentals table."""

from __future__ import annotations

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from tractatus.catalog.schemas._types import TableDef

FUNDAMENTALS_TABLE = TableDef(
    name="fundamentals",
    schema=Schema(
        NestedField(field_id=1, name="symbol", field_type=StringType(), required=False),
        NestedField(field_id=2, name="sample_date", field_type=DateType(), required=False),
        NestedField(field_id=3, name="asset_type", field_type=StringType(), required=False),
        NestedField(field_id=4, name="exchange", field_type=StringType(), required=False),
        NestedField(field_id=5, name="description", field_type=StringType(), required=False),
        NestedField(field_id=6, name="cusip", field_type=StringType(), required=False),
        NestedField(field_id=7, name="pe_ratio", field_type=DoubleType(), required=False),
        NestedField(field_id=8, name="peg_ratio", field_type=DoubleType(), required=False),
        NestedField(field_id=9, name="pb_ratio", field_type=DoubleType(), required=False),
        NestedField(field_id=10, name="pr_ratio", field_type=DoubleType(), required=False),
        NestedField(field_id=11, name="pcf_ratio", field_type=DoubleType(), required=False),
        NestedField(field_id=12, name="gross_margin_ttm", field_type=DoubleType(), required=False),
        NestedField(field_id=13, name="gross_margin_mrq", field_type=DoubleType(), required=False),
        NestedField(
            field_id=14, name="net_profit_margin_ttm", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=15, name="net_profit_margin_mrq", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=16, name="operating_margin_ttm", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=17, name="operating_margin_mrq", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=18, name="return_on_equity", field_type=DoubleType(), required=False),
        NestedField(field_id=19, name="return_on_assets", field_type=DoubleType(), required=False),
        NestedField(
            field_id=20, name="return_on_investment", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=21, name="quick_ratio", field_type=DoubleType(), required=False),
        NestedField(field_id=22, name="current_ratio", field_type=DoubleType(), required=False),
        NestedField(field_id=23, name="interest_coverage", field_type=DoubleType(), required=False),
        NestedField(
            field_id=24, name="total_debt_to_capital", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=25, name="lt_debt_to_equity", field_type=DoubleType(), required=False),
        NestedField(
            field_id=26, name="total_debt_to_equity", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=27, name="eps_ttm", field_type=DoubleType(), required=False),
        NestedField(
            field_id=28, name="eps_change_percent_ttm", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=29, name="eps_change", field_type=DoubleType(), required=False),
        NestedField(field_id=30, name="eps_change_year", field_type=DoubleType(), required=False),
        NestedField(field_id=31, name="eps", field_type=DoubleType(), required=False),
        NestedField(field_id=32, name="rev_change_year", field_type=DoubleType(), required=False),
        NestedField(field_id=33, name="rev_change_ttm", field_type=DoubleType(), required=False),
        NestedField(field_id=34, name="rev_change_in", field_type=DoubleType(), required=False),
        NestedField(
            field_id=35, name="shares_outstanding", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=36, name="market_cap_float", field_type=DoubleType(), required=False),
        NestedField(field_id=37, name="market_cap", field_type=DoubleType(), required=False),
        NestedField(
            field_id=38, name="book_value_per_share", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=39, name="short_int_to_float", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=40, name="short_int_day_to_cover", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=41, name="fund_leverage_factor", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=42, name="is_hard_to_borrow", field_type=BooleanType(), required=False
        ),
        NestedField(field_id=43, name="is_shortable", field_type=BooleanType(), required=False),
        NestedField(field_id=44, name="htb_quantity", field_type=LongType(), required=False),
        NestedField(field_id=45, name="htb_rate", field_type=DoubleType(), required=False),
        NestedField(field_id=46, name="dividend_amount", field_type=DoubleType(), required=False),
        NestedField(field_id=47, name="dividend_yield", field_type=DoubleType(), required=False),
        NestedField(field_id=48, name="dividend_date", field_type=DateType(), required=False),
        NestedField(field_id=49, name="dividend_pay_date", field_type=DateType(), required=False),
        NestedField(
            field_id=50, name="dividend_pay_amount", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=51, name="declaration_date", field_type=DateType(), required=False),
        NestedField(field_id=52, name="next_div_ex_date", field_type=DateType(), required=False),
        NestedField(field_id=53, name="next_div_pay_date", field_type=DateType(), required=False),
        NestedField(
            field_id=54, name="div_growth_rate_3year", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=55, name="dividend_freq", field_type=LongType(), required=False),
        NestedField(field_id=56, name="beta", field_type=DoubleType(), required=False),
        NestedField(field_id=57, name="high_52", field_type=DoubleType(), required=False),
        NestedField(field_id=58, name="low_52", field_type=DoubleType(), required=False),
        NestedField(field_id=59, name="close_price", field_type=DoubleType(), required=False),
        NestedField(field_id=60, name="last_price", field_type=DoubleType(), required=False),
        NestedField(field_id=61, name="net_change", field_type=DoubleType(), required=False),
        NestedField(
            field_id=62, name="net_percent_change", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=63, name="total_volume", field_type=LongType(), required=False),
        NestedField(field_id=64, name="avg_10_days_volume", field_type=LongType(), required=False),
        NestedField(field_id=65, name="avg_1_day_volume", field_type=LongType(), required=False),
        NestedField(field_id=66, name="avg_3_month_volume", field_type=LongType(), required=False),
        NestedField(field_id=67, name="avg_1_year_volume", field_type=LongType(), required=False),
        NestedField(
            field_id=68, name="last_earnings_date", field_type=TimestamptzType(), required=False
        ),
        NestedField(field_id=69, name="fetched_at", field_type=TimestamptzType(), required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            transform=MonthTransform(),
            name="sample_date_month",
        )
    ),
)

# Date string columns that need casting during normalization.
DATE_COLUMNS: list[str] = [
    "sample_date",
    "dividend_date",
    "dividend_pay_date",
    "declaration_date",
    "next_div_ex_date",
    "next_div_pay_date",
]

TIMESTAMP_COLUMNS: list[str] = [
    "last_earnings_date",
    "fetched_at",
]
