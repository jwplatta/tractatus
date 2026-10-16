"""Iceberg schema definition for the options table."""

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

OPTIONS_TABLE = TableDef(
    name="options",
    schema=Schema(
        NestedField(field_id=1, name="contract_type", field_type=StringType(), required=False),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
        NestedField(field_id=3, name="description", field_type=StringType(), required=False),
        NestedField(field_id=4, name="strike", field_type=DoubleType(), required=False),
        NestedField(field_id=5, name="expiration_date", field_type=DateType(), required=False),
        NestedField(field_id=6, name="open", field_type=DoubleType(), required=False),
        NestedField(field_id=7, name="high", field_type=DoubleType(), required=False),
        NestedField(field_id=8, name="low", field_type=DoubleType(), required=False),
        NestedField(field_id=9, name="close", field_type=DoubleType(), required=False),
        NestedField(field_id=10, name="mark", field_type=DoubleType(), required=False),
        NestedField(field_id=11, name="bid", field_type=DoubleType(), required=False),
        NestedField(field_id=12, name="bid_size", field_type=LongType(), required=False),
        NestedField(field_id=13, name="ask", field_type=DoubleType(), required=False),
        NestedField(field_id=14, name="ask_size", field_type=LongType(), required=False),
        NestedField(field_id=15, name="last", field_type=DoubleType(), required=False),
        NestedField(field_id=16, name="last_size", field_type=DoubleType(), required=False),
        NestedField(field_id=17, name="open_interest", field_type=DoubleType(), required=False),
        NestedField(field_id=18, name="total_volume", field_type=DoubleType(), required=False),
        NestedField(field_id=19, name="transactions", field_type=LongType(), required=False),
        NestedField(field_id=20, name="delta", field_type=DoubleType(), required=False),
        NestedField(field_id=21, name="gamma", field_type=DoubleType(), required=False),
        NestedField(field_id=22, name="theta", field_type=DoubleType(), required=False),
        NestedField(field_id=23, name="vega", field_type=DoubleType(), required=False),
        NestedField(field_id=24, name="rho", field_type=DoubleType(), required=False),
        NestedField(field_id=25, name="volatility", field_type=DoubleType(), required=False),
        NestedField(
            field_id=26, name="theoretical_volatility", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=27, name="theoretical_option_value", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=28, name="intrinsic_value", field_type=DoubleType(), required=False),
        NestedField(field_id=29, name="extrinsic_value", field_type=DoubleType(), required=False),
        NestedField(field_id=30, name="underlying_price", field_type=DoubleType(), required=False),
        NestedField(field_id=31, name="sampled_at", field_type=TimestamptzType(), required=False),
        NestedField(field_id=32, name="trade_at", field_type=TimestamptzType(), required=False),
        NestedField(field_id=33, name="quote_at", field_type=TimestamptzType(), required=False),
        NestedField(field_id=34, name="time_value", field_type=DoubleType(), required=False),
        NestedField(field_id=35, name="net_change", field_type=DoubleType(), required=False),
        NestedField(field_id=36, name="percent_change", field_type=DoubleType(), required=False),
        NestedField(field_id=37, name="settlement_type", field_type=StringType(), required=False),
        NestedField(field_id=38, name="exercise_type", field_type=StringType(), required=False),
        NestedField(field_id=39, name="expiration_type", field_type=StringType(), required=False),
        NestedField(field_id=40, name="multiplier", field_type=DoubleType(), required=False),
        NestedField(field_id=41, name="non_standard", field_type=BooleanType(), required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=31,
            field_id=1000,
            transform=MonthTransform(),
            name="sampled_at_month",
        )
    ),
)
