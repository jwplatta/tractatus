"""Iceberg schema definition for the level_one table.

Covers both LEVELONE_EQUITIES and LEVELONE_FUTURES services. High-value
fields from the Schwab streaming ``extra_json`` are promoted to top-level
columns; the remaining ~30 sparse fields live in a nullable ``extra`` struct.
"""

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
    StructType,
)

from tractatus.catalog.schemas._types import TableDef

# Extra struct field IDs start at 200 to avoid collisions.
_EXTRA_STRUCT = StructType(
    # Shared (equities + futures)
    NestedField(field_id=201, name="f6", field_type=StringType(), required=False),
    NestedField(field_id=202, name="f7", field_type=StringType(), required=False),
    NestedField(field_id=203, name="f9", field_type=LongType(), required=False),
    NestedField(field_id=204, name="f12", field_type=DoubleType(), required=False),
    NestedField(field_id=205, name="f13", field_type=StringType(), required=False),
    NestedField(field_id=206, name="f14", field_type=BooleanType(), required=False),
    NestedField(field_id=207, name="f15", field_type=StringType(), required=False),
    NestedField(field_id=208, name="f16", field_type=StringType(), required=False),
    NestedField(field_id=209, name="f17", field_type=DoubleType(), required=False),
    NestedField(field_id=210, name="f18", field_type=DoubleType(), required=False),
    NestedField(field_id=211, name="f19", field_type=DoubleType(), required=False),
    NestedField(field_id=212, name="f20", field_type=DoubleType(), required=False),
    NestedField(field_id=213, name="f21", field_type=DoubleType(), required=False),
    NestedField(field_id=214, name="f22", field_type=DoubleType(), required=False),
    NestedField(field_id=215, name="f23", field_type=DoubleType(), required=False),
    NestedField(field_id=216, name="f25", field_type=StringType(), required=False),
    NestedField(field_id=217, name="f26", field_type=StringType(), required=False),
    NestedField(field_id=218, name="f27", field_type=BooleanType(), required=False),
    NestedField(field_id=219, name="f28", field_type=BooleanType(), required=False),
    NestedField(field_id=220, name="f29", field_type=DoubleType(), required=False),
    NestedField(field_id=221, name="f30", field_type=LongType(), required=False),
    NestedField(field_id=222, name="f31", field_type=DoubleType(), required=False),
    NestedField(field_id=223, name="f32", field_type=StringType(), required=False),
    NestedField(field_id=224, name="f39", field_type=StringType(), required=False),
    NestedField(field_id=225, name="f40", field_type=StringType(), required=False),
    # Equities-only
    NestedField(field_id=230, name="f10", field_type=DoubleType(), required=False),
    NestedField(field_id=231, name="f11", field_type=DoubleType(), required=False),
    NestedField(field_id=232, name="f24", field_type=LongType(), required=False),
    NestedField(field_id=233, name="f41", field_type=StringType(), required=False),
    NestedField(field_id=234, name="f42", field_type=DoubleType(), required=False),
    NestedField(field_id=235, name="f43", field_type=DoubleType(), required=False),
    NestedField(field_id=236, name="f46", field_type=LongType(), required=False),
    NestedField(field_id=237, name="f47", field_type=LongType(), required=False),
    NestedField(field_id=238, name="f48", field_type=LongType(), required=False),
    NestedField(field_id=239, name="f49", field_type=LongType(), required=False),
    NestedField(field_id=240, name="f50", field_type=LongType(), required=False),
    NestedField(field_id=241, name="f51", field_type=LongType(), required=False),
    # Futures-only
    NestedField(field_id=250, name="f33", field_type=StringType(), required=False),
    NestedField(field_id=251, name="f34", field_type=StringType(), required=False),
    NestedField(field_id=252, name="f35", field_type=StringType(), required=False),
)

# Top-level field IDs for promoted extra_json fields start at 100.
LEVEL_ONE_TABLE = TableDef(
    name="level_one",
    schema=Schema(
        NestedField(field_id=1, name="received_at_ms", field_type=LongType(), required=False),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
        NestedField(field_id=3, name="service", field_type=StringType(), required=False),
        NestedField(field_id=4, name="quote_time_ms", field_type=LongType(), required=False),
        NestedField(field_id=5, name="trade_time_ms", field_type=LongType(), required=False),
        NestedField(field_id=6, name="bid", field_type=DoubleType(), required=False),
        NestedField(field_id=7, name="ask", field_type=DoubleType(), required=False),
        NestedField(field_id=8, name="last", field_type=DoubleType(), required=False),
        NestedField(field_id=9, name="mark", field_type=DoubleType(), required=False),
        NestedField(field_id=10, name="bid_size", field_type=LongType(), required=False),
        NestedField(field_id=11, name="ask_size", field_type=LongType(), required=False),
        NestedField(field_id=12, name="volume", field_type=LongType(), required=False),
        NestedField(field_id=13, name="sample_date", field_type=DateType(), required=False),
        # Promoted from extra_json
        NestedField(field_id=100, name="quote_timestamp_ms", field_type=LongType(), required=False),
        NestedField(field_id=101, name="ask_timestamp_ms", field_type=LongType(), required=False),
        NestedField(field_id=102, name="trade_timestamp_ms", field_type=LongType(), required=False),
        NestedField(
            field_id=103, name="implied_volatility", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=104, name="delta", field_type=DoubleType(), required=False),
        NestedField(field_id=105, name="asset_main_type", field_type=StringType(), required=False),
        NestedField(field_id=106, name="asset_sub_type", field_type=StringType(), required=False),
        NestedField(field_id=107, name="cusip", field_type=StringType(), required=False),
        NestedField(field_id=108, name="delayed", field_type=BooleanType(), required=False),
        # Remaining extra_json fields
        NestedField(field_id=199, name="extra", field_type=_EXTRA_STRUCT, required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=13,
            field_id=1000,
            transform=MonthTransform(),
            name="sample_date_month",
        )
    ),
)

# Mapping from extra_json key → (promoted top-level column name | None).
# Keys not listed here go into the ``extra`` struct as ``f{key}``.
PROMOTED_FIELDS: dict[str, str] = {
    "36": "quote_timestamp_ms",
    "37": "ask_timestamp_ms",
    "38": "trade_timestamp_ms",
    "44": "implied_volatility",
    "45": "delta",
    "assetMainType": "asset_main_type",
    "assetSubType": "asset_sub_type",
    "cusip": "cusip",
    "delayed": "delayed",
}

# All extra_json keys that go into the struct, mapped to their struct field name.
EXTRA_STRUCT_FIELDS: dict[str, str] = {
    "6": "f6",
    "7": "f7",
    "9": "f9",
    "10": "f10",
    "11": "f11",
    "12": "f12",
    "13": "f13",
    "14": "f14",
    "15": "f15",
    "16": "f16",
    "17": "f17",
    "18": "f18",
    "19": "f19",
    "20": "f20",
    "21": "f21",
    "22": "f22",
    "23": "f23",
    "24": "f24",
    "25": "f25",
    "26": "f26",
    "27": "f27",
    "28": "f28",
    "29": "f29",
    "30": "f30",
    "31": "f31",
    "32": "f32",
    "33": "f33",
    "34": "f34",
    "35": "f35",
    "39": "f39",
    "40": "f40",
    "41": "f41",
    "42": "f42",
    "43": "f43",
    "46": "f46",
    "47": "f47",
    "48": "f48",
    "49": "f49",
    "50": "f50",
    "51": "f51",
}
