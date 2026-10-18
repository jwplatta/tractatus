"""Iceberg schema definition for the level_one_futures table.

All columns are flat — no nested structs. Field names come from the Schwab
streaming API LEVELONE_FUTURES service definition.
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
    TimestamptzType,
)

from tractatus.catalog.schemas._types import TableDef

LEVEL_ONE_FUTURES_TABLE = TableDef(
    name="level_one_futures",
    schema=Schema(
        # --- Source parquet columns (from tickrake common extraction) ---
        NestedField(field_id=1, name="received_at", field_type=TimestamptzType(), required=False),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
        NestedField(field_id=3, name="service", field_type=StringType(), required=False),
        NestedField(field_id=4, name="quote_time", field_type=TimestamptzType(), required=False),
        NestedField(field_id=5, name="trade_time", field_type=TimestamptzType(), required=False),
        NestedField(field_id=6, name="bid", field_type=DoubleType(), required=False),
        NestedField(field_id=7, name="ask", field_type=DoubleType(), required=False),
        NestedField(field_id=8, name="last", field_type=DoubleType(), required=False),
        NestedField(field_id=9, name="mark", field_type=DoubleType(), required=False),
        NestedField(field_id=10, name="bid_size", field_type=LongType(), required=False),
        NestedField(field_id=11, name="ask_size", field_type=LongType(), required=False),
        NestedField(field_id=12, name="volume", field_type=LongType(), required=False),
        NestedField(field_id=13, name="sample_date", field_type=DateType(), required=False),
        # --- Metadata (from extra_json) ---
        NestedField(field_id=14, name="asset_main_type", field_type=StringType(), required=False),
        NestedField(field_id=15, name="delayed", field_type=BooleanType(), required=False),
        # --- Schwab field 6-40 (LEVELONE_FUTURES numbering) ---
        NestedField(field_id=100, name="ask_id", field_type=StringType(), required=False),
        NestedField(field_id=101, name="bid_id", field_type=StringType(), required=False),
        NestedField(field_id=102, name="last_size", field_type=LongType(), required=False),
        # Fields 10, 11 are extracted by tickrake to top-level quote_time_ms, trade_time_ms.
        NestedField(field_id=105, name="high_price", field_type=DoubleType(), required=False),
        NestedField(field_id=106, name="low_price", field_type=DoubleType(), required=False),
        NestedField(field_id=107, name="close_price", field_type=DoubleType(), required=False),
        NestedField(field_id=108, name="exchange_id", field_type=StringType(), required=False),
        NestedField(field_id=109, name="description", field_type=StringType(), required=False),
        NestedField(field_id=110, name="last_id", field_type=StringType(), required=False),
        NestedField(field_id=111, name="open_price", field_type=DoubleType(), required=False),
        NestedField(field_id=112, name="net_change", field_type=DoubleType(), required=False),
        NestedField(
            field_id=113, name="future_percent_change", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=114, name="exchange_name", field_type=StringType(), required=False),
        NestedField(field_id=115, name="security_status", field_type=StringType(), required=False),
        NestedField(field_id=116, name="open_interest", field_type=LongType(), required=False),
        # Field 24 is extracted by tickrake to top-level mark.
        NestedField(field_id=118, name="tick", field_type=DoubleType(), required=False),
        NestedField(field_id=119, name="tick_amount", field_type=DoubleType(), required=False),
        NestedField(field_id=120, name="product", field_type=StringType(), required=False),
        NestedField(
            field_id=121, name="future_price_format", field_type=StringType(), required=False
        ),
        NestedField(
            field_id=122, name="future_trading_hours", field_type=StringType(), required=False
        ),
        NestedField(
            field_id=123, name="future_is_tradable", field_type=BooleanType(), required=False
        ),
        NestedField(
            field_id=124, name="future_multiplier", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=125, name="future_is_active", field_type=BooleanType(), required=False
        ),
        NestedField(
            field_id=126,
            name="future_settlement_price",
            field_type=DoubleType(),
            required=False,
        ),
        NestedField(
            field_id=127, name="future_active_symbol", field_type=StringType(), required=False
        ),
        NestedField(
            field_id=128,
            name="future_expiration_date",
            field_type=StringType(),
            required=False,
        ),
        NestedField(field_id=129, name="expiration_style", field_type=StringType(), required=False),
        NestedField(field_id=130, name="ask_mic_id", field_type=StringType(), required=False),
        NestedField(field_id=131, name="bid_mic_id", field_type=StringType(), required=False),
        NestedField(field_id=132, name="last_mic_id", field_type=StringType(), required=False),
        NestedField(field_id=133, name="settlement_date", field_type=StringType(), required=False),
        # Residual extra_json — unmapped keys preserved as raw JSON string.
        NestedField(field_id=200, name="extra_json", field_type=StringType(), required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=13,
            field_id=1000,
            transform=MonthTransform(),
            name="sample_date_month",
        )
    ),
    sort_keys=["received_at", "quote_time", "trade_time"],
    properties={
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "3",
        "write.parquet.row-group-limit": "150000",
    },
)

# Mapping from extra_json key → Iceberg column name.
EXTRA_JSON_FIELDS: dict[str, str] = {
    "6": "ask_id",
    "7": "bid_id",
    "9": "last_size",
    # 10, 11 are extracted by tickrake to top-level quote_time_ms, trade_time_ms.
    "12": "high_price",
    "13": "low_price",
    "14": "close_price",
    "15": "exchange_id",
    "16": "description",
    "17": "last_id",
    "18": "open_price",
    "19": "net_change",
    "20": "future_percent_change",
    "21": "exchange_name",
    "22": "security_status",
    "23": "open_interest",
    # 24 is extracted by tickrake to top-level mark.
    "25": "tick",
    "26": "tick_amount",
    "27": "product",
    "28": "future_price_format",
    "29": "future_trading_hours",
    "30": "future_is_tradable",
    "31": "future_multiplier",
    "32": "future_is_active",
    "33": "future_settlement_price",
    "34": "future_active_symbol",
    "35": "future_expiration_date",
    "36": "expiration_style",
    "37": "ask_mic_id",
    "38": "bid_mic_id",
    "39": "last_mic_id",
    "40": "settlement_date",
    "assetMainType": "asset_main_type",
    "delayed": "delayed",
}

# Extra_json fields that are epoch-ms timestamps — need conversion.
TIMESTAMP_FIELDS: set[str] = set()
