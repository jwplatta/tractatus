"""Iceberg schema definition for the level_one_equities table.

All columns are flat — no nested structs. Field names come from the Schwab
streaming API LEVELONE_EQUITIES service definition.
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

LEVEL_ONE_EQUITIES_TABLE = TableDef(
    name="level_one_equities",
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
        NestedField(field_id=15, name="asset_sub_type", field_type=StringType(), required=False),
        NestedField(field_id=16, name="cusip", field_type=StringType(), required=False),
        NestedField(field_id=17, name="delayed", field_type=BooleanType(), required=False),
        # --- Schwab field 6-51 (LEVELONE_EQUITIES numbering) ---
        NestedField(field_id=100, name="ask_id", field_type=StringType(), required=False),
        NestedField(field_id=101, name="bid_id", field_type=StringType(), required=False),
        NestedField(field_id=102, name="last_size", field_type=LongType(), required=False),
        NestedField(field_id=103, name="high_price", field_type=DoubleType(), required=False),
        NestedField(field_id=104, name="low_price", field_type=DoubleType(), required=False),
        NestedField(field_id=105, name="close_price", field_type=DoubleType(), required=False),
        NestedField(field_id=106, name="exchange_id", field_type=StringType(), required=False),
        NestedField(field_id=107, name="marginable", field_type=BooleanType(), required=False),
        NestedField(field_id=108, name="description", field_type=StringType(), required=False),
        NestedField(field_id=109, name="last_id", field_type=StringType(), required=False),
        NestedField(field_id=110, name="open_price", field_type=DoubleType(), required=False),
        NestedField(field_id=111, name="net_change", field_type=DoubleType(), required=False),
        NestedField(
            field_id=112, name="high_price_52_week", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=113, name="low_price_52_week", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=114, name="pe_ratio", field_type=DoubleType(), required=False),
        NestedField(field_id=115, name="dividend_amount", field_type=DoubleType(), required=False),
        NestedField(field_id=116, name="dividend_yield", field_type=DoubleType(), required=False),
        NestedField(field_id=117, name="nav", field_type=LongType(), required=False),
        NestedField(field_id=118, name="exchange_name", field_type=StringType(), required=False),
        NestedField(field_id=119, name="dividend_date", field_type=StringType(), required=False),
        NestedField(
            field_id=120, name="regular_market_quote", field_type=BooleanType(), required=False
        ),
        NestedField(
            field_id=121, name="regular_market_trade", field_type=BooleanType(), required=False
        ),
        NestedField(
            field_id=122,
            name="regular_market_last_price",
            field_type=DoubleType(),
            required=False,
        ),
        NestedField(
            field_id=123,
            name="regular_market_last_size",
            field_type=LongType(),
            required=False,
        ),
        NestedField(
            field_id=124,
            name="regular_market_net_change",
            field_type=DoubleType(),
            required=False,
        ),
        NestedField(field_id=125, name="security_status", field_type=StringType(), required=False),
        # Fields 33, 34, 35 are extracted by tickrake to top-level mark, quote_time_ms, trade_time_ms.
        # They do NOT appear in extra_json, so no columns here.
        NestedField(
            field_id=129,
            name="regular_market_trade_time",
            field_type=TimestamptzType(),
            required=False,
        ),
        NestedField(field_id=130, name="bid_time", field_type=TimestamptzType(), required=False),
        NestedField(field_id=131, name="ask_time", field_type=TimestamptzType(), required=False),
        NestedField(field_id=132, name="ask_mic_id", field_type=StringType(), required=False),
        NestedField(field_id=133, name="bid_mic_id", field_type=StringType(), required=False),
        NestedField(field_id=134, name="last_mic_id", field_type=StringType(), required=False),
        NestedField(
            field_id=135, name="net_change_percent", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=136,
            name="regular_market_change_percent",
            field_type=DoubleType(),
            required=False,
        ),
        NestedField(field_id=137, name="mark_change", field_type=DoubleType(), required=False),
        NestedField(
            field_id=138, name="mark_change_percent", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=139, name="htb_quantity", field_type=LongType(), required=False),
        NestedField(field_id=140, name="htb_rate", field_type=LongType(), required=False),
        NestedField(field_id=141, name="hard_to_borrow", field_type=LongType(), required=False),
        NestedField(field_id=142, name="is_shortable", field_type=LongType(), required=False),
        NestedField(
            field_id=143, name="post_market_net_change", field_type=LongType(), required=False
        ),
        NestedField(
            field_id=144,
            name="post_market_net_change_percent",
            field_type=LongType(),
            required=False,
        ),
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
)

# Mapping from extra_json key → Iceberg column name.
EXTRA_JSON_FIELDS: dict[str, str] = {
    "6": "ask_id",
    "7": "bid_id",
    "9": "last_size",
    "10": "high_price",
    "11": "low_price",
    "12": "close_price",
    "13": "exchange_id",
    "14": "marginable",
    "15": "description",
    "16": "last_id",
    "17": "open_price",
    "18": "net_change",
    "19": "high_price_52_week",
    "20": "low_price_52_week",
    "21": "pe_ratio",
    "22": "dividend_amount",
    "23": "dividend_yield",
    "24": "nav",
    "25": "exchange_name",
    "26": "dividend_date",
    "27": "regular_market_quote",
    "28": "regular_market_trade",
    "29": "regular_market_last_price",
    "30": "regular_market_last_size",
    "31": "regular_market_net_change",
    "32": "security_status",
    # 33, 34, 35 are extracted by tickrake to top-level mark, quote_time_ms, trade_time_ms.
    "36": "regular_market_trade_time",
    "37": "bid_time",
    "38": "ask_time",
    "39": "ask_mic_id",
    "40": "bid_mic_id",
    "41": "last_mic_id",
    "42": "net_change_percent",
    "43": "regular_market_change_percent",
    "44": "mark_change",
    "45": "mark_change_percent",
    "46": "htb_quantity",
    "47": "htb_rate",
    "48": "hard_to_borrow",
    "49": "is_shortable",
    "50": "post_market_net_change",
    "51": "post_market_net_change_percent",
    "assetMainType": "asset_main_type",
    "assetSubType": "asset_sub_type",
    "cusip": "cusip",
    "delayed": "delayed",
}

# Extra_json fields that are epoch-ms timestamps — need conversion.
TIMESTAMP_FIELDS: set[str] = {
    "regular_market_trade_time",
    "bid_time",
    "ask_time",
}
