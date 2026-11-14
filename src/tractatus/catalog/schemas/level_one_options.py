"""Iceberg schema for Schwab options level-one events."""

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

LEVEL_ONE_OPTIONS_TABLE = TableDef(
    name="level_one_options",
    schema=Schema(
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
        NestedField(field_id=101, name="description", field_type=StringType(), required=False),
        NestedField(field_id=105, name="high_price", field_type=DoubleType(), required=False),
        NestedField(field_id=106, name="low_price", field_type=DoubleType(), required=False),
        NestedField(field_id=107, name="close_price", field_type=DoubleType(), required=False),
        NestedField(field_id=109, name="open_interest", field_type=LongType(), required=False),
        NestedField(field_id=110, name="volatility", field_type=DoubleType(), required=False),
        NestedField(
            field_id=111, name="money_intrinsic_value", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=112, name="expiration_year", field_type=LongType(), required=False),
        NestedField(field_id=113, name="multiplier", field_type=DoubleType(), required=False),
        NestedField(field_id=114, name="digits", field_type=LongType(), required=False),
        NestedField(field_id=115, name="open_price", field_type=DoubleType(), required=False),
        NestedField(field_id=118, name="last_size", field_type=LongType(), required=False),
        NestedField(field_id=119, name="net_change", field_type=DoubleType(), required=False),
        NestedField(field_id=120, name="strike_type", field_type=StringType(), required=False),
        NestedField(field_id=121, name="contract_type", field_type=StringType(), required=False),
        NestedField(field_id=122, name="underlying", field_type=StringType(), required=False),
        NestedField(field_id=123, name="expiration_month", field_type=LongType(), required=False),
        NestedField(field_id=124, name="deliverables", field_type=StringType(), required=False),
        NestedField(field_id=125, name="time_value", field_type=DoubleType(), required=False),
        NestedField(field_id=126, name="expiration_day", field_type=LongType(), required=False),
        NestedField(field_id=127, name="days_to_expiration", field_type=LongType(), required=False),
        NestedField(field_id=128, name="delta", field_type=DoubleType(), required=False),
        NestedField(field_id=129, name="gamma", field_type=DoubleType(), required=False),
        NestedField(field_id=130, name="theta", field_type=DoubleType(), required=False),
        NestedField(field_id=131, name="vega", field_type=DoubleType(), required=False),
        NestedField(field_id=132, name="rho", field_type=DoubleType(), required=False),
        NestedField(field_id=133, name="security_status", field_type=StringType(), required=False),
        NestedField(
            field_id=134, name="theoretical_option_value", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=135, name="underlying_price", field_type=DoubleType(), required=False),
        NestedField(
            field_id=136, name="uv_expiration_type", field_type=StringType(), required=False
        ),
        NestedField(field_id=140, name="exchange_id", field_type=StringType(), required=False),
        NestedField(field_id=141, name="exchange_name", field_type=StringType(), required=False),
        NestedField(field_id=142, name="last_trading_day", field_type=StringType(), required=False),
        NestedField(field_id=143, name="settlement_type", field_type=StringType(), required=False),
        NestedField(
            field_id=144, name="net_percent_change", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=145, name="mark_change", field_type=DoubleType(), required=False),
        NestedField(
            field_id=146, name="mark_change_percent", field_type=DoubleType(), required=False
        ),
        NestedField(field_id=147, name="implied_yield", field_type=DoubleType(), required=False),
        NestedField(field_id=148, name="is_penny", field_type=BooleanType(), required=False),
        NestedField(field_id=149, name="option_root", field_type=StringType(), required=False),
        NestedField(
            field_id=150, name="high_price_52_week", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=151, name="low_price_52_week", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=152, name="indicative_asking_price", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=153, name="indicative_bid_price", field_type=DoubleType(), required=False
        ),
        NestedField(
            field_id=154, name="indicative_quote_time", field_type=LongType(), required=False
        ),
        NestedField(field_id=155, name="exercise_type", field_type=StringType(), required=False),
        NestedField(field_id=200, name="extra_json", field_type=StringType(), required=False),
        NestedField(field_id=201, name="provider", field_type=StringType(), required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=13,
            field_id=1000,
            transform=MonthTransform(),
            name="sample_date_month",
        )
    ),
    sort_keys=["provider", "received_at", "quote_time", "trade_time"],
    properties={
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "3",
        "write.parquet.row-group-limit": "150000",
    },
)

EXTRA_JSON_FIELDS: dict[str, str] = {
    "1": "description",
    "5": "high_price",
    "6": "low_price",
    "7": "close_price",
    "9": "open_interest",
    "10": "volatility",
    "11": "money_intrinsic_value",
    "12": "expiration_year",
    "13": "multiplier",
    "14": "digits",
    "15": "open_price",
    "18": "last_size",
    "19": "net_change",
    "20": "strike_type",
    "21": "contract_type",
    "22": "underlying",
    "23": "expiration_month",
    "24": "deliverables",
    "25": "time_value",
    "26": "expiration_day",
    "27": "days_to_expiration",
    "28": "delta",
    "29": "gamma",
    "30": "theta",
    "31": "vega",
    "32": "rho",
    "33": "security_status",
    "34": "theoretical_option_value",
    "35": "underlying_price",
    "36": "uv_expiration_type",
    "40": "exchange_id",
    "41": "exchange_name",
    "42": "last_trading_day",
    "43": "settlement_type",
    "44": "net_percent_change",
    "45": "mark_change",
    "46": "mark_change_percent",
    "47": "implied_yield",
    "48": "is_penny",
    "49": "option_root",
    "50": "high_price_52_week",
    "51": "low_price_52_week",
    "52": "indicative_asking_price",
    "53": "indicative_bid_price",
    "54": "indicative_quote_time",
    "55": "exercise_type",
}

TIMESTAMP_FIELDS: set[str] = set()
