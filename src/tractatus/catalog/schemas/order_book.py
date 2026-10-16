"""Iceberg schema definition for the order_book table.

Stores L2 market data with nested bid/ask price levels and venue breakdowns.
"""

from __future__ import annotations

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    DateType,
    DoubleType,
    ListType,
    LongType,
    NestedField,
    StringType,
    StructType,
    TimestamptzType,
)

from tractatus.catalog.schemas._types import TableDef

_VENUE_STRUCT = StructType(
    NestedField(field_id=301, name="venue", field_type=StringType(), required=False),
    NestedField(field_id=302, name="size", field_type=LongType(), required=False),
    NestedField(field_id=303, name="sequence_id", field_type=LongType(), required=False),
)

_PRICE_LEVEL_STRUCT = StructType(
    NestedField(field_id=310, name="price", field_type=DoubleType(), required=False),
    NestedField(field_id=311, name="size", field_type=LongType(), required=False),
    NestedField(field_id=312, name="num_orders", field_type=LongType(), required=False),
    NestedField(
        field_id=313,
        name="venues",
        field_type=ListType(element_id=314, element=_VENUE_STRUCT, element_required=False),
        required=False,
    ),
)

ORDER_BOOK_TABLE = TableDef(
    name="order_book",
    schema=Schema(
        NestedField(field_id=1, name="received_at", field_type=TimestamptzType(), required=False),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
        NestedField(field_id=3, name="service", field_type=StringType(), required=False),
        NestedField(field_id=4, name="book_time", field_type=TimestamptzType(), required=False),
        NestedField(field_id=5, name="sample_date", field_type=DateType(), required=False),
        NestedField(
            field_id=6,
            name="bids",
            field_type=ListType(element_id=7, element=_PRICE_LEVEL_STRUCT, element_required=False),
            required=False,
        ),
        NestedField(
            field_id=8,
            name="asks",
            field_type=ListType(element_id=9, element=_PRICE_LEVEL_STRUCT, element_required=False),
            required=False,
        ),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=5,
            field_id=1000,
            transform=MonthTransform(),
            name="sample_date_month",
        )
    ),
)
