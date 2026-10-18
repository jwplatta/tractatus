"""Iceberg schema definition for the economic_events table."""

from __future__ import annotations

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from tractatus.catalog.schemas._types import TableDef

ECONOMIC_EVENTS_TABLE = TableDef(
    name="economic_events",
    schema=Schema(
        NestedField(
            field_id=1, name="event_datetime", field_type=TimestamptzType(), required=False
        ),
        NestedField(field_id=2, name="event_name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="category", field_type=StringType(), required=False),
        NestedField(field_id=4, name="source", field_type=StringType(), required=False),
        NestedField(field_id=5, name="actual", field_type=DoubleType(), required=False),
        NestedField(field_id=6, name="estimate", field_type=DoubleType(), required=False),
        NestedField(field_id=7, name="previous", field_type=DoubleType(), required=False),
        NestedField(field_id=8, name="unit", field_type=StringType(), required=False),
        NestedField(field_id=9, name="symbol", field_type=StringType(), required=False),
        NestedField(field_id=10, name="company_name", field_type=StringType(), required=False),
        NestedField(field_id=11, name="time_of_day", field_type=StringType(), required=False),
        NestedField(field_id=12, name="release_id", field_type=LongType(), required=False),
        NestedField(field_id=13, name="series_id", field_type=StringType(), required=False),
        NestedField(field_id=14, name="sampled_at", field_type=TimestamptzType(), required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=1,
            field_id=1000,
            transform=MonthTransform(),
            name="event_datetime_month",
        )
    ),
    sort_keys=["event_datetime", "event_name"],
    properties={
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "3",
        "write.parquet.row-group-limit": "50000",
    },
)

TIMESTAMP_COLUMNS: list[str] = [
    "event_datetime",
    "sampled_at",
]
