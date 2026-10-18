"""Iceberg schema definition for the candles table."""

from __future__ import annotations

from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    DoubleType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from tractatus.catalog.schemas._types import TableDef

CANDLES_TABLE = TableDef(
    name="candles",
    schema=Schema(
        NestedField(field_id=1, name="symbol", field_type=StringType(), required=False),
        NestedField(field_id=2, name="interval", field_type=StringType(), required=False),
        NestedField(field_id=3, name="datetime", field_type=TimestamptzType(), required=False),
        NestedField(field_id=4, name="open", field_type=DoubleType(), required=False),
        NestedField(field_id=5, name="high", field_type=DoubleType(), required=False),
        NestedField(field_id=6, name="low", field_type=DoubleType(), required=False),
        NestedField(field_id=7, name="close", field_type=DoubleType(), required=False),
        NestedField(field_id=8, name="volume", field_type=LongType(), required=False),
    ),
    partition_spec=PartitionSpec(
        PartitionField(
            source_id=2,
            field_id=1000,
            transform=IdentityTransform(),
            name="interval_id",
        )
    ),
    sort_keys=["datetime"],
    properties={
        "write.parquet.compression-codec": "zstd",
        "write.parquet.compression-level": "3",
        "write.parquet.row-group-limit": "50000",
    },
)
