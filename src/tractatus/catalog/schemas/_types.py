"""Shared types for schema definitions."""

from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema


@dataclass(frozen=True)
class TableDef:
    """An Iceberg table definition."""

    name: str
    schema: Schema
    partition_spec: PartitionSpec
