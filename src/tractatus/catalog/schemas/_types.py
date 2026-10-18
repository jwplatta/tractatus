"""Shared types for schema definitions."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema


@dataclass(frozen=True)
class TableDef:
    """An Iceberg table definition."""

    name: str
    schema: Schema
    partition_spec: PartitionSpec
    sort_keys: list[str] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)
