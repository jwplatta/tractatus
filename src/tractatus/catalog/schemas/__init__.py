"""Iceberg table schema definitions.

Each table definition is a ``TableDef`` containing the table name, Iceberg
schema, and partition spec.  To add a new table, create a module in this
package and register it in ``TABLE_DEFS``.
"""

from __future__ import annotations

from pyiceberg.catalog import Catalog
from pyiceberg.table import Table

from tractatus.catalog.schemas._types import TableDef
from tractatus.catalog.schemas.candles import CANDLES_TABLE
from tractatus.catalog.schemas.economic_events import ECONOMIC_EVENTS_TABLE
from tractatus.catalog.schemas.fundamentals import FUNDAMENTALS_TABLE
from tractatus.catalog.schemas.level_one_equities import LEVEL_ONE_EQUITIES_TABLE
from tractatus.catalog.schemas.level_one_futures import LEVEL_ONE_FUTURES_TABLE
from tractatus.catalog.schemas.options import OPTIONS_TABLE
from tractatus.catalog.schemas.order_book import ORDER_BOOK_TABLE

TABLE_DEFS: dict[str, TableDef] = {
    OPTIONS_TABLE.name: OPTIONS_TABLE,
    LEVEL_ONE_EQUITIES_TABLE.name: LEVEL_ONE_EQUITIES_TABLE,
    LEVEL_ONE_FUTURES_TABLE.name: LEVEL_ONE_FUTURES_TABLE,
    ORDER_BOOK_TABLE.name: ORDER_BOOK_TABLE,
    CANDLES_TABLE.name: CANDLES_TABLE,
    FUNDAMENTALS_TABLE.name: FUNDAMENTALS_TABLE,
    ECONOMIC_EVENTS_TABLE.name: ECONOMIC_EVENTS_TABLE,
}

__all__ = [
    "CANDLES_TABLE",
    "ECONOMIC_EVENTS_TABLE",
    "FUNDAMENTALS_TABLE",
    "LEVEL_ONE_EQUITIES_TABLE",
    "LEVEL_ONE_FUTURES_TABLE",
    "OPTIONS_TABLE",
    "ORDER_BOOK_TABLE",
    "TABLE_DEFS",
    "TableDef",
    "ensure_table",
]


def ensure_table(catalog: Catalog, database: str, table_name: str) -> Table:
    """Create an Iceberg table if it does not exist, or load it.

    Looks up the schema and partition spec from ``TABLE_DEFS``.
    """
    table_def = TABLE_DEFS[table_name]
    full_name = f"{database}.{table_name}"
    try:
        return catalog.load_table(full_name)
    except Exception:
        catalog.create_namespace_if_not_exists(database)
        return catalog.create_table(
            full_name,
            schema=table_def.schema,
            partition_spec=table_def.partition_spec,
            properties=table_def.properties,
        )
