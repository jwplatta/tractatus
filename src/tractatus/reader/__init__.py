"""Unified data reader for MinIO and Iceberg lakehouse."""

from tractatus.reader.config import ReaderConfig
from tractatus.reader.reader import Reader

__all__ = ["Reader", "ReaderConfig"]
