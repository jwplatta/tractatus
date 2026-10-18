"""Unified data reader for MinIO and Iceberg lakehouse."""

from tractatus.reader.config import ReaderConfig
from tractatus.reader.reader import DataReader

__all__ = ["DataReader", "ReaderConfig"]
