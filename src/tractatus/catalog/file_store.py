"""Abstraction over file storage backends for catalog discovery.

Provides a protocol and two implementations:
- ``LocalFileStore``: reads from the local filesystem (pathlib)
- ``S3FileStore``: reads from an S3-compatible object store (e.g. MinIO)
"""

from __future__ import annotations

import hashlib
import io
from pathlib import Path
from typing import Protocol

import boto3
import pyarrow as pa
import pyarrow.parquet as pq


class FileStore(Protocol):
    """Backend-agnostic interface for discovering and reading parquet files."""

    def list_parquet(
        self, prefix: str, providers: list[str] | None = None
    ) -> list[tuple[str, int]]:
        """Return ``(key, size_bytes)`` pairs for all ``.parquet`` files under *prefix*.

        Keys are relative to the store root (e.g. ``schwab/2026/06/25/SPX.parquet``).
        If *providers* is given, only scan those provider subdirectories.
        """
        ...

    def sha256(self, key: str) -> str:
        """Return the SHA-256 hex digest of the object at *key*."""
        ...

    def read_parquet(self, key: str) -> pa.Table:
        """Read a parquet file and return an Arrow table."""
        ...


class LocalFileStore:
    """File store backed by a local directory."""

    def __init__(self, root: Path) -> None:
        self._root = root

    def list_parquet(
        self, prefix: str, providers: list[str] | None = None
    ) -> list[tuple[str, int]]:
        base = self._root / prefix
        if not base.is_dir():
            return []

        if providers:
            search_dirs = [base / p for p in providers]
        else:
            search_dirs = [d for d in sorted(base.iterdir()) if d.is_dir()]

        results: list[tuple[str, int]] = []
        for provider_dir in search_dirs:
            if not provider_dir.is_dir():
                continue
            for parquet_path in sorted(provider_dir.rglob("*.parquet")):
                if not parquet_path.is_file():
                    continue
                size = parquet_path.stat().st_size
                if size == 0:
                    continue
                relative = str(parquet_path.relative_to(self._root))
                results.append((relative, size))
        return results

    def sha256(self, key: str) -> str:
        path = self._root / key
        h = hashlib.sha256()
        with path.open("rb") as f:
            while chunk := f.read(8192):
                h.update(chunk)
        return h.hexdigest()

    def read_parquet(self, key: str) -> pa.Table:
        return pq.read_table(self._root / key)


class S3FileStore:
    """File store backed by an S3-compatible object store (e.g. MinIO)."""

    def __init__(
        self,
        endpoint_url: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
    ) -> None:
        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

    def list_parquet(
        self, prefix: str, providers: list[str] | None = None
    ) -> list[tuple[str, int]]:
        prefixes = [f"{prefix}/{p}/" for p in providers] if providers else [f"{prefix}/"]

        results: list[tuple[str, int]] = []
        for search_prefix in prefixes:
            paginator = self._client.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=self._bucket, Prefix=search_prefix):
                for obj in page.get("Contents", []):
                    key: str = obj["Key"]
                    size: int = obj["Size"]
                    if key.endswith(".parquet") and size > 0:
                        results.append((key, size))
        results.sort()
        return results

    def sha256(self, key: str) -> str:
        resp = self._client.get_object(Bucket=self._bucket, Key=key)
        h = hashlib.sha256()
        for chunk in resp["Body"].iter_chunks(8192):
            h.update(chunk)
        return h.hexdigest()

    def read_parquet(self, key: str) -> pa.Table:
        resp = self._client.get_object(Bucket=self._bucket, Key=key)
        data = resp["Body"].read()
        return pq.read_table(io.BytesIO(data))
