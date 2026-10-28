"""Thin S3-compatible client for MinIO read/write operations."""

from __future__ import annotations

import json
import logging
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, Any

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError

from tractatus.reader.config import ReaderConfig

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

logger = logging.getLogger(__name__)


class MinIOStore:
    """S3-compatible object store client for MinIO (intraday bucket)."""

    def __init__(self, cfg: ReaderConfig) -> None:
        self._cfg = cfg
        self._s3: S3Client = boto3.client(
            "s3",
            endpoint_url=cfg.minio_endpoint,
            aws_access_key_id=cfg.minio_access_key,
            aws_secret_access_key=cfg.minio_secret_key,
            config=Config(signature_version="s3v4"),
        )

    @property
    def intraday_bucket(self) -> str:
        return self._cfg.minio_intraday_bucket

    @property
    def lakehouse_bucket(self) -> str:
        return self._cfg.minio_lakehouse_bucket

    def get_object(self, bucket: str, key: str) -> bytes:
        resp = self._s3.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()  # type: ignore[return-value]

    def put_object(self, bucket: str, key: str, data: bytes) -> None:
        self._s3.put_object(Bucket=bucket, Key=key, Body=data)

    def head_object(self, bucket: str, key: str) -> bool:
        """Return True if the key exists."""
        try:
            self._s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    def list_object_metadata(self, bucket: str, prefix: str) -> list[tuple[str, str, int]]:
        """List keys with opaque ETag revisions and size; propagate listing failures."""
        paginator = self._s3.get_paginator("list_objects_v2")
        return [
            (str(obj["Key"]), str(obj.get("ETag", "")), int(obj["Size"]))
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix)
            for obj in page.get("Contents", [])
        ]

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        """Return all keys under prefix."""
        paginator = self._s3.get_paginator("list_objects_v2")
        keys: list[str] = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                keys.append(str(obj["Key"]))
        return keys

    def get_json(self, bucket: str, key: str) -> dict[str, Any]:
        """Fetch a JSON object. Returns empty dict on any error."""
        try:
            data = self.get_object(bucket, key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            logger.debug("get_json %s/%s: %s", bucket, key, code)
            return {}
        except Exception as exc:
            logger.debug("get_json %s/%s: %s", bucket, key, exc)
            return {}
        return json.loads(data)  # type: ignore[no-any-return]

    def get_csv(self, bucket: str, key: str, dtypes: dict[str, Any] | None = None) -> pd.DataFrame:
        """Fetch a CSV object and return a DataFrame."""
        data = self.get_object(bucket, key)
        content = data.decode("utf-8")
        return pd.read_csv(StringIO(content), dtype=dtypes)  # type: ignore[arg-type]

    def upload_from_local(self, bucket: str, key: str, local_path: Path) -> None:
        """Upload a local file to the store."""
        self._s3.upload_file(str(local_path), bucket, key)
