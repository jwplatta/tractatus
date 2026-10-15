"""S3 archive access with an integrity-checked local parquet cache."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

import boto3

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client

from tractatus.tickrake.config import TickrakeConfig


class ArchiveClient:
    def __init__(self, cfg: TickrakeConfig) -> None:
        self._cfg = cfg
        session = boto3.Session(profile_name=cfg.aws_profile, region_name=cfg.s3_region)
        self._s3: S3Client = session.client("s3")

    def get_root_index(
        self, root: str, provider: str = "schwab", *, refresh: bool = False
    ) -> dict[str, Any]:
        """Read a published root index, fetching it when the local copy is absent or refreshed."""
        path = self._cfg.provider_options_dir(provider) / f"{root}.json"
        if refresh or not path.exists():
            self._download_index(root, provider, path)
        if not path.exists():
            return {}
        return cast(dict[str, Any], json.loads(path.read_text()))

    def get_parquet_path(
        self,
        root: str,
        sample_date: date,
        provider: str = "schwab",
        *,
        refresh_index: bool = False,
    ) -> Path | None:
        """Return a local historical parquet, downloading it once when necessary."""
        local_path = self._local_parquet_path(root, sample_date, provider)
        if local_path.exists() and self._is_valid_cached(local_path):
            return local_path
        descriptor = self._find_parquet_descriptor(root, sample_date, provider, refresh_index)
        if descriptor is None:
            return None
        self._download_descriptor(descriptor, local_path)
        return local_path if self._is_valid_cached(local_path) else None

    def _local_parquet_path(self, root: str, sample_date: date, provider: str) -> Path:
        return (
            self._cfg.provider_options_dir(provider)
            / f"{sample_date.year:04d}"
            / f"{sample_date.month:02d}"
            / f"{sample_date.day:02d}"
            / f"{root}_samples_{sample_date.isoformat()}.parquet"
        )

    def _find_parquet_descriptor(
        self, root: str, sample_date: date, provider: str, refresh: bool
    ) -> dict[str, Any] | None:
        index = self.get_root_index(root, provider, refresh=refresh)
        for entry in index.get("historical", []):
            if entry.get("sample_date") == sample_date.isoformat():
                files = entry.get("files")
                if isinstance(files, list):
                    for descriptor in files:
                        if (
                            isinstance(descriptor, dict)
                            and descriptor.get("format") == "parquet"
                            and descriptor.get("uri")
                        ):
                            return cast(dict[str, Any], descriptor)
        return None

    def _download_index(self, root: str, provider: str, path: Path) -> None:
        if not self._cfg.s3_bucket:
            return
        # Tickrake's normal local layout mirrors this published object path.
        key = f"options/{provider}/{root}.json"
        try:
            self._atomic_s3_download(self._cfg.s3_bucket, key, path)
        except Exception as exc:  # boto errors differ across endpoint implementations
            if path.exists():
                return
            raise RuntimeError(
                f"Could not fetch published index s3://{self._cfg.s3_bucket}/{key}: {exc}"
            ) from exc

    def _download_descriptor(self, descriptor: dict[str, Any], local_path: Path) -> None:
        uri = str(descriptor["uri"])
        parsed = urlparse(uri)
        bucket = parsed.netloc or self._cfg.s3_bucket
        key = parsed.path.lstrip("/")
        if not bucket or not key:
            raise ValueError(f"Invalid archive URI: {uri}")
        with self._lock(local_path):
            if local_path.exists() and self._is_valid_cached(local_path):
                return
            self._atomic_s3_download(bucket, key, local_path)
            digest = self._sha256(local_path)
            expected_digest = descriptor.get("sha256") or descriptor.get("digest")
            if (
                expected_digest
                and digest.lower() != str(expected_digest).removeprefix("sha256:").lower()
            ):
                local_path.unlink(missing_ok=True)
                raise ValueError(f"Integrity check failed for {uri}: digest mismatch")
            expected_size = descriptor.get("size") or descriptor.get("byte_size")
            if expected_size is not None and local_path.stat().st_size != int(expected_size):
                local_path.unlink(missing_ok=True)
                raise ValueError(f"Integrity check failed for {uri}: size mismatch")
            self._sidecar_path(local_path).write_text(
                json.dumps(
                    {
                        "source_uri": uri,
                        "version_id": descriptor.get("version_id") or descriptor.get("version"),
                        "etag": descriptor.get("etag"),
                        "byte_size": local_path.stat().st_size,
                        "sha256": digest,
                        "retrieved_at": datetime.now(UTC).isoformat(),
                    },
                    indent=2,
                )
            )

    def _atomic_s3_download(self, bucket: str, key: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(
            prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
        )
        os.close(fd)
        try:
            self._s3.download_file(bucket, key, temporary)
            os.replace(temporary, destination)
        finally:
            Path(temporary).unlink(missing_ok=True)

    @staticmethod
    def _sidecar_path(path: Path) -> Path:
        return path.with_suffix(path.suffix + ".metadata.json")

    def _is_valid_cached(self, path: Path) -> bool:
        sidecar = self._sidecar_path(path)
        if not sidecar.exists():
            return True  # Existing Tickrake caches predate sidecars.
        try:
            metadata = json.loads(sidecar.read_text())
            return bool(
                metadata.get("byte_size") == path.stat().st_size
                and metadata.get("sha256") == self._sha256(path)
            )
        except (OSError, ValueError, json.JSONDecodeError):
            return False

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    @contextmanager
    def _lock(self, path: Path) -> Iterator[None]:
        """An advisory per-object lock; works on the Unix systems Tickrake supports."""
        lock_path = path.with_suffix(path.suffix + ".lock")
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("w") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
