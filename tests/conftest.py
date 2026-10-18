"""Shared fixtures for tractatus tests."""

from __future__ import annotations

from pathlib import Path

import pytest


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    """Create a minimal data directory structure."""
    return tmp_path / "data"
