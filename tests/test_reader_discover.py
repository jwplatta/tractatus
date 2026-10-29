"""Tests for CacheReader.discover_option_sources."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from tractatus.reader.reader import CacheReader, OptionSource


def _make_reader(objects: list[tuple[str, str, int]]) -> CacheReader:
    """Return a CacheReader with a mocked MinIOStore.list_object_metadata."""
    cfg = MagicMock()
    cfg.minio_endpoint = "http://localhost:9000"
    cfg.minio_access_key = "minioadmin"
    cfg.minio_secret_key = "minioadmin"
    cfg.minio_lakehouse_bucket = "tractatus-lakehouse"
    cfg.minio_intraday_bucket = "tickrake"

    with patch("tractatus.reader.reader.MinIOStore") as mock_store_cls:
        store = MagicMock()
        store.lakehouse_bucket = "tractatus-lakehouse"
        store.list_object_metadata.return_value = objects
        mock_store_cls.return_value = store
        reader = CacheReader(cfg=cfg)
    return reader


def _key(root: str, d: date, provider: str = "schwab") -> str:
    return (
        f"options/{provider}/{d.year:04d}/{d.month:02d}/{d.day:02d}"
        f"/{root}_samples_{d.isoformat()}.parquet"
    )


def _obj(root: str, d: date, etag: str = "abc123", size: int = 1000) -> tuple[str, str, int]:
    return (_key(root, d), etag, size)


def test_discover_returns_empty_for_empty_roots():
    reader = _make_reader([])
    assert reader.discover_option_sources([], date(2026, 1, 1), date(2026, 1, 31)) == {}


def test_discover_returns_empty_when_before_lt_after():
    reader = _make_reader([_obj("SPY", date(2026, 10, 1))])
    assert reader.discover_option_sources(["SPY"], date(2026, 10, 2), date(2026, 10, 1)) == {}


def test_discover_single_listing_for_multiple_roots():
    d1 = date(2026, 10, 1)
    d2 = date(2026, 10, 2)
    objects = [
        _obj("SPY", d1, etag="e1", size=1000),
        _obj("SPXW", d1, etag="e2", size=2000),
        _obj("SPY", d2, etag="e3", size=1100),
    ]
    reader = _make_reader(objects)

    result = reader.discover_option_sources(["SPY", "SPXW"], d1, d2)

    # list_object_metadata called exactly once — the whole point
    reader._minio.list_object_metadata.assert_called_once()

    assert set(result.keys()) == {"SPY", "SPXW"}
    assert [s.sample_date for s in result["SPY"]] == [d1, d2]
    assert result["SPY"][0].revision == "e1:1000"
    assert result["SPY"][0].size_bytes == 1000
    assert result["SPY"][0].uri == f"s3://tractatus-lakehouse/{_key('SPY', d1)}"
    assert result["SPXW"][0].sample_date == d1


def test_discover_excludes_roots_not_requested():
    d = date(2026, 10, 1)
    objects = [_obj("SPY", d), _obj("TSLA", d)]
    reader = _make_reader(objects)

    result = reader.discover_option_sources(["SPY"], d, d)

    assert "TSLA" not in result
    assert "SPY" in result


def test_discover_filters_by_date_range():
    in_range = date(2026, 10, 15)
    too_early = date(2026, 9, 30)
    too_late = date(2026, 10, 31)
    objects = [
        _obj("SPY", too_early, etag="e1", size=100),
        _obj("SPY", in_range, etag="e2", size=200),
        _obj("SPY", too_late, etag="e3", size=300),
    ]
    reader = _make_reader(objects)

    result = reader.discover_option_sources(["SPY"], date(2026, 10, 1), date(2026, 10, 30))

    assert result["SPY"] == [
        OptionSource(
            in_range,
            f"s3://tractatus-lakehouse/{_key('SPY', in_range)}",
            "e2:200",
            200,
        )
    ]


def test_discover_sources_sorted_by_sample_date():
    d1, d2, d3 = date(2026, 10, 3), date(2026, 10, 1), date(2026, 10, 2)
    objects = [_obj("SPY", d) for d in (d1, d2, d3)]
    reader = _make_reader(objects)

    result = reader.discover_option_sources(["SPY"], d2, d1)

    assert [s.sample_date for s in result["SPY"]] == [d2, d3, d1]


def test_discover_omits_roots_with_no_files():
    d = date(2026, 10, 1)
    reader = _make_reader([_obj("SPY", d)])

    result = reader.discover_option_sources(["SPY", "SPXW"], d, d)

    assert "SPXW" not in result
