"""Benchmark DataReader query performance.

Usage:
    python scripts/benchmark_reader.py [--root SPXW] [--provider schwab] [--interval 30]

Requires MinIO to be running and cache populated. Set env vars or .env as usual.
"""

from __future__ import annotations

import argparse
import time
from datetime import date, timedelta
from pathlib import Path

from tractatus.reader import DataReader


def _timer(label: str):
    """Context manager that prints elapsed time."""

    class Timer:
        def __enter__(self):
            self.start = time.perf_counter()
            return self

        def __exit__(self, *_):
            elapsed = time.perf_counter() - self.start
            self.elapsed = elapsed
            print(f"  {label}: {elapsed:.3f}s")

    return Timer()


def run(root: str, provider: str, interval_minutes: int) -> None:
    print(f"Benchmarking DataReader — root={root}, provider={provider}\n")

    # --- Init ---
    with _timer("DataReader init"):
        reader = DataReader()

    # --- list_roots ---
    with _timer("list_roots"):
        roots = reader.list_roots(provider)
    print(f"    → {len(roots)} roots found")

    # --- list_sample_dates ---
    with _timer("list_sample_dates"):
        sample_dates = reader.list_sample_dates(root, provider)
    print(f"    → {len(sample_dates)} sample dates")

    if not sample_dates:
        print("\nNo sample dates found — nothing else to benchmark.")
        return

    # --- get_parquet (warm cache) ---
    recent = sample_dates[-1]
    with _timer(f"get_parquet (warm cache, {recent})"):
        parquet = reader.get_parquet(root, recent, provider)
    if not parquet:
        print("    → No parquet returned. Skipping query benchmarks.")
        return
    print(f"    → {parquet}")
    file_mb = parquet.stat().st_size / (1024 * 1024)
    print(f"    → {file_mb:.1f} MB")

    # --- ensure_parquet_range (warm cache, last 5 dates) ---
    range_start = sample_dates[max(0, len(sample_dates) - 5)]
    range_end = sample_dates[-1]
    with _timer(f"ensure_parquet_range ({range_start}..{range_end})"):
        glob_pattern = reader.ensure_parquet_range(root, range_start, range_end, provider)
    print(f"    → glob: {glob_pattern}")

    # Count matching files
    from pathlib import Path as _P

    matches = list(_P("/").glob(glob_pattern.lstrip("/")))
    print(f"    → {len(matches)} parquet files matched")

    q = reader.options_query

    # --- list_expirations ---
    with _timer("list_expirations"):
        expirations = q.list_expirations(parquet, date.today() - timedelta(days=90))
    print(f"    → {len(expirations)} expirations")

    if not expirations:
        with _timer("list_expirations (no filter)"):
            expirations = q.list_expirations(parquet, date(2000, 1, 1))
        print(f"    → {len(expirations)} expirations (unfiltered)")

    if expirations:
        exp = expirations[0]

        # --- list_snapshot_times ---
        with _timer(f"list_snapshot_times (exp={exp})"):
            snap_times = q.list_snapshot_times(parquet, exp)
        print(f"    → {len(snap_times)} snapshot times")

        # --- load_snapshot (single) ---
        if snap_times:
            with _timer("load_snapshot (single)"):
                df = q.load_snapshot(parquet, exp, snap_times[-1])
            print(f"    → {len(df)} rows, {len(df.columns)} cols")

        # --- load_expiry (all snapshots for one expiry) ---
        with _timer(f"load_expiry (exp={exp})"):
            df = q.load_expiry(parquet, exp)
        print(f"    → {len(df)} rows")

    # --- latest_window ---
    with _timer("latest_window"):
        window = q.latest_window(parquet, date(2000, 1, 1), date(2099, 1, 1))
    print(f"    → {len(window)} expiry windows")

    # --- Multi-parquet glob queries (only if multiple files) ---
    if len(matches) >= 2 and expirations:
        exp_range = (expirations[0], expirations[-1])

        with _timer(f"load_lookback ({len(matches)} files, interval={interval_minutes}m)"):
            df = q.load_lookback(glob_pattern, exp_range, interval_minutes)
        print(f"    → {len(df)} rows")

        with _timer(f"load_sample_window ({len(matches)} files, interval={interval_minutes}m)"):
            df = q.load_sample_window(glob_pattern, range_start, interval_minutes)
        print(f"    → {len(df)} rows")

        with _timer(
            f"load_expiry_lookback ({len(matches)} files, exp={expirations[0]}, "
            f"interval={interval_minutes}m)"
        ):
            df = q.load_expiry_lookback(glob_pattern, expirations[0], interval_minutes)
        print(f"    → {len(df)} rows")

    # --- Candles ---
    print()
    try:
        with _timer("load_candles (daily)"):
            df = reader.load_candles(root.rstrip("W"), "daily", provider)
        print(f"    → {len(df)} rows")
    except FileNotFoundError:
        print("    → no candle data found")

    print("\nDone.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark DataReader queries")
    parser.add_argument("--root", default="SPXW", help="Option root symbol")
    parser.add_argument("--provider", default="schwab", help="Data provider")
    parser.add_argument("--interval", type=int, default=30, help="Interval minutes for lookback")
    args = parser.parse_args()
    run(args.root, args.provider, args.interval)
