"""Research: when does open interest update in Schwab SPXW option chain samples?

Strategy:
  - Use options/schwab/ parquets from tickrake-historical (have OI; massive/ does not)
  - For a set of SPXW expirations, pick consecutive trading day pairs
  - Compare last sample of day N vs first sample of day N+1 for near-ATM strikes
  - Also scan every snapshot within a day to see if OI changes intraday at all
  - Focus on near-ATM strikes (highest gamma / most OI change activity)

Usage:
    uv run python scripts/research_oi_update_timing.py

No arguments needed — picks sensible expirations automatically.
"""

from __future__ import annotations

import io
import re
import tempfile
import os
from datetime import date, timedelta
from pathlib import Path

import boto3
import duckdb
import pandas as pd

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET = "tickrake-historical"
PREFIX = "options/schwab/"

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
pd.set_option("display.float_format", "{:,.0f}".format)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def list_spxw_parquets(s3) -> list[tuple[str, str]]:
    """Return sorted [(date_str, s3_key)] for all SPXW schwab parquets."""
    paginator = s3.get_paginator("list_objects_v2")
    results = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            m = re.search(r"SPXW_samples_(\d{4}-\d{2}-\d{2})\.parquet", obj["Key"])
            if m:
                results.append((m.group(1), obj["Key"]))
    return sorted(results)


_parquet_cache: dict[str, pd.DataFrame] = {}


def load_parquet(s3, key: str) -> pd.DataFrame:
    if key in _parquet_cache:
        return _parquet_cache[key]
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
        f.write(body)
        tmp = f.name
    try:
        conn = duckdb.connect(":memory:")
        df = conn.execute(f"SELECT * FROM read_parquet('{tmp}')").df()
    finally:
        os.unlink(tmp)
    df["sampled_at"] = pd.to_datetime(df["sampled_at"], utc=True)
    df["sampled_at_et"] = df["sampled_at"].dt.tz_convert("America/New_York")
    df["expiration_date"] = pd.to_datetime(df["expiration_date"]).dt.date
    df["strike"] = df["strike"].astype(float)
    df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce")
    _parquet_cache[key] = df
    return df


# ---------------------------------------------------------------------------
# Core analyses
# ---------------------------------------------------------------------------

def intraday_oi_stability(df: pd.DataFrame, expiry: date, label: str) -> None:
    """Q1: Does OI change at any point within a single trading day?"""
    sub = df[df["expiration_date"] == expiry].copy()
    if sub.empty:
        print(f"  [no data for expiry {expiry}]")
        return

    conn = duckdb.connect(":memory:")
    conn.register("sub", sub)
    result = conn.execute("""
        WITH lagged AS (
            SELECT symbol, strike, contract_type, sampled_at_et, open_interest,
                   LAG(open_interest) OVER (PARTITION BY symbol ORDER BY sampled_at_et) AS prev_oi
            FROM sub
        )
        SELECT COUNT(DISTINCT symbol) AS total_contracts,
               SUM(CASE WHEN open_interest <> prev_oi AND prev_oi IS NOT NULL THEN 1 ELSE 0 END) AS n_oi_changed,
               COUNT(DISTINCT sampled_at_et) AS n_snapshots
        FROM lagged
    """).df()
    print(f"\n  {label} | expiry={expiry}")
    print(f"  Snapshots: {result['n_snapshots'].iloc[0]}  "
          f"Contracts: {result['total_contracts'].iloc[0]}  "
          f"OI changes detected: {result['n_oi_changed'].iloc[0]}")


def day_boundary_comparison(
    s3,
    day_n_key: str,
    day_n1_key: str,
    day_n: str,
    day_n1: str,
    expiry: date,
) -> pd.DataFrame:
    """
    Q2: Compare last snapshot of day N vs first snapshot of day N+1 for
    near-ATM strikes on a given expiry.  Returns a DataFrame of the diff.
    """
    df_n = load_parquet(s3, day_n_key)
    df_n1 = load_parquet(s3, day_n1_key)

    sub_n = df_n[df_n["expiration_date"] == expiry]
    sub_n1 = df_n1[df_n1["expiration_date"] == expiry]

    if sub_n.empty or sub_n1.empty:
        return pd.DataFrame()

    last_snap = sub_n["sampled_at"].max()
    first_snap = sub_n1["sampled_at"].min()

    last = sub_n[sub_n["sampled_at"] == last_snap][
        ["symbol", "strike", "contract_type", "open_interest", "underlying_price", "sampled_at_et"]
    ].copy()
    first = sub_n1[sub_n1["sampled_at"] == first_snap][
        ["symbol", "strike", "contract_type", "open_interest", "underlying_price", "sampled_at_et"]
    ].copy()

    merged = last.merge(
        first,
        on=["symbol", "strike", "contract_type"],
        suffixes=(f"_{day_n}", f"_{day_n1}"),
    )
    merged["oi_delta"] = merged[f"open_interest_{day_n1}"] - merged[f"open_interest_{day_n}"]
    merged["abs_delta"] = merged["oi_delta"].abs()

    # Keep near-ATM: within 3% of underlying price
    atm = merged[f"underlying_price_{day_n}"].median()
    near_atm = merged[merged["strike"].between(atm * 0.97, atm * 1.03)].copy()
    near_atm = near_atm.sort_values("abs_delta", ascending=False)

    return near_atm[[
        "symbol", "strike", "contract_type",
        f"sampled_at_et_{day_n}", f"open_interest_{day_n}",
        f"sampled_at_et_{day_n1}", f"open_interest_{day_n1}",
        "oi_delta",
    ]]


def intraday_first_snapshot_vs_last(df: pd.DataFrame, expiry: date) -> None:
    """
    Q3: Within a single day, compare first snapshot OI to last snapshot OI
    to confirm whether OI is completely static intraday.
    """
    sub = df[df["expiration_date"] == expiry]
    if sub.empty:
        return

    first_t = sub["sampled_at"].min()
    last_t = sub["sampled_at"].max()
    first = sub[sub["sampled_at"] == first_t][["symbol", "strike", "contract_type", "open_interest"]].rename(columns={"open_interest": "oi_first"})
    last = sub[sub["sampled_at"] == last_t][["symbol", "strike", "contract_type", "open_interest"]].rename(columns={"open_interest": "oi_last"})

    merged = first.merge(last, on=["symbol", "strike", "contract_type"])
    merged["delta"] = merged["oi_last"] - merged["oi_first"]
    changed = merged[merged["delta"] != 0]

    first_et = sub[sub["sampled_at"] == first_t]["sampled_at_et"].iloc[0]
    last_et = sub[sub["sampled_at"] == last_t]["sampled_at_et"].iloc[0]
    print(f"    First snap: {first_et}  Last snap: {last_et}")
    print(f"    Contracts with OI change first→last: {len(changed)} / {len(merged)}")


def scan_intraday_oi_changes(df: pd.DataFrame, expiry: date, top_n: int = 5) -> None:
    """
    Q4: For the top-N highest OI contracts, show every snapshot where OI changed.
    """
    sub = df[df["expiration_date"] == expiry].copy()
    atm = sub["underlying_price"].median()
    near_atm = sub[sub["strike"].between(atm * 0.97, atm * 1.03)]

    top_syms = (
        near_atm.groupby("symbol")["open_interest"]
        .max()
        .nlargest(top_n)
        .index.tolist()
    )

    conn = duckdb.connect(":memory:")
    conn.register("sub", sub[sub["symbol"].isin(top_syms)])
    changes = conn.execute("""
        WITH lagged AS (
            SELECT symbol, strike, contract_type, sampled_at_et, open_interest,
                   LAG(open_interest) OVER (PARTITION BY symbol ORDER BY sampled_at_et) AS prev_oi
            FROM sub
        )
        SELECT * FROM lagged
        WHERE prev_oi IS NOT NULL AND open_interest <> prev_oi
        ORDER BY symbol, sampled_at_et
    """).df()

    if changes.empty:
        print(f"    No intraday OI changes detected for top {top_n} near-ATM contracts")
    else:
        print(changes.to_string(index=False))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    s3 = s3_client()
    parquets = list_spxw_parquets(s3)
    by_date = {d: k for d, k in parquets}
    sorted_dates = sorted(by_date.keys())

    print(f"Available SPXW schwab parquets: {len(sorted_dates)}")
    print(f"Range: {sorted_dates[0]} → {sorted_dates[-1]}")

    # Pick 4 consecutive-day pairs spread across the date range
    # Target expirations that are a few days out (high gamma, active OI)
    # We'll auto-pick pairs where both days exist
    consecutive_pairs = [
        (sorted_dates[i], sorted_dates[i + 1])
        for i in range(len(sorted_dates) - 1)
    ]

    # Pick pairs spread across range
    step = len(consecutive_pairs) // 4
    selected_pairs = [consecutive_pairs[i * step] for i in range(4)]

    # For each pair, find a good expiry: expires 1-5 days after day_n
    study_cases = []
    for day_n, day_n1 in selected_pairs:
        d = date.fromisoformat(day_n)
        for delta in range(1, 8):
            exp = (d + timedelta(days=delta)).isoformat()
            # Check if expiry is in day_n parquet
            df_n = load_parquet(s3, by_date[day_n])
            expiry_dates = [str(e) for e in df_n["expiration_date"].unique()]
            if exp in expiry_dates:
                df_n1 = load_parquet(s3, by_date[day_n1])
                expiry_dates_n1 = [str(e) for e in df_n1["expiration_date"].unique()]
                if exp in expiry_dates_n1:
                    study_cases.append((day_n, day_n1, date.fromisoformat(exp)))
                    break

    print(f"\nStudy cases (day_N, day_N+1, expiry):")
    for c in study_cases:
        print(f"  {c}")

    # -----------------------------------------------------------------------
    # Analysis 1: Is OI static intraday on day N?
    # -----------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("ANALYSIS 1: Is OI static throughout a single trading day?")
    print("=" * 72)
    for day_n, day_n1, expiry in study_cases:
        df_n = load_parquet(s3, by_date[day_n])
        intraday_oi_stability(df_n, expiry, f"Day={day_n}")
        intraday_first_snapshot_vs_last(df_n, expiry)
        print(f"  Top near-ATM intraday OI change events:")
        scan_intraday_oi_changes(df_n, expiry)

    # -----------------------------------------------------------------------
    # Analysis 2: Last snapshot day N vs first snapshot day N+1
    # -----------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("ANALYSIS 2: OI change from last snapshot day N → first snapshot day N+1")
    print("=" * 72)
    for day_n, day_n1, expiry in study_cases:
        print(f"\n  Day N={day_n} → Day N+1={day_n1} | expiry={expiry}")
        diff = day_boundary_comparison(
            s3, by_date[day_n], by_date[day_n1], day_n, day_n1, expiry
        )
        if diff.empty:
            print("  [no overlapping near-ATM contracts]")
        else:
            changed = diff[diff["oi_delta"] != 0]
            unchanged = diff[diff["oi_delta"] == 0]
            print(f"  Near-ATM contracts: {len(diff)}  Changed: {len(changed)}  Unchanged: {len(unchanged)}")
            if not changed.empty:
                print(f"\n  Top movers (by abs OI delta):")
                print(changed.head(10).to_string(index=False))
            else:
                print("  OI identical across day boundary for all near-ATM contracts.")
                print(diff.head(5).to_string(index=False))

    # -----------------------------------------------------------------------
    # Analysis 3: For each study case, scan ALL snapshots across day N and N+1
    #             looking for the first snapshot where OI differs from the
    #             last snapshot of day N.
    # -----------------------------------------------------------------------
    print("\n" + "=" * 72)
    print("ANALYSIS 3: Pinpoint the exact snapshot where OI updates")
    print("  (last snap day N as baseline → scan every snap day N+1 chronologically)")
    print("=" * 72)
    for day_n, day_n1, expiry in study_cases:
        df_n = load_parquet(s3, by_date[day_n])
        df_n1 = load_parquet(s3, by_date[day_n1])

        sub_n = df_n[df_n["expiration_date"] == expiry]
        sub_n1 = df_n1[df_n1["expiration_date"] == expiry]
        if sub_n.empty or sub_n1.empty:
            continue

        # Baseline: last snap of day N, near-ATM top contracts by OI
        last_t = sub_n["sampled_at"].max()
        baseline = sub_n[sub_n["sampled_at"] == last_t]
        atm = baseline["underlying_price"].median()
        baseline_near = baseline[baseline["strike"].between(atm * 0.97, atm * 1.03)]
        top_syms = (
            baseline_near.groupby("symbol")["open_interest"].max().nlargest(8).index.tolist()
        )
        baseline_oi = baseline_near[baseline_near["symbol"].isin(top_syms)].set_index("symbol")["open_interest"]

        print(f"\n  Day N={day_n} last snap: {baseline[baseline['symbol'].isin(top_syms)]['sampled_at_et'].iloc[0]}")
        print(f"  Baseline OI for top {len(top_syms)} near-ATM contracts:")
        print(f"  {baseline_oi.to_dict()}")

        snap_times = sorted(sub_n1["sampled_at"].unique())
        print(f"  Scanning {len(snap_times)} snapshots on day N+1={day_n1}...")

        first_change_snap = None
        for t in snap_times:
            snap = sub_n1[(sub_n1["sampled_at"] == t) & (sub_n1["symbol"].isin(top_syms))].set_index("symbol")["open_interest"]
            diffs = (snap - baseline_oi).dropna()
            if (diffs != 0).any():
                first_change_snap = t
                snap_et = sub_n1[sub_n1["sampled_at"] == t]["sampled_at_et"].iloc[0]
                print(f"  --> OI first changes at snapshot: {snap_et}")
                print(f"      Deltas: {diffs[diffs != 0].to_dict()}")
                break

        if first_change_snap is None:
            print(f"  OI unchanged across ALL snapshots on day N+1 for these contracts.")


if __name__ == "__main__":
    main()
