"""Lakehouse coverage visualization app."""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Ledger path resolution
# ---------------------------------------------------------------------------

_DEFAULT_LEDGER = Path.home() / ".tractatus" / "catalog_ledger.db"


def _resolve_ledger_path() -> Path:
    url = os.environ.get("LEDGER_DATABASE_URL", "")
    if url:
        if url.startswith("sqlite:///"):
            return Path(url.removeprefix("sqlite:///"))
        if url.startswith("postgresql") or url.startswith("postgres"):
            st.error(
                "LEDGER_DATABASE_URL points to a PostgreSQL database. "
                "This app only supports SQLite. Set LEDGER_DATABASE_URL to a "
                "sqlite:/// URL or leave it unset."
            )
            st.stop()
    # Check for CLI arg (passed after --): streamlit run coverage.py -- /path/to/ledger.db
    if len(sys.argv) > 1:
        return Path(sys.argv[1])
    return _DEFAULT_LEDGER


LEDGER_PATH = _resolve_ledger_path()

# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

_QUERY = """
SELECT
    table_name,
    provider,
    symbol,
    date(sample_date) AS sample_date,
    cached
FROM published_artifacts
"""


@st.cache_data(ttl=300)
def load_artifacts(ledger_path: str) -> pd.DataFrame:
    con = sqlite3.connect(ledger_path)
    try:
        df = pd.read_sql_query(_QUERY, con)
    finally:
        con.close()
    df["sample_date"] = pd.to_datetime(df["sample_date"])
    df = df.dropna(subset=["sample_date"])
    return df


# ---------------------------------------------------------------------------
# App layout
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Lakehouse Coverage", layout="wide")
st.title("Lakehouse Coverage")

if not LEDGER_PATH.exists():
    st.error(f"Ledger not found: {LEDGER_PATH}")
    st.stop()

df_all = load_artifacts(str(LEDGER_PATH))

if df_all.empty:
    st.warning("No published artifacts found in ledger.")
    st.stop()

# ---------------------------------------------------------------------------
# Sidebar filters
# ---------------------------------------------------------------------------

st.sidebar.header("Filters")

all_tables = sorted(df_all["table_name"].unique().tolist())
sel_tables = st.sidebar.multiselect("table_name", all_tables, default=all_tables)

all_providers = sorted(df_all["provider"].unique().tolist())
sel_providers = st.sidebar.multiselect("provider", all_providers, default=all_providers)

symbol_filter = st.sidebar.text_input("symbol (contains)", "")

# Apply filters
df = df_all.copy()
if sel_tables:
    df = df[df["table_name"].isin(sel_tables)]
if sel_providers:
    df = df[df["provider"].isin(sel_providers)]
if symbol_filter:
    df = df[df["symbol"].str.contains(symbol_filter, case=False, na=False)]

if df.empty:
    st.info("No data matches the current filters.")
    st.stop()

# ---------------------------------------------------------------------------
# Calendar heatmap — one per (table_name, provider)
# ---------------------------------------------------------------------------

st.header("Calendar Heatmap")

groups = df.groupby(["table_name", "provider"])

for (table_name, provider), grp in groups:
    st.subheader(f"{table_name} — {provider}")

    grp = grp.copy()
    grp["year_month"] = grp["sample_date"].dt.to_period("M").astype(str)
    grp["day"] = grp["sample_date"].dt.day

    # Aggregate: prefer cached=1 over cached=0 when multiple symbols on same day
    agg = (
        grp.groupby(["year_month", "day"])["cached"]
        .max()
        .reset_index()
    )

    all_months = sorted(agg["year_month"].unique())
    all_days = list(range(1, 32))

    # Build z matrix: rows=months, cols=days
    # Values: 1=cached(green), 0=in lakehouse(blue), None=no data
    pivot = agg.pivot(index="year_month", columns="day", values="cached")
    pivot = pivot.reindex(index=all_months, columns=all_days)

    # Encode: 1 -> 2 (cached), 0 -> 1 (lakehouse), NaN -> 0 (no data)
    z = pivot.copy()
    z_encoded = z.map(lambda v: 2 if v == 1 else (1 if v == 0 else 0))

    colorscale = [
        [0.0, "#ffffff"],   # no data
        [0.5, "#3498db"],   # in lakehouse, not cached
        [0.999, "#3498db"],
        [1.0, "#2ecc71"],   # cached
    ]

    fig = go.Figure(
        go.Heatmap(
            z=z_encoded.values,
            x=[str(d) for d in all_days],
            y=all_months,
            colorscale=colorscale,
            zmin=0,
            zmax=2,
            showscale=False,
            hovertemplate="Month: %{y}<br>Day: %{x}<br>Status: %{customdata}<extra></extra>",
            customdata=z_encoded.map(
                lambda v: "cached" if v == 2 else ("in lakehouse" if v == 1 else "no data")
            ).values,
        )
    )
    fig.update_layout(
        xaxis_title="Day of Month",
        yaxis_title="Month",
        height=max(300, len(all_months) * 25 + 100),
        margin={"t": 20, "b": 40},
    )
    st.plotly_chart(fig, use_container_width=True, key=f"heatmap_{table_name}_{provider}")

# ---------------------------------------------------------------------------
# Summary table
# ---------------------------------------------------------------------------

st.header("Summary")


def _compute_summary(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for (symbol, table_name, provider), grp in df.groupby(
        ["symbol", "table_name", "provider"]
    ):
        dates = grp["sample_date"].dropna().drop_duplicates().sort_values()
        if dates.empty:
            continue
        first = dates.min()
        last = dates.max()
        total = len(dates)
        cached = int(grp.groupby("sample_date")["cached"].max().sum())

        bdays = pd.bdate_range(first, last)
        gap_count = max(0, len(bdays) - total)
        pct_cached = round(100.0 * cached / total, 1) if total else 0.0

        rows.append(
            {
                "table_name": table_name,
                "provider": provider,
                "symbol": symbol,
                "first_date": first.date(),
                "last_date": last.date(),
                "total_days": total,
                "cached_days": cached,
                "pct_cached": pct_cached,
                "gaps": gap_count,
            }
        )
    return pd.DataFrame(rows).sort_values("symbol").reset_index(drop=True)


summary = _compute_summary(df)

def _highlight_gaps(row: pd.Series) -> list[str]:
    color = "background-color: #ffe0e0" if row["gaps"] > 0 else ""
    return [color] * len(row)


styled = summary.style.apply(_highlight_gaps, axis=1)
st.dataframe(styled, use_container_width=True)
