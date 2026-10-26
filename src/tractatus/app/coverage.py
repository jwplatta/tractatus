"""Lakehouse coverage visualization app."""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

import numpy as np
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
    cached,
    row_count
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

st.markdown(
    """
    <style>
    /* ---- Light mode ---- */
    @media (prefers-color-scheme: light) {
        :root {
            --bg-page:    #F9FAFB;
            --bg-card:    #FFFFFF;
            --blue:       #1D4ED8;
            --teal:       #0D9488;
            --text-main:  #111827;
            --text-sec:   #4B5563;
            --border:     #D1D5DB;
            --btn-text:   #FFFFFF;
        }
    }
    /* ---- Dark mode ---- */
    @media (prefers-color-scheme: dark) {
        :root {
            --bg-page:    #0B1220;
            --bg-card:    #111827;
            --blue:       #60A5FA;
            --teal:       #2DD4BF;
            --text-main:  #F9FAFB;
            --text-sec:   #9CA3AF;
            --border:     #374151;
            --btn-text:   #0B1220;
        }
    }

    [data-testid="stAppViewContainer"],
    [data-testid="stAppViewBlockContainer"] { background-color: var(--bg-page) !important; }
    [data-testid="stHeader"]               { background-color: var(--bg-page) !important; }

    /* Text */
    html, body, p, li, span, label,
    [data-testid="stMarkdownContainer"]    { color: var(--text-main) !important; }
    h1, h2, h3, h4                         { color: var(--text-main) !important; }

    /* Selectbox */
    [data-testid="stSelectbox"] > div { background-color: var(--bg-card) !important; border: 1px solid var(--border) !important; }
    [data-testid="stSelectbox"] span  { color: var(--text-main) !important; }

    /* Widget labels */
    [data-testid="stWidgetLabel"] p { color: var(--text-sec) !important; }

    /* Buttons */
    [data-testid="stButton"] > button {
        background-color: var(--blue) !important;
        color: var(--btn-text) !important;
        border: none !important;
    }

    /* Dataframe */
    [data-testid="stDataFrame"] { border: 1px solid var(--border) !important; border-radius: 6px; }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Lakehouse Coverage")

if not LEDGER_PATH.exists():
    st.error(f"Ledger not found: {LEDGER_PATH}")
    st.stop()

df_all = load_artifacts(str(LEDGER_PATH))

if df_all.empty:
    st.warning("No published artifacts found in ledger.")
    st.stop()

# ---------------------------------------------------------------------------
# Filters — inline dropdowns
# ---------------------------------------------------------------------------

_ALL = "All"

col1, col2, col3 = st.columns(3)

all_tables = sorted(df_all["table_name"].dropna().unique().tolist())
default_table_idx = all_tables.index("options") if "options" in all_tables else 0
sel_table = col1.selectbox("Table", all_tables, index=default_table_idx)

# Provider options depend on selected table
df_for_provider = df_all[df_all["table_name"] == sel_table]
all_providers = sorted(df_for_provider["provider"].dropna().unique().tolist())
default_provider_idx = all_providers.index("schwab") if "schwab" in all_providers else 0
sel_provider = col2.selectbox("Provider", all_providers, index=default_provider_idx)

# Symbol options depend on both selections
df_for_symbol = df_for_provider[df_for_provider["provider"] == sel_provider]
all_symbols = sorted(df_for_symbol["symbol"].dropna().unique().tolist())
sel_symbol = col3.selectbox("Symbol", [_ALL] + all_symbols)

# Apply filters
df = df_all[(df_all["table_name"] == sel_table) & (df_all["provider"] == sel_provider)].copy()
if sel_symbol != _ALL:
    df = df[df["symbol"] == sel_symbol]

if df.empty:
    st.info("No data matches the current filters.")
    st.stop()

# ---------------------------------------------------------------------------
# Calendar heatmap — one per (table_name, provider)
# ---------------------------------------------------------------------------

groups = df.groupby(["table_name", "provider"])

for (table_name, provider), grp in groups:
    st.subheader(f"{table_name} — {provider}")

    grp = grp.copy()
    grp["year_month"] = grp["sample_date"].dt.to_period("M").astype(str)
    grp["day"] = grp["sample_date"].dt.day

    # Aggregate per (year_month, day): max cached, sum row_count
    agg = (
        grp.groupby(["year_month", "day"])
        .agg(cached=("cached", "max"), row_count=("row_count", "sum"))
        .reset_index()
    )

    # Determine if row_count is meaningful for this table
    has_row_count = agg["row_count"].notna().any() and agg["row_count"].sum() > 0
    max_row_count = float(agg["row_count"].max()) if has_row_count else 1.0

    all_months = sorted(agg["year_month"].unique())
    all_days = list(range(1, 32))

    pivot_cached = agg.pivot(index="year_month", columns="day", values="cached").reindex(
        index=all_months, columns=all_days
    )
    pivot_rc = agg.pivot(index="year_month", columns="day", values="row_count").reindex(
        index=all_months, columns=all_days
    )

    z_vals = []
    hover_text = []
    for ym in all_months:
        z_row = []
        h_row = []
        for d in all_days:
            try:
                actual_date = pd.Timestamp(f"{ym}-{d:02d}")
            except ValueError:
                z_row.append(np.nan)
                h_row.append("")
                continue

            is_weekend = actual_date.dayofweek >= 5
            date_str = actual_date.strftime("%Y-%m-%d")

            if is_weekend:
                z_row.append(-1.0)
                h_row.append(f"{date_str} (weekend)")
                continue

            cached_val = pivot_cached.loc[ym, d] if ym in pivot_cached.index else np.nan
            rc_val = pivot_rc.loc[ym, d] if ym in pivot_rc.index else np.nan

            if pd.isna(cached_val):
                z_row.append(0.0)
                h_row.append(f"{date_str}: no data")
            else:
                if has_row_count and not pd.isna(rc_val) and max_row_count > 0:
                    # Scale row_count to (0.1, 1.0) so even low-count days are visible
                    intensity = 0.1 + 0.9 * (float(rc_val) / max_row_count)
                else:
                    intensity = 1.0
                z_row.append(intensity)
                status = "cached" if cached_val == 1 else "in lakehouse"
                rc_str = f", {int(rc_val):,} rows" if has_row_count and not pd.isna(rc_val) else ""
                h_row.append(f"{date_str}: {status}{rc_str}")
        z_vals.append(z_row)
        hover_text.append(h_row)

    # z=-1: weekend(gray), z=0: no data(white), z=0.1–1.0: teal gradient
    colorscale = [
        [0.0,    "#D1D5DB"],  # z=-1: weekend — border gray
        [0.4999, "#D1D5DB"],
        [0.5,    "#FFFFFF"],  # z=0: no data — surface white
        [0.5001, "#cceeec"],  # z~0.1: faint teal
        [1.0,    "#0D9488"],  # z=1.0: full teal accent
    ]

    fig = go.Figure(
        go.Heatmap(
            z=z_vals,
            x=[str(d) for d in all_days],
            y=all_months,
            colorscale=colorscale,
            zmin=-1.0,
            zmax=1.0,
            showscale=False,
            xgap=2,
            ygap=2,
            hovertemplate="%{customdata}<extra></extra>",
            customdata=hover_text,
        )
    )
    fig.update_layout(
        xaxis_title="Day of Month",
        yaxis_title="Month",
        height=max(300, len(all_months) * 25 + 100),
        margin={"t": 20, "b": 40},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
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
st.dataframe(summary, use_container_width=True)
