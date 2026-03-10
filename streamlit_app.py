import logging
import os

import pandas as pd
import streamlit as st
import numpy as np
import yaml
import plotly.graph_objects as go

from db import get_duckdb_connection

logger = logging.getLogger(__name__)

SQL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries")

def load_sql(filename: str) -> str:
    with open(os.path.join(SQL_DIR, filename)) as f:
        return f.read()

st.set_page_config(layout="wide")

# ── UI ────────────────────────────────────────────────────────────────────────

st.image("OpenPrescribing.svg")

st.info("""##### Hello!  This is a **very** early prototype of analysing hospital FP10s that have been dispensed in the community.  
Please let use know what you think, and what you've like to see.  Email us at [bennett@phc.ox.ac.uk](mailto:bennett@phc.ox.ac.uk)""")

st.title("Hospital FP10s dispensed in the community viewer")

conn = get_duckdb_connection()

df = conn.execute("SELECT * FROM ods_mapping").fetchdf()
df["ultimate_successors"] = df["ultimate_successors"].apply(
    lambda x: list(x) if isinstance(x, np.ndarray) else ([] if x is None else x)
)

df_open = df[df["legal_closed_date"].isna()].copy()

code_to_name = df[df["legal_closed_date"].isna()].set_index("ods_code")["ods_name"].to_dict()
predecessor_to_successor = {}
for _, row in df[df["legal_closed_date"].notna()].iterrows():
    for successor in row["ultimate_successors"]:
        predecessor_to_successor[row["ods_code"]] = successor

# ── Sidebar part 1: organisation filters ─────────────────────────────────────

with st.sidebar:
    st.header("Filters")
    st.info("Select an organisation at any level.")

    region_opts = sorted(df_open["region"].dropna().unique().tolist())
    sel_regions = [v for v in st.session_state.get("sel_region", []) if v in region_opts]
    sel_regions = st.multiselect("Region", region_opts, default=sel_regions, key="sel_region")
    df_region = df_open if not sel_regions else df_open[df_open["region"].isin(sel_regions)]

    icb_opts = sorted(df_region["icb"].dropna().unique().tolist())
    sel_icbs = [v for v in st.session_state.get("sel_icb", []) if v in icb_opts]
    sel_icbs = st.multiselect("ICB", icb_opts, default=sel_icbs, key="sel_icb")
    df_icb = df_region if not sel_icbs else df_region[df_region["icb"].isin(sel_icbs)]

    pr_pairs = (
        df_icb[["ods_code", "ods_name"]]
        .drop_duplicates()
        .sort_values("ods_name")
    )
    pr_map: dict[str, str] = {
        f"{row.ods_name} ({row.ods_code})": row.ods_code
        for row in pr_pairs.itertuples(index=False)
    }
    pr_opts = list(pr_map.keys())
    sel_prs = [v for v in st.session_state.get("sel_pr", []) if v in pr_opts]
    sel_prs = st.multiselect("Hospital Trust", pr_opts, default=sel_prs, key="sel_pr")

# ── ODS code resolution ───────────────────────────────────────────────────────

earliest_month = pd.to_datetime(
    conn.execute("SELECT MIN(CAST(month AS DATE)) FROM prescribing").fetchone()[0]
)

def resolve_ods_codes(selected_codes: list[str], df_full: pd.DataFrame) -> list[str]:
    all_codes = set(selected_codes)
    for code in selected_codes:
        mask = df_full["ultimate_successors"].apply(lambda x: code in x)
        closed = df_full[
            df_full["legal_closed_date"].notna() &
            (pd.to_datetime(df_full["legal_closed_date"]) >= earliest_month) &
            mask
        ]["ods_code"].tolist()
        all_codes.update(closed)
    return list(all_codes)

if sel_prs:
    direct_codes = [pr_map[p] for p in sel_prs]
    ods_codes = resolve_ods_codes(direct_codes, df)
elif sel_icbs:
    ods_codes = resolve_ods_codes(df_icb["ods_code"].unique().tolist(), df)
elif sel_regions:
    ods_codes = resolve_ods_codes(df_region["ods_code"].unique().tolist(), df)
else:
    ods_codes = resolve_ods_codes(df_open["ods_code"].unique().tolist(), df)

# ── Sidebar part 2: predecessor info + display controls ──────────────────────

with st.sidebar:
    predecessors = df[df["ods_code"].isin(ods_codes) & df["legal_closed_date"].notna()]
    if not predecessors.empty and (sel_prs or sel_icbs or sel_regions):
        parts = [
            f"- {row.ods_name} (closed: {pd.to_datetime(row.legal_closed_date).strftime('%-d %B %Y')})"
            for row in predecessors.itertuples(index=False)
        ]
        noun = "organisation" if len(predecessors) == 1 else "organisations"
        st.info(f"ℹ️ Also includes predecessor {noun}:\n" + "\n".join(parts))

    st.divider()

    min_date, max_date = conn.execute(load_sql("date_range.sql")).fetchone()
    default_start = max_date - pd.DateOffset(months=3)

    start_date, end_date = st.slider(
        "Date range",
        min_value=min_date,
        max_value=max_date,
        value=(default_start.date(), max_date),
        format="MMM YYYY"
    )

    top_n = st.slider("Top N items", min_value=5, max_value=100, value=20)
    sort_by = st.radio("Sort by", ["Cost", "Items"], horizontal=True)

# ── Charts ────────────────────────────────────────────────────────────────────

with st.spinner("Loading data..."):
    month_data = conn.execute(load_sql("month_data.sql"), [ods_codes]).fetchdf()

col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(x=month_data["month"], y=month_data["items"], mode="lines"))
    fig1.update_layout(
        title="Items over Time",
        xaxis=dict(type="date"),
        yaxis=dict(title="Items", rangemode="tozero"),
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=month_data["month"], y=month_data["actual_cost"], mode="lines"))
    fig2.update_layout(
        title="Cost over Time",