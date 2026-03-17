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

st.info(
    """##### Hello!  This is a **very** early prototype of analysing hospital FP10s that have been dispensed in the community.  
Please let us know what you think, and what you'd like to see.  Email us at [bennett@phc.ox.ac.uk](mailto:bennett@phc.ox.ac.uk)"""
)

st.title("Hospital FP10s dispensed in the community viewer")

conn = get_duckdb_connection()
st.write(conn.execute("SHOW TABLES").fetchall())
df = conn.execute("SELECT * FROM ods_mapping").fetchdf()
df["ultimate_successors"] = df["ultimate_successors"].apply(
    lambda x: list(x) if isinstance(x, np.ndarray) else ([] if x is None else x)
)

# Derive closed_date as the earliest of legal_closed_date and operational_closed_date
df["closed_date"] = df[["legal_closed_date", "operational_closed_date"]].apply(
    lambda row: min(d for d in [row["legal_closed_date"], row["operational_closed_date"]] if pd.notna(d))
    if any(pd.notna(d) for d in [row["legal_closed_date"], row["operational_closed_date"]])
    else pd.NaT,
    axis=1
)

df_open = df[df["closed_date"].isna()].copy()

code_to_name = df_open.set_index("ods_code")["ods_name"].to_dict()

predecessor_to_successor = {}
for _, row in df[df["closed_date"].notna()].iterrows():
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
    sel_icbs = st.multiselect("ICS", icb_opts, default=sel_icbs, key="sel_icb")
    df_icb = df_region if not sel_icbs else df_region[df_region["icb"].isin(sel_icbs)]

    pr_pairs = (
        df_icb[["ods_code", "ods_name"]]
        .drop_duplicates()
        .sort_values("ods_name")
    )

    pr_map = {
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
            df_full["closed_date"].notna()
            & (pd.to_datetime(df_full["closed_date"]) >= earliest_month)
            & mask
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


# ── Sidebar part 2: display controls ─────────────────────────────────────────

with st.sidebar:
    st.divider()

    min_date, max_date = conn.execute(load_sql("date_range.sql")).fetchone()
    default_start = max_date - pd.DateOffset(months=3)

    start_date, end_date = st.slider(
        "Date range",
        min_value=min_date,
        max_value=max_date,
        value=(default_start.date(), max_date),
        format="MMM YYYY",
    )

    top_n = st.slider("Top N items", min_value=5, max_value=100, value=20)
    sort_by = st.radio("Sort by", ["Cost", "Items"], horizontal=True)


# ── Predecessor info ──────────────────────────────────────────────────────────

predecessors = df[df["ods_code"].isin(ods_codes) & df["closed_date"].notna()]
if not predecessors.empty and (sel_prs or sel_icbs or sel_regions):
    parts = [
        f"- {row.ods_name} (closed: {pd.to_datetime(row.closed_date).strftime('%-d %B %Y')})"
        for row in predecessors.itertuples(index=False)
    ]
    noun = "organisation" if len(predecessors) == 1 else "organisations"
    st.info(f"ℹ️ Also includes predecessor {noun}:\n" + "\n".join(parts))


# ── Table ─────────────────────────────────────────────────────────────────────

st.info("ℹ️ If you have selected multiple trusts, click on the arrow next to the drug to see prescribing for individual trusts")

with st.spinner("Loading table data..."):
    detail_data = conn.execute(load_sql("top.sql"), [ods_codes, start_date, end_date]).fetchdf()

detail_data["hospital"] = detail_data["hospital"].apply(
    lambda x: predecessor_to_successor.get(x, x)
)

def lookup_name(code: str) -> str:
    if code in code_to_name:
        return code_to_name[code]
    for ods_code, name in code_to_name.items():
        if code.startswith(ods_code):
            return name
    return code


detail_data["hospital"] = detail_data["hospital"].apply(lookup_name)

with st.sidebar:
    cd_opts = sorted(detail_data["cd_category"].dropna().unique().tolist())
    sel_cd = st.multiselect(
        "Filter by CD category", cd_opts,
        default=[v for v in st.session_state.get("sel_cd", []) if v in cd_opts],
        key="cd_other"
    )

if sel_cd:
    detail_data = detail_data[detail_data["cd_category"].isin(sel_cd)]
    
#with st.sidebar:
 #   bnf_opts = sorted(detail_data["bnf_name"].dropna().unique().tolist())
 #   sel_bnf = st.multiselect(
 # #      "Filter by BNF name", bnf_opts,
 #       default=[v for v in st.session_state.get("sel_bnf", []) if v in bnf_opts],
#        key="sel_bnf"
#    )
#
#if sel_bnf:
#    detail_data = detail_data[detail_data["bnf_name"].isin(sel_bnf)]

sort_col = "actual_cost" if sort_by == "Cost" else "items"
single_trust = detail_data["hospital"].nunique() == 1

top_ranked = (
    detail_data.groupby("bnf_name")[["items", "actual_cost"]]
    .sum().reset_index()
    .nlargest(top_n, sort_col)
)

st.subheader(f"Top {top_n} by {sort_by.lower()} — {start_date.strftime('%b %Y')} to {end_date.strftime('%b %Y')}")

if single_trust:
    height = min(740, (len(top_ranked) + 1) * 35 + 10)
    st.dataframe(
        top_ranked
        .assign(actual_cost=lambda d: d["actual_cost"].map("£{:,.2f}".format))
        .rename(columns={"bnf_name": "BNF Name", "actual_cost": "Actual Cost", "items": "Items"}),
        hide_index=True,
        height=height,
    )
else:
    for _, row in top_ranked.iterrows():
        label = f"{row['bnf_name']} — £{row['actual_cost']:,.2f} ({row['items']:,.0f} items)"
        trust_breakdown = detail_data[detail_data["bnf_name"] == row["bnf_name"]]
        with st.expander(label):
            st.dataframe(
                trust_breakdown[["hospital", "actual_cost", "items"]]
                .sort_values(sort_col, ascending=False)
                .assign(actual_cost=lambda d: d["actual_cost"].map("£{:,.2f}".format))
                .rename(columns={"hospital": "Hospital", "actual_cost": "Actual Cost", "items": "Items"}),
                hide_index=True,
            )


# ── Charts ────────────────────────────────────────────────────────────────────


st.divider()

st.subheader("Total organisation prescribing")

with st.spinner("Loading data..."):
    month_data = conn.execute(load_sql("month_data.sql"), [ods_codes]).fetchdf()

col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(x=month_data["month"], y=month_data["items"], mode="lines"))
    fig1.update_layout(
        title="Total number of prescription items for organisation",
        xaxis=dict(type="date"),
        yaxis=dict(title="Items", rangemode="tozero"),
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=month_data["month"], y=month_data["actual_cost"], mode="lines"))
    fig2.update_layout(
        title="Total actual cost for organisation",
        xaxis=dict(type="date"),
        yaxis=dict(title="Cost", rangemode="tozero"),
    )
    st.plotly_chart(fig2, use_container_width=True)


# ── Changelog ─────────────────────────────────────────────────────────────────

st.divider()

st.subheader("Changelog")
with open("changelog.yaml") as f:
    changelog = yaml.safe_load(f)

with st.expander("Click to see changelog"):
    for entry in reversed(changelog):
        st.markdown(f"**{entry['date']}** — {entry['change']} *({entry['person']})*")
