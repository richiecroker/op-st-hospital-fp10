# ── Sidebar part 2: display controls (date range, top_n, sort_by) ────────────
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


# ── Main body: show predecessor organisations (moved out of sidebar) ────────
predecessors = df[
    df["ods_code"].isin(ods_codes) & df["legal_closed_date"].notna()
]
if not predecessors.empty and (sel_prs or sel_icbs or sel_regions):
    parts = [
        f"- {row.ods_name} (closed: {pd.to_datetime(row.legal_closed_date).strftime('%-d %B %Y')})"
        for row in predecessors.itertuples(index=False)
    ]
    noun = "organisation" if len(predecessors) == 1 else "organisations"
    # Use st.info to show it on the main page. You can replace with st.expander if you prefer it collapsed.
    st.info(f"ℹ️ Also includes predecessor {noun}:\n" + "\n".join(parts))
