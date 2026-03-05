
import streamlit as st
import duckdb
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import plotly.graph_objects as go
import os

st.set_page_config(layout="wide")

@st.cache_resource
def get_duckdb_connection():
    local_db = "/tmp/app.duckdb"
    bucket_name = "ebmdatalab"
    gcs_db_path = "RC_tests/hospitalfp10.duckdb"

    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(bucket_name)

    needs_download = True
    if os.path.exists(local_db):
        try:
            conn = duckdb.connect(local_db)
            result = conn.execute("SELECT COUNT(*) FROM prescribing").fetchone()
            conn.close()
            if result[0] > 0:
                needs_download = False
        except:
            pass

    if needs_download:
        with st.spinner("Downloading database..."):
            bucket.blob(gcs_db_path).download_to_filename(local_db)

    return duckdb.connect(local_db)

st.image("OpenPrescribing.svg")

st.title("Hospital FP10s dispensed in the community viewer")

conn = get_duckdb_connection()
df = conn.execute(
    """
    SELECT ods_name, ods_code, region, icb
    FROM ods_mapping AS ods 
    GROUP BY ods_name, ods_code, region, icb
    """
    ).fetchdf()


ALL = "All"

# Region
region_opts = [ALL] + sorted(df["region"].dropna().unique().tolist())
sel_region = st.selectbox("Region", region_opts, index=0)
df_region = df if sel_region == ALL else df[df["region"] == sel_region]

# ICB (dependent on region)
icb_opts = [ALL] + sorted(df_region["icb"].dropna().unique().tolist())
sel_icb = st.selectbox("ICB", icb_opts, index=0)
df_icb = df_region if sel_icb == ALL else df_region[df_region["icb"] == sel_icb]

# Hospital (dependent on ICB)
pr_pairs = df_icb[["ods_code", "ods_name"]].drop_duplicates().sort_values("ods_name")
pr_opts = [ALL] + [f"{r.ods_name} ({r.ods_code})" for r in pr_pairs.itertuples()]
pr_map = {opt: opt.split(" (")[-1][:-1] for opt in pr_opts if opt != ALL}
sel_pr = st.selectbox("Hospital", pr_opts, index=0)
ods_codes = df_icb["ods_code"].unique().tolist() if sel_pr == ALL else [pr_map[sel_pr]]

# Register as virtual table with duckdb
codes_df = pd.DataFrame({"ods_code": ods_codes})
conn.register("_selected_hospitals", codes_df)

#get data for selected hospitals
month_data = conn.execute("""
    SELECT month, sum(items) AS items, sum(actual_cost) AS actual_cost
    FROM prescribing AS rx
    JOIN _selected_hospitals AS s
        ON CASE 
        WHEN LENGTH(s.ods_code) = 3 THEN LEFT(rx.hospital, 3) = s.ods_code
        ELSE rx.hospital = s.ods_code
        END
    GROUP BY month
    ORDER BY month
""").fetchdf()

top_items_data = conn.execute("""
    SELECT bnf_name, sum(items) as items
    FROM prescribing AS rx
    JOIN _selected_hospitals AS s
        ON CASE 
        WHEN LENGTH(s.ods_code) = 3 THEN LEFT(rx.hospital, 3) = s.ods_code
        ELSE rx.hospital = s.ods_code
        END
    WHERE CAST(month AS DATE) >= (SELECT MAX(CAST(month AS DATE)) FROM prescribing) - INTERVAL '3 months'
    GROUP BY bnf_name
    ORDER BY items DESC
    LIMIT 20
""").fetchdf()

top_cost_data = conn.execute("""
    SELECT bnf_name, sum(actual_cost) as actual_cost
    FROM prescribing AS rx
    JOIN _selected_hospitals AS s
        ON CASE 
        WHEN LENGTH(s.ods_code) = 3 THEN LEFT(rx.hospital, 3) = s.ods_code
        ELSE rx.hospital = s.ods_code
        END
    WHERE CAST(month AS DATE) >= (SELECT MAX(CAST(month AS DATE)) FROM prescribing) - INTERVAL '3 months'
    GROUP BY bnf_name
    ORDER BY actual_cost DESC
    LIMIT 20
""").fetchdf()

#unregister virtual table
conn.unregister("_selected_hospitals")

col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(x=month_data["month"], y=month_data["items"], mode="lines"))
    fig1.update_layout(
        title="Items over Time",
        xaxis=dict(type="date"),
        yaxis=dict(title="Items", rangemode="tozero")
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=month_data["month"], y=month_data["actual_cost"], mode="lines"))
    fig2.update_layout(
        title="Cost over Time",
        xaxis=dict(type="date"),
        yaxis=dict(title="Cost", rangemode="tozero")
    )
    st.plotly_chart(fig2, use_container_width=True)

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 20 items over last 3 months")
    st.dataframe(top_items_data, hide_index=True, height=800)

with col2:
    st.subheader("Top 20 cost items over last 3 months")
    st.dataframe(
    top_cost_data.assign(**{top_cost_data.columns[1]: top_cost_data.iloc[:, 1].map("£{:,.2f}".format)}),
    hide_index=True,
    height=800
    )
