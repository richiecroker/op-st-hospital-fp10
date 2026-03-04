
import streamlit as st
import duckdb
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import os

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


st.title("🎈 My new app")

conn = get_duckdb_connection()
df = conn.execute(
    """
    SELECT *
    FROM prescribing AS rx
    INNER JOIN ods_mapping AS ods 
    ON CASE 
        WHEN LENGTH(ods.ods_code) = 3 THEN LEFT(rx.hospital, 3) = ods.ods_code
        ELSE rx.hospital = ods.ods_code
        END
    """
    ).fetchdf()
st.dataframe(df)
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
