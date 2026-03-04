import re
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
            result = conn.execute("SELECT COUNT(*) FROM ome_data").fetchone()
            conn.close()
            if result[0] > 0:
                needs_download = False
        except:
            pass
    if needs_download:
        with st.spinner("Loading data from cache..."):
            bucket.blob(gcs_db_path).download_to_filename(local_db)
    return duckdb.connect(local_db)

st.title("🎈 My new app")

try:
    conn = get_duckdb_connection()
    df = conn.execute("SELECT * FROM prescribing").fetchdf()
    st.dataframe(df)
except Exception as e:
    st.error(f"Error: {e}")
