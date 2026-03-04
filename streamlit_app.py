import re
import streamlit as st
import duckdb
from google.cloud import storage, bigquery
from google.oauth2 import service_account
import pandas as pd
import itertools
import plotly.graph_objects as go
import os
from datetime import datetime

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
st.write(
    "Let's start building! For help and inspiration, head over to [docs.streamlit.io](https://docs.streamlit.io/)."
)

DB_PATH = "hospitalfp10.duckdb"
conn = duckdb.connect(DB_PATH, read_only=True)

result = conn.execute(
    """
    SELECT
   * FROM prescribing
    """).fetchdf()

df = result.copy()

st.dataframe(df)
