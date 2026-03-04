
import streamlit as st
import duckdb
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import os

credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
storage_client = storage.Client(credentials=credentials)
bucket = storage_client.bucket("ebmdatalab")
blob = bucket.blob("RC_tests/hospitalfp10.duckdb")
st.write("bucket connected")
st.write(blob.exists())
local_db = "/tmp/app.duckdb"

if not os.path.exists(local_db):
    with st.spinner("Downloading..."):
        blob.download_to_filename(local_db)

conn = duckdb.connect(local_db)
tables = conn.execute("SHOW TABLES").fetchdf()
st.write(tables)))
