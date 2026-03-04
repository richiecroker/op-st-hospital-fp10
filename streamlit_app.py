
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
