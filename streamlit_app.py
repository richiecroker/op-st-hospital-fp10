
import streamlit as st

import duckdb
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import os

st.write("secrets check")
st.write(st.secrets["gcp_service_account"]["project_id"])
