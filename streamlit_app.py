import streamlit as st
import duckdb
import pandas as pd

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

df = result.copy

st.dataframe(df)
