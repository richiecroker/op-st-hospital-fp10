import streamlit as st
import duckdb
import pandas as pd
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
DB_PATH = "/tmp/duckdb_builder.duckdb"

st.set_page_config(page_title="CSV Loader", page_icon="📂", layout="centered")
st.title("📂 CSV Loader")


def get_tables():
    try:
        con = duckdb.connect(DB_PATH)
        tables = con.execute("SHOW TABLES").fetchdf()
        con.close()
        return tables
    except:
        return pd.DataFrame(columns=["name"])


def get_table_info(table_name):
    try:
        con = duckdb.connect(DB_PATH)
        count = con.execute(f"SELECT COUNT(*) FROM '{table_name}'").fetchone()[0]
        cols = con.execute(f"DESCRIBE '{table_name}'").fetchdf()
        con.close()
        return count, cols
    except:
        return 0, pd.DataFrame()


# --- Upload section ---
st.subheader("Add a table")

uploaded_file = st.file_uploader("Upload a CSV", type="csv")
table_name = st.text_input("Table name", placeholder="e.g. users, orders, products")

if st.button("➕ Load Table", type="primary", disabled=not (uploaded_file and table_name.strip())):
    clean_name = table_name.strip().replace(" ", "_").lower()
    df = pd.read_csv(uploaded_file)
    con = duckdb.connect(DB_PATH)
    con.execute(f"CREATE OR REPLACE TABLE {clean_name} AS SELECT * FROM df")
    con.close()
    st.success(f"Loaded **{clean_name}** — {len(df):,} rows, {len(df.columns)} columns")
    st.rerun()

st.divider()

# --- Tables loaded so far ---
st.subheader("Loaded tables")

tables = get_tables()

if tables.empty:
    st.caption("No tables loaded yet.")
else:
    for _, row in tables.iterrows():
        name = row["name"]
        count, cols = get_table_info(name)
        with st.expander(f"**{name}** — {count:,} rows · {len(cols)} columns"):
            st.dataframe(cols[["column_name", "column_type"]], hide_index=True, use_container_width=True)
            if st.button(f"🗑 Drop {name}", key=f"drop_{name}"):
                con = duckdb.connect(DB_PATH)
                con.execute(f"DROP TABLE IF EXISTS {name}")
                con.close()
                st.rerun()

st.divider()

# --- Download section ---
st.subheader("Download")

if not tables.empty:
     with open(DB_PATH, "rb") as f:
        data = f.read()
    
    st.download_button(
        label="⬇️ Download .duckdb file",
        data=data,
        file_name="output.duckdb",
        mime="application/octet-stream",
        type="primary",
    )
        )
else:
    st.caption("Load at least one table to enable download.")
