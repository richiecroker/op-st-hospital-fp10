
import shutil
import logging
import os
import re

import duckdb
import plotly.graph_objects as go
import pandas as pd
import streamlit as st
from google.cloud import bigquery, storage
from google.oauth2 import service_account

logger = logging.getLogger(__name__)

st.set_page_config(layout="wide")

# ── Constants ─────────────────────────────────────────────────────────────────

BUCKET_NAME      = "ebmdatalab"
CSV_PREFIX       = "RC_tests/HOSPITAL_DISP_COMMUNITY_"  # blobs end in _yyyymm.csv
GCS_DB_PATH      = "hospitalcommunityprescribing/hospitalfp10.duckdb"
LOCAL_DB         = "/tmp/app.duckdb"
SQL_PRESCRIBING  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sql", "build_prescribing.sql")
BQ_ODS_TABLE     = "ebmdatalab.scmd.ods_mapped"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _credentials():
    return service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )


def _gcs_client():
    return storage.Client(credentials=_credentials())


def _bq_client():
    return bigquery.Client(credentials=_credentials(), project="ebmdatalab")


def _latest_csv_yyyymm(bucket) -> str | None:
    """Return the latest yyyymm suffix found among CSVs in GCS, e.g. '202503'."""
    months = []
    for blob in bucket.list_blobs(prefix=CSV_PREFIX):
        m = re.search(r"_(\d{6})\.csv$", blob.name)
        if m:
            months.append(m.group(1))
    return max(months) if months else None


def _cached_yyyymm(conn) -> str | None:
    """Return the latest yyyymm stored in the local DuckDB prescribing table."""
    try:
        result = conn.execute(
            "SELECT strftime(MAX(CAST(month AS DATE)), '%Y%m') FROM prescribing"
        ).fetchone()
        return result[0] if result else None
    except Exception:
        return None


def _rebuild_prescribing(conn):
    """Pull pre-aggregated prescribing data from BigQuery using build_prescribing.sql."""
    with open(SQL_PRESCRIBING) as f:
        sql = f.read()
    bq = _bq_client()
    df = bq.query(sql).to_dataframe()
    conn.execute("DROP TABLE IF EXISTS prescribing")
    conn.register("_tmp", df)
    conn.execute("CREATE TABLE prescribing AS SELECT * FROM _tmp")
    conn.unregister("_tmp")


def _rebuild_ods_mapping(conn):
    """Pull ods_mapping from BigQuery into DuckDB."""
    bq = _bq_client()
    df = bq.query(f"SELECT * FROM `{BQ_ODS_TABLE}`").to_dataframe()
    conn.execute("DROP TABLE IF EXISTS ods_mapping")
    conn.register("_tmp", df)
    conn.execute("CREATE TABLE ods_mapping AS SELECT * FROM _tmp")
    conn.unregister("_tmp")


def _save_db_to_gcs(bucket):
    """Upload the local DuckDB to GCS so the next cold start can skip a rebuild."""
    with st.spinner("Saving database to GCS for next time..."):
        tmp = LOCAL_DB + ".upload.tmp"
        shutil.copy2(LOCAL_DB, tmp)
        try:
            bucket.blob(GCS_DB_PATH).upload_from_filename(tmp)
        finally:
            os.remove(tmp)


# ── DB bootstrap ──────────────────────────────────────────────────────────────

@st.cache_resource
def get_duckdb_connection():
    """Return a ready DuckDB connection, rebuilding from source if stale or absent.

    Logic:
    1. If a local DB exists and its latest month matches the latest CSV in GCS -> reuse it.
    2. Else try downloading the GCS-cached DuckDB and check freshness again (fast path).
    3. If still stale or missing -> full rebuild from BQ, then save back to GCS.

    NOTE: Connection is shared across Streamlit sessions (cache_resource).
    Never register virtual tables on it - use parameterised queries instead.
    """
    storage_client = _gcs_client()
    bucket = storage_client.bucket(BUCKET_NAME)

    latest_csv = _latest_csv_yyyymm(bucket)
    logger.info("Latest CSV month in GCS: %s", latest_csv)

    # 1. Check local cache
    if os.path.exists(LOCAL_DB):
        try:
            conn = duckdb.connect(LOCAL_DB)
            if _cached_yyyymm(conn) == latest_csv:
                logger.info("Local DuckDB is up to date, reusing.")
                return conn
            conn.close()
            logger.info("Local DuckDB is stale.")
        except Exception as e:
            logger.warning("Local DuckDB unusable: %s", e)

    # 2. Try GCS-cached DuckDB
    tmp_path = LOCAL_DB + ".tmp"
    try:
        with st.spinner("Downloading cached database..."):
            bucket.blob(GCS_DB_PATH).download_to_filename(tmp_path)
        os.replace(tmp_path, LOCAL_DB)
        conn = duckdb.connect(LOCAL_DB)
        if _cached_yyyymm(conn) == latest_csv:
            logger.info("GCS-cached DuckDB is up to date, using it.")
            return conn
        logger.info("GCS-cached DuckDB is also stale, doing full rebuild.")
        conn.close()
    except Exception as e:
        logger.info("No usable GCS-cached DuckDB (%s), doing full rebuild.", e)
        if os.path.exists(tmp_path):
            os.remove(tmp_path)

    # 3. Full rebuild from BigQuery
    # Remove any stale local DB first to ensure a clean connection
    if os.path.exists(LOCAL_DB):
        os.remove(LOCAL_DB)

    with st.spinner("Rebuilding database from source data - this may take a few minutes..."):
        conn = duckdb.connect(LOCAL_DB)
        _rebuild_prescribing(conn)
        _rebuild_ods_mapping(conn)
        conn.checkpoint()  # flush all writes to disk before uploading

    _save_db_to_gcs(bucket)
    return conn


# ── Query helpers ─────────────────────────────────────────────────────────────

def query_month_data(conn: duckdb.DuckDBPyConnection, ods_codes: list[str]) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT month, sum(items) AS items, sum(actual_cost) AS actual_cost
        FROM prescribing AS rx
        WHERE EXISTS (
            SELECT 1 FROM (SELECT unnest($1) AS code)
            WHERE LEFT(rx.hospital, LENGTH(code)) = code
        )
        GROUP BY month
        ORDER BY month
        """,
        [ods_codes],
    ).fetchdf()


def query_top_items(conn: duckdb.DuckDBPyConnection, ods_codes: list[str]) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT bnf_name, sum(items) AS items
        FROM prescribing AS rx
        WHERE EXISTS (
            SELECT 1 FROM (SELECT unnest($1) AS code)
            WHERE LEFT(rx.hospital, LENGTH(code)) = code
        )
          AND CAST(month AS DATE) >= (SELECT MAX(CAST(month AS DATE)) FROM prescribing) - INTERVAL '3 months'
        GROUP BY bnf_name
        ORDER BY items DESC
        LIMIT 20
        """,
        [ods_codes],
    ).fetchdf()


def query_top_cost(conn: duckdb.DuckDBPyConnection, ods_codes: list[str]) -> pd.DataFrame:
    return conn.execute(
        """
        SELECT bnf_name, sum(actual_cost) AS actual_cost
        FROM prescribing AS rx
        WHERE EXISTS (
            SELECT 1 FROM (SELECT unnest($1) AS code)
            WHERE LEFT(rx.hospital, LENGTH(code)) = code
        )
          AND CAST(month AS DATE) >= (SELECT MAX(CAST(month AS DATE)) FROM prescribing) - INTERVAL '3 months'
        GROUP BY bnf_name
        ORDER BY actual_cost DESC
        LIMIT 20
        """,
        [ods_codes],
    ).fetchdf()


# ── UI ────────────────────────────────────────────────────────────────────────

st.image("OpenPrescribing.svg")
st.title("Hospital FP10s dispensed in the community viewer")

conn = get_duckdb_connection()

df = conn.execute(
    """
    SELECT ods_name, ods_code, region, icb
    FROM ods_mapping
    GROUP BY ods_name, ods_code, region, icb
    """
).fetchdf()

ALL = "All"

# Initialise session state for selections
if "sel_region" not in st.session_state:
    st.session_state.sel_region = ALL
if "sel_icb" not in st.session_state:
    st.session_state.sel_icb = ALL
if "sel_pr" not in st.session_state:
    st.session_state.sel_pr = ALL

# Region filter
region_opts = [ALL] + sorted(df["region"].dropna().unique().tolist())
sel_region = st.selectbox(
    "Region",
    region_opts,
    index=region_opts.index(st.session_state.sel_region) if st.session_state.sel_region in region_opts else 0,
    key="sel_region",
)
df_region = df if sel_region == ALL else df[df["region"] == sel_region]

# ICB filter - reset if current value no longer valid given region
icb_opts = [ALL] + sorted(df_region["icb"].dropna().unique().tolist())
if st.session_state.sel_icb not in icb_opts:
    st.session_state.sel_icb = ALL
sel_icb = st.selectbox(
    "ICB",
    icb_opts,
    index=icb_opts.index(st.session_state.sel_icb),
    key="sel_icb",
)
df_icb = df_region if sel_icb == ALL else df_region[df_region["icb"] == sel_icb]

# Hospital filter - reset if current value no longer valid given region+ICB
pr_pairs = (
    df_icb[["ods_code", "ods_name"]]
    .drop_duplicates()
    .sort_values("ods_name")
)
pr_map: dict[str, str] = {
    f"{row.ods_name} ({row.ods_code})": row.ods_code
    for row in pr_pairs.itertuples(index=False)
}
pr_opts = [ALL] + list(pr_map.keys())
if st.session_state.sel_pr not in pr_opts:
    st.session_state.sel_pr = ALL
sel_pr = st.selectbox(
    "Hospital",
    pr_opts,
    index=pr_opts.index(st.session_state.sel_pr),
    key="sel_pr",
)

# Resolve which ODS codes to query - use the most specific selection made
if sel_pr != ALL:
    ods_codes = [pr_map[sel_pr]]
elif sel_icb != ALL:
    ods_codes = df_icb["ods_code"].unique().tolist()
elif sel_region != ALL:
    ods_codes = df_region["ods_code"].unique().tolist()
else:
    ods_codes = df["ods_code"].unique().tolist()

if not ods_codes:
    ods_codes = df["ods_code"].unique().tolist()

# ── Data queries ──────────────────────────────────────────────────────────────

with st.spinner("Loading data..."):
    month_data = query_month_data(conn, ods_codes)
    top_items_data = query_top_items(conn, ods_codes)
    top_cost_data = query_top_cost(conn, ods_codes)

# ── Charts ────────────────────────────────────────────────────────────────────

col1, col2 = st.columns(2)

with col1:
    fig1 = go.Figure()
    fig1.add_trace(go.Scatter(x=month_data["month"], y=month_data["items"], mode="lines"))
    fig1.update_layout(
        title="Items over Time",
        xaxis=dict(type="date"),
        yaxis=dict(title="Items", rangemode="tozero"),
    )
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(x=month_data["month"], y=month_data["actual_cost"], mode="lines"))
    fig2.update_layout(
        title="Cost over Time",
        xaxis=dict(type="date"),
        yaxis=dict(title="Cost", rangemode="tozero"),
    )
    st.plotly_chart(fig2, use_container_width=True)

# ── Tables ────────────────────────────────────────────────────────────────────

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top 20 items over last 3 months")
    st.dataframe(top_items_data, hide_index=True, height=740)

with col2:
    st.subheader("Top 20 cost items over last 3 months")
    st.dataframe(
        top_cost_data.assign(actual_cost=top_cost_data["actual_cost"].map("£{:,.2f}".format)),
        hide_index=True,
        height=740,
    )