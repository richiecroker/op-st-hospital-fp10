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
GCS_DB_PATH      = "hospitalcommunityprescribing/hospitalfp10-dev.duckdb"
LOCAL_DB         = "/tmp/app.duckdb"
SQL_PRESCRIBING  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries", "build_prescribing.sql")
BQ_ODS_TABLE     = "ebmdatalab.scmd_pipeline.ods"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _credentials():
    return service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])


def _gcs_client():
   
    return storage.Client(credentials=_credentials())


def _bq_client():
    return bigquery.Client(credentials=_credentials(), project="ebmdatalab")


def _latest_csv_yyyymm(bucket) -> str | None: #Return the latest yyyymm suffix found among CSVs in GCS
    months = []
    for blob in bucket.list_blobs(prefix=CSV_PREFIX):
        m = re.search(r"_(\d{6})\.csv$", blob.name)
        if m:
            months.append(m.group(1))
    return max(months) if months else None


def _cached_yyyymm(conn) -> str | None: #Return the latest yyyymm stored in the local DuckDB prescribing table
    try:
        result = conn.execute(
            "SELECT strftime(MAX(CAST(month AS DATE)), '%Y%m') FROM prescribing"
        ).fetchone()
        return result[0] if result else None
    except Exception:
        return None


def _normalise_df(df: pd.DataFrame) -> pd.DataFrame: #Convert BQ-specific types that DuckDB doesn't recognise (e.g. dbdate) to standard types.
    for col in df.columns:
        if hasattr(df[col].dtype, "name") and "date" in str(df[col].dtype).lower():
            df[col] = pd.to_datetime(df[col]).dt.date
    return df


def _rebuild_prescribing(conn): #Pull pre-aggregated prescribing data from BigQuery using build_prescribing.sql.
    with open(SQL_PRESCRIBING) as f:
        sql = f.read()
    bq = _bq_client()
    df = _normalise_df(bq.query(sql).to_dataframe())
    conn.execute("DROP TABLE IF EXISTS prescribing")
    conn.register("_tmp", df)
    conn.execute("CREATE TABLE prescribing AS SELECT * FROM _tmp")
    conn.unregister("_tmp")


def _rebuild_ods_mapping(conn): #Pull ods_mapping from BigQuery into DuckDB.
    bq = _bq_client()
    df = _normalise_df(bq.query(f"SELECT * FROM `{BQ_ODS_TABLE}`").to_dataframe())
    conn.execute("DROP TABLE IF EXISTS ods_mapping")
    conn.register("_tmp", df)
    conn.execute("CREATE TABLE ods_mapping AS SELECT * FROM _tmp")
    conn.unregister("_tmp")


def _save_db_to_gcs(bucket): #Upload the local DuckDB to GCS so the next cold start can skip a rebuild.
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
            tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
            if _cached_yyyymm(conn) == latest_csv and "ods_mapping" in tables and "prescribing" in tables:
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
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        if _cached_yyyymm(conn) == latest_csv and "ods_mapping" in tables and "prescribing" in tables:
            logger.info("GCS-cached DuckDB is up to date, using it.")
            return conn
        logger.info("GCS-cached DuckDB is stale or missing tables, doing full rebuild.")
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
        WHERE rx.hospital = ANY($1)
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
        WHERE rx.hospital = ANY($1)
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
        WHERE rx.hospital = ANY($1)
        AND CAST(month AS DATE) >= (SELECT MAX(CAST(month AS DATE)) FROM prescribing) - INTERVAL '3 months'
        GROUP BY bnf_name
        ORDER BY actual_cost DESC
        LIMIT 20
        """,
        [ods_codes],
    ).fetchdf()


# ── UI ────────────────────────────────────────────────────────────────────────

st.image("OpenPrescribing.svg")

st.info("""##### Hello!  This is a **very** early prototype of analysing hospital FP10s that have been dispensed in the community.  
Please let use know what you think, and what you've like to see.  Email us at [bennett@phc.ox.ac.uk](mailto:bennett@phc.ox.ac.uk)""")

st.title("Hospital FP10s dispensed in the community viewer")

conn = get_duckdb_connection()

df = conn.execute("SELECT * FROM ods_mapping").fetchdf()
df["ultimate_successors"] = df["ultimate_successors"].apply(
    lambda x: list(x) if isinstance(x, np.ndarray) else ([] if x is None else x)
)

# Initialise session state (empty list = no filter = show all)
if "sel_region" not in st.session_state:
    st.session_state.sel_region = []
if "sel_icb" not in st.session_state:
    st.session_state.sel_icb = []
if "sel_pr" not in st.session_state:
    st.session_state.sel_pr = []

# Region filter
region_opts = sorted(df["region"].dropna().unique().tolist())
sel_regions = [v for v in st.session_state.get("sel_region", []) if v in region_opts]
sel_regions = st.multiselect("Region", region_opts, default=sel_regions, key="sel_region")
df_region = df if not sel_regions else df[df["region"].isin(sel_regions)]

# ICB filter
icb_opts = sorted(df_region["icb"].dropna().unique().tolist())
sel_icbs = [v for v in st.session_state.get("sel_icb", []) if v in icb_opts]
sel_icbs = st.multiselect("ICB", icb_opts, default=sel_icbs, key="sel_icb")
df_icb = df_region if not sel_icbs else df_region[df_region["icb"].isin(sel_icbs)]

# Hospital filter - only show open trusts
pr_pairs = (
    df_icb[df_icb["legal_closed_date"].isna()][["ods_code", "ods_name"]]
    .drop_duplicates()
    .sort_values("ods_name")
)
pr_map: dict[str, str] = {
    f"{row.ods_name} ({row.ods_code})": row.ods_code
    for row in pr_pairs.itertuples(index=False)
}
pr_opts = list(pr_map.keys())
sel_prs = [v for v in st.session_state.get("sel_pr", []) if v in pr_opts]
sel_prs = st.multiselect("Hospital Trust", pr_opts, default=sel_prs, key="sel_pr")

# Build a lookup: successor ODS code -> all predecessor ODS codes (where ultimate_successor = that code)
def resolve_ods_codes(selected_codes: list[str], df_full: pd.DataFrame) -> list[str]:
    all_codes = set(selected_codes)
    for code in selected_codes:
        mask = df_full["ultimate_successors"].apply(lambda x: code in x)
        predecessors = df_full[
            df_full["legal_closed_date"].notna() & mask
        ]["ods_code"].tolist()
        all_codes.update(predecessors)
    return list(all_codes)

# Resolve which ODS codes to query - use the most specific selection made
if sel_prs:
    direct_codes = [pr_map[p] for p in sel_prs]
    ods_codes = resolve_ods_codes(direct_codes, df)  # use full df, not df_icb
elif sel_icbs:
    ods_codes = resolve_ods_codes(df_icb["ods_code"].unique().tolist(), df)
elif sel_regions:
    ods_codes = resolve_ods_codes(df_region["ods_code"].unique().tolist(), df)
else:
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