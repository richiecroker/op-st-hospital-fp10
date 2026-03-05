import streamlit as st
import duckdb
from google.cloud import storage
from google.oauth2 import service_account
import pandas as pd
import plotly.graph_objects as go
import os
import logging

logger = logging.getLogger(__name__)

st.set_page_config(layout="wide")


@st.cache_resource
def get_duckdb_connection():
    """Download the DuckDB file from GCS if needed and return a connection.

    NOTE: This connection is shared across all Streamlit sessions (cache_resource).
    Do NOT register virtual tables on it — use parameterised queries or per-query
    temp tables inside a local connection instead.
    """
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
            if result and result[0] > 0:
                needs_download = False
        except Exception as e:
            logger.warning("Existing local DB unusable, will re-download: %s", e)

    if needs_download:
        tmp_path = local_db + ".tmp"
        try:
            with st.spinner("Downloading database..."):
                bucket.blob(gcs_db_path).download_to_filename(tmp_path)
            os.replace(tmp_path, local_db)  # atomic swap — no partial files left behind
        except Exception as e:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            raise RuntimeError(f"Failed to download database from GCS: {e}") from e

    return duckdb.connect(local_db)


def query_month_data(conn: duckdb.DuckDBPyConnection, ods_codes: list[str]) -> pd.DataFrame:
    """Return items and cost aggregated by month for the given ODS codes.

    Passes ods_codes as a query parameter to avoid virtual-table registration,
    which is not thread-safe on a shared connection.
    """
    return conn.execute(
        """
        SELECT
            month,
            sum(items)       AS items,
            sum(actual_cost) AS actual_cost
        FROM prescribing AS rx
        WHERE (
            CASE
                WHEN length(rx.hospital) = 3 THEN rx.hospital
                ELSE left(rx.hospital, 3)
            END
        ) IN (
            SELECT CASE WHEN length(code) = 3 THEN code ELSE left(code, 3) END
            FROM (SELECT unnest($1) AS code)
        )
           OR rx.hospital IN (SELECT unnest($1))
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
        WHERE (
            CASE
                WHEN length(rx.hospital) = 3 THEN rx.hospital
                ELSE left(rx.hospital, 3)
            END
        ) IN (
            SELECT CASE WHEN length(code) = 3 THEN code ELSE left(code, 3) END
            FROM (SELECT unnest($1) AS code)
        )
           OR rx.hospital IN (SELECT unnest($1))
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
        WHERE (
            CASE
                WHEN length(rx.hospital) = 3 THEN rx.hospital
                ELSE left(rx.hospital, 3)
            END
        ) IN (
            SELECT CASE WHEN length(code) = 3 THEN code ELSE left(code, 3) END
            FROM (SELECT unnest($1) AS code)
        )
           OR rx.hospital IN (SELECT unnest($1))
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

# ICB filter — reset if current value no longer valid given region
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

# Hospital filter — reset if current value no longer valid given region+ICB
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

# Resolve which ODS codes to query — use the most specific selection made
if sel_pr != ALL:
    ods_codes = [pr_map[sel_pr]]
elif sel_icb != ALL:
    ods_codes = df_icb["ods_code"].unique().tolist()
elif sel_region != ALL:
    ods_codes = df_region["ods_code"].unique().tolist()
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