import shutil
import logging
import os
import re

import duckdb
import pandas as pd
import streamlit as st

from google.cloud import bigquery, storage
from google.oauth2 import service_account

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME      = "ebmdatalab"
CSV_PREFIX       = "hospitalcommunityprescribing/HOSPITAL_DISP_COMMUNITY_"
GCS_DB_PATH      = "hospitalcommunityprescribing/hospitalfp10-dev.duckdb"
LOCAL_DB         = "/tmp/app.duckdb"
SQL_DIR          = os.path.join(os.path.dirname(os.path.abspath(__file__)), "queries")
BQ_ODS_TABLE     = "ebmdatalab.scmd_pipeline.ods"


def _credentials():
    return service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

def _gcs_client():
    return storage.Client(credentials=_credentials())

def _bq_client():
    return bigquery.Client(credentials=_credentials(), project="ebmdatalab")

def _latest_csv_yyyymm(bucket) -> str | None:
    months = []
    blobs = list(bucket.list_blobs(prefix=CSV_PREFIX))
    logger.info("Found %d blobs with prefix %s", len(blobs), CSV_PREFIX)
    for blob in blobs:
        # remove this line: logger.info("Blob: %s", blob.name)
        m = re.search(r"_(\d{6})\.csv$", blob.name)
        if m:
            months.append(m.group(1))
    return max(months) if months else None

def _cached_yyyymm(conn) -> str | None:
    try:
        result = conn.execute(
            "SELECT strftime(MAX(CAST(month AS DATE)), '%Y%m') FROM prescribing"
        ).fetchone()
        return result[0] if result else None
    except Exception:
        return None

def _normalise_df(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if hasattr(df[col].dtype, "name") and "date" in str(df[col].dtype).lower():
            df[col] = pd.to_datetime(df[col]).dt.date
    return df

def _rebuild_prescribing(conn):
    with open(os.path.join(SQL_DIR, "build_prescribing.sql")) as f:
        sql = f.read()
    bq = _bq_client()
    try:
        query_job = bq.query(sql)
        df = _normalise_df(query_job.result().to_dataframe())
    except Exception as e:
        logger.exception("BigQuery error in build_prescribing.sql")
        raise
    conn.execute("DROP TABLE IF EXISTS prescribing")
    conn.register("_tmp", df)
    conn.execute("CREATE TABLE prescribing AS SELECT * FROM _tmp")
    conn.unregister("_tmp")

def _rebuild_ods_mapping(conn):
    bq = _bq_client()
    try:
        df = _normalise_df(bq.query(f"SELECT * FROM `{BQ_ODS_TABLE}`").to_dataframe())
    except Exception as e:
        logger.exception("BigQuery error fetching ODS table")
        raise
    conn.execute("DROP TABLE IF EXISTS ods_mapping")
    conn.register("_tmp", df)
    conn.execute("CREATE TABLE ods_mapping AS SELECT * FROM _tmp")
    conn.unregister("_tmp")

def _save_db_to_gcs(bucket):
    with st.spinner("Saving database to GCS for next time..."):
        tmp = LOCAL_DB + ".upload.tmp"
        try:
            shutil.copy2(LOCAL_DB, tmp)
            blob = bucket.blob(GCS_DB_PATH)
            blob.upload_from_filename(tmp)
            logger.info("Successfully saved DB to GCS at %s", GCS_DB_PATH)
        except Exception as e:
            logger.exception("Failed to save DB to GCS")
            st.error(f"Failed to save DB to GCS: {e}")
        finally:
            if os.path.exists(tmp):
                os.remove(tmp)


@st.cache_resource
def get_duckdb_connection():
    storage_client = _gcs_client()
    bucket = storage_client.bucket(BUCKET_NAME)

    latest_csv = _latest_csv_yyyymm(bucket)
    logger.info("Latest CSV month in GCS: %s", latest_csv)

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

    # Clean up any stale local DB or lock files before rebuild
    for ext in ["", ".wal"]:
        p = LOCAL_DB + ext
        if os.path.exists(p):
            os.remove(p)

    with st.spinner("Rebuilding database from source data - this may take a few minutes..."):
        try:
            conn = duckdb.connect(LOCAL_DB)
            try:
                _rebuild_prescribing(conn)
                _rebuild_ods_mapping(conn)
                conn.checkpoint()
            finally:
                conn.close()
        except Exception as e:
            logger.exception("Failed during DB rebuild")
            st.exception(e)
            raise

    logger.info("DB file exists after rebuild: %s, size: %s",
                os.path.exists(LOCAL_DB),
                os.path.getsize(LOCAL_DB) if os.path.exists(LOCAL_DB) else "N/A")

    if not os.path.exists(LOCAL_DB):
        logger.error("DuckDB file not created at %s", LOCAL_DB)
        return duckdb.connect(LOCAL_DB)

    _save_db_to_gcs(bucket)
    return duckdb.connect(LOCAL_DB)
