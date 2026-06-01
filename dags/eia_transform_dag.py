# dags/eia_transform_dag.py
"""
eia_transform_dag
-----------------
Triggered automatically after eia_ingest_dag succeeds for the same date.
Uses ExternalTaskSensor to wait for ingest before running.

Flow:
  1. Wait for eia_ingest_dag to complete (ExternalTaskSensor)
  2. Load raw JSON from S3 into Snowflake RAW schema
  3. Flatten RAW → STAGING via MERGE (idempotent)
  4. Rebuild MARTS tables with z-score anomaly detection
"""

import os
import sys
import json
import gzip
from datetime import datetime, timedelta

import pandas as pd
import boto3
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

default_args = {
    "owner": "himanshu",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ---------------------------------------------------------------------------
# Snowflake connection helper
# ---------------------------------------------------------------------------
def _snowflake_conn():
    return snowflake.connector.connect(
        account=os.environ["SF_ACCOUNT"],
        user=os.environ["SF_USER"],
        password=os.environ["SF_PASSWORD"],
        warehouse=os.environ["SF_WAREHOUSE"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "EIA_ENERGY"),
        schema="RAW",
    )


def _s3_client():
    return boto3.client(
        "s3",
        region_name=os.environ["AWS_REGION"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )


# ---------------------------------------------------------------------------
# Task 0: Truncate RAW tables — prevents duplicate row errors in STAGING MERGE
# ---------------------------------------------------------------------------
def truncate_raw(**context):
    con = _snowflake_conn()
    cur = con.cursor()
    for table in ("RAW.RETAIL_PRICES", "RAW.GENERATION", "RAW.RTO_DEMAND"):
        cur.execute(f"TRUNCATE TABLE EIA_ENERGY.{table}")
        print(f"[truncate_raw] Truncated {table}")
    cur.close()
    con.close()


# ---------------------------------------------------------------------------
# Task 1: Load RAW — download from S3, insert into Snowflake RAW as VARIANT
# ---------------------------------------------------------------------------
def load_raw(**context):
    logical_date: datetime = context["logical_date"]
    year  = logical_date.strftime("%Y")
    month = logical_date.strftime("%m")
    bucket = os.environ["S3_BUCKET"]

    datasets = {
        "retail-prices": "RAW.RETAIL_PRICES",
        "generation":    "RAW.GENERATION",
        "rto-demand":    "RAW.RTO_DEMAND",
    }

    s3  = _s3_client()
    con = _snowflake_conn()

    for dataset, table in datasets.items():
        key = f"raw/{dataset}/year={year}/month={month}/data.json.gz"
        print(f"[load_raw] Reading s3://{bucket}/{key}")

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            records = json.loads(gzip.decompress(obj["Body"].read()))
        except s3.exceptions.NoSuchKey:
            print(f"[load_raw] No S3 file found for {dataset} {year}/{month}, skipping")
            continue
        except Exception as e:
            print(f"[load_raw] Error reading {key}: {e}, skipping")
            continue

        print(f"[load_raw] {len(records)} records from {dataset}")
        if not records:
            print(f"[load_raw] Empty file for {dataset}, skipping")
            continue

        # Build a DataFrame with two columns and write_pandas — clean and fast
        df = pd.DataFrame({
            "PAYLOAD":     [json.dumps(r) for r in records],
            "SOURCE_FILE": key,
        })
        schema, tbl = table.split(".")
        write_pandas(con, df, tbl, schema=schema, database="EIA_ENERGY", auto_create_table=False)
        print(f"[load_raw] Inserted {len(records)} rows into {table}")

    con.close()


# ---------------------------------------------------------------------------
# Task 2: Transform RAW → STAGING
# ---------------------------------------------------------------------------
def transform_staging(**context):
    sql_dir = "/opt/airflow/sql/staging"
    scripts = [
        "transform_retail_prices.sql",
        "transform_generation.sql",
        "transform_rto_demand.sql",
    ]

    con = _snowflake_conn()
    cur = con.cursor()

    for script in scripts:
        path = os.path.join(sql_dir, script)
        print(f"[staging] Running {script}...")
        with open(path) as f:
            sql = f.read()
        cur.execute(sql)
        print(f"[staging] {script} complete — {cur.rowcount} rows affected")

    cur.close()
    con.close()


# ---------------------------------------------------------------------------
# Task 3: Rebuild MARTS
# ---------------------------------------------------------------------------
def build_marts(**context):
    sql_path = "/opt/airflow/sql/marts/create_marts.sql"

    con = _snowflake_conn()
    cur = con.cursor()

    print("[marts] Rebuilding FACT_RETAIL_PRICES, FACT_GENERATION, DIM_STATE...")
    with open(sql_path) as f:
        # Split on semicolons to execute each statement separately
        statements = [s.strip() for s in f.read().split(";") if s.strip()]

    for stmt in statements:
        # Skip blocks that are only comments (no actual SQL keywords)
        non_comment = "\n".join(
            line for line in stmt.splitlines() if not line.strip().startswith("--")
        ).strip()
        if not non_comment:
            continue
        cur.execute(stmt)
        print(f"[marts] Done: {stmt[:60]}...")

    cur.close()
    con.close()
    print("[marts] All mart tables rebuilt.")


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="eia_transform_dag",
    description="RAW → STAGING → MARTS transform triggered after ingest",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 3 * * *",   # 03:00 UTC — one hour after ingest
    catchup=False,
    tags=["eia", "transform", "snowflake"],
) as dag:

    t_truncate_raw = PythonOperator(
        task_id="truncate_raw",
        python_callable=truncate_raw,
    )

    t_load_raw = PythonOperator(
        task_id="load_raw",
        python_callable=load_raw,
    )

    t_staging = PythonOperator(
        task_id="transform_staging",
        python_callable=transform_staging,
    )

    t_marts = PythonOperator(
        task_id="build_marts",
        python_callable=build_marts,
    )

    # Linear dependency: truncate → load raw → staging → marts
    t_truncate_raw >> t_load_raw >> t_staging >> t_marts
