# dags/eia_ingest_dag.py
"""
eia_ingest_dag
--------------
Runs daily at 02:00 UTC. For each execution date, pulls one month of data
from the EIA API and lands it in S3 as gzipped JSON.

Why one month per run?
  The EIA API publishes monthly data. Running daily means we always catch
  the latest published month as soon as it appears, without pulling huge
  date ranges. If a run fails, we only re-pull one month — not years of data.

Partition written to S3:
  raw/retail-prices/year=YYYY/month=MM/data.json.gz
  raw/generation/year=YYYY/month=MM/data.json.gz
  raw/rto-demand/year=YYYY/month=MM/data.json.gz
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Make src/ importable inside the container (it's mounted at /opt/airflow/src)
sys.path.insert(0, "/opt/airflow")

from src.eia_client import EIAClient
from src.s3_client import S3Client


# ---------------------------------------------------------------------------
# Default args — applied to every task in this DAG
# ---------------------------------------------------------------------------
default_args = {
    "owner": "himanshu",
    "retries": 3,                          # retry 3 times before failing
    "retry_delay": timedelta(minutes=5),   # wait 5 min between retries
    "email_on_failure": False,             # flip to True once you have SMTP set up
}


# ---------------------------------------------------------------------------
# Helper: derive the target year/month from Airflow's logical execution date.
# Airflow passes the START of the schedule interval as `logical_date`.
# We pull the same month as the execution date.
# ---------------------------------------------------------------------------
def _year_month(logical_date: datetime) -> tuple[str, str]:
    return logical_date.strftime("%Y"), logical_date.strftime("%m")


def _period(logical_date: datetime) -> str:
    """EIA API period string: YYYY-MM"""
    return logical_date.strftime("%Y-%m")


# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------
def ingest_retail_prices(**context):
    logical_date: datetime = context["logical_date"]
    year, month = _year_month(logical_date)
    period = _period(logical_date)

    eia = EIAClient(api_key=os.environ["EIA_API_KEY"])
    s3  = S3Client()

    print(f"[retail-prices] Fetching {period}...")
    records = eia.get_retail_prices(start=period, end=period)
    print(f"[retail-prices] Got {len(records)} records")

    key = s3.upload_json(data=records, dataset="retail-prices", year=year, month=month)
    print(f"[retail-prices] Uploaded → s3://{s3.bucket}/{key}")

    # Push record count to XCom so eia_quality_dag can compare later
    context["ti"].xcom_push(key="retail_prices_count", value=len(records))


def ingest_generation(**context):
    logical_date: datetime = context["logical_date"]
    year, month = _year_month(logical_date)
    period = _period(logical_date)

    eia = EIAClient(api_key=os.environ["EIA_API_KEY"])
    s3  = S3Client()

    print(f"[generation] Fetching {period}...")
    records = eia.get_generation_by_fuel(start=period, end=period)
    print(f"[generation] Got {len(records)} records")

    key = s3.upload_json(data=records, dataset="generation", year=year, month=month)
    print(f"[generation] Uploaded → s3://{s3.bucket}/{key}")

    context["ti"].xcom_push(key="generation_count", value=len(records))


def ingest_rto_demand(**context):
    """
    RTO demand is hourly, so one month = ~720 hours × number of regions.
    We pass the first and last day of the execution month as start/end.
    """
    logical_date: datetime = context["logical_date"]
    year, month = _year_month(logical_date)

    # first day of the month
    start = logical_date.strftime("%Y-%m-%d")
    # last day of the month: first day of next month minus one day
    if logical_date.month == 12:
        end_dt = logical_date.replace(year=logical_date.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        end_dt = logical_date.replace(month=logical_date.month + 1, day=1) - timedelta(days=1)
    end = end_dt.strftime("%Y-%m-%d")

    eia = EIAClient(api_key=os.environ["EIA_API_KEY"])
    s3  = S3Client()

    print(f"[rto-demand] Fetching {start} → {end}...")
    records = eia.get_rto_demand(start=start, end=end)
    print(f"[rto-demand] Got {len(records)} records")

    key = s3.upload_json(data=records, dataset="rto-demand", year=year, month=month)
    print(f"[rto-demand] Uploaded → s3://{s3.bucket}/{key}")

    context["ti"].xcom_push(key="rto_demand_count", value=len(records))


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="eia_ingest_dag",
    description="Daily EIA API → S3 ingestion for retail prices, generation, and RTO demand",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),   # backfill starts here if you enable catchup
    schedule_interval="0 2 * * *",     # 02:00 UTC every day
    catchup=False,                     # don't backfill historical runs on first deploy
    tags=["eia", "ingest", "s3"],
) as dag:

    t_retail = PythonOperator(
        task_id="ingest_retail_prices",
        python_callable=ingest_retail_prices,
    )

    t_generation = PythonOperator(
        task_id="ingest_generation",
        python_callable=ingest_generation,
    )

    t_rto = PythonOperator(
        task_id="ingest_rto_demand",
        python_callable=ingest_rto_demand,
    )

    # All three tasks are independent — run them in parallel
    [t_retail, t_generation, t_rto]
