# dags/eia_pipeline_dag.py
"""
eia_pipeline_dag
----------------
One monthly DAG, EIA API -> S3 -> BigQuery (RAW -> STAGING -> MARTS) -> Google Sheet.

  ingest (3 in parallel) -> setup tables -> load RAW -> staging MERGE -> marts -> validate -> publish sheet

Each run re-pulls the last `lookback_months` months (default 3). See src/pipeline.py for why.
Airflow runs locally, so if the laptop is off on the scheduled day, the missed run starts
the next time Docker is up.
For a one-time catch-up, trigger the DAG manually with a bigger value, for example 9.
"""
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow")

from src import pipeline
from src.bq_client import BQ
from src.eia_client import EIAClient
from src.s3_client import S3Client

default_args = {
    "owner": "himanshu",
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}


def _window(context):
    anchor = context["data_interval_end"].date()
    months = pipeline.month_window(anchor, int(context["params"]["lookback_months"]))
    return months, anchor


def _ingest(dataset, **context):
    months, anchor = _window(context)
    eia = EIAClient(api_key=os.environ["EIA_API_KEY"])
    counts = pipeline.ingest_dataset(dataset, eia, S3Client(), months, anchor)
    print(f"[{dataset}] uploaded months: {counts}")


def _setup(**context):
    pipeline.setup_tables(BQ())


def _load_raw(**context):
    months, _ = _window(context)
    print(pipeline.load_raw(S3Client(), BQ(), months))


def _staging(**context):
    pipeline.run_staging(BQ())


def _marts(**context):
    pipeline.run_marts(BQ())


def _validate(**context):
    print(pipeline.validate(BQ()))


def _publish(**context):
    print(pipeline.publish_sheet(BQ()))


with DAG(
    dag_id="eia_pipeline_dag",
    description="Monthly EIA -> S3 -> BigQuery -> Google Sheet pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 14 2 * *",  # 2nd of each month, 14:00 UTC
    catchup=False,
    max_active_runs=1,
    params={"lookback_months": Param(3, type="integer", minimum=0, maximum=84)},
    tags=["eia", "s3", "bigquery", "sheets"],
) as dag:

    ingest_tasks = [
        PythonOperator(
            task_id=f"ingest_{name.replace('-', '_')}",
            python_callable=_ingest,
            op_kwargs={"dataset": name},
        )
        for name in pipeline.DATASETS
    ]
    t_setup = PythonOperator(task_id="setup_tables", python_callable=_setup)
    t_load = PythonOperator(task_id="load_raw", python_callable=_load_raw)
    t_staging = PythonOperator(task_id="transform_staging", python_callable=_staging)
    t_marts = PythonOperator(task_id="build_marts", python_callable=_marts)
    t_validate = PythonOperator(task_id="validate", python_callable=_validate)
    t_publish = PythonOperator(task_id="publish_sheet", python_callable=_publish)

    ingest_tasks >> t_setup >> t_load >> t_staging >> t_marts >> t_validate >> t_publish
