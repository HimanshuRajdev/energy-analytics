"""
run_transforms.py
-----------------
Reloads RAW from S3 and runs staging + marts transforms.
Run after truncating RAW and STAGING tables in Snowflake.
"""
import gzip, json, os
from datetime import date
from dateutil.relativedelta import relativedelta

import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

s3 = boto3.client(
    "s3",
    region_name=os.environ["AWS_REGION"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)
bucket = os.environ["S3_BUCKET"]

con = snowflake.connector.connect(
    account=os.environ["SF_ACCOUNT"],
    user=os.environ["SF_USER"],
    password=os.environ["SF_PASSWORD"],
    warehouse=os.environ["SF_WAREHOUSE"],
    database=os.environ.get("SNOWFLAKE_DATABASE", "EIA_ENERGY"),
    schema="RAW",
)
cur = con.cursor()

START = date(2020, 1, 1)
END   = date(2024, 12, 1)

datasets = {
    "retail-prices": "RETAIL_PRICES",
    "generation":    "GENERATION",
    "rto-demand":    "RTO_DEMAND",
}

# ── Load RAW from S3 ──────────────────────────────────────────────────────────
d = START
while d <= END:
    year  = d.strftime("%Y")
    month = d.strftime("%m")
    period = d.strftime("%Y-%m")

    for dataset, table in datasets.items():
        key = f"raw/{dataset}/year={year}/month={month}/data.json.gz"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            records = json.loads(gzip.decompress(obj["Body"].read()))
            if not records:
                continue
            df = pd.DataFrame({
                "PAYLOAD":     [json.dumps(r) for r in records],
                "SOURCE_FILE": key,
            })
            write_pandas(con, df, table, schema="RAW", database="EIA_ENERGY", auto_create_table=False)
        except Exception as e:
            if "NoSuchKey" not in str(e) and "does not exist" not in str(e):
                print(f"  WARN {period} {dataset}: {e}")

    print(f"{period}: loaded")
    d += relativedelta(months=1)

# ── Staging ───────────────────────────────────────────────────────────────────
print("\nRunning staging transforms...")
for script in ["transform_retail_prices.sql", "transform_generation.sql", "transform_rto_demand.sql"]:
    with open(f"sql/staging/{script}") as f:
        cur.execute(f.read())
    print(f"  {script}: {cur.rowcount} rows")

# ── Marts ─────────────────────────────────────────────────────────────────────
print("\nRebuilding marts...")
with open("sql/marts/create_marts.sql") as f:
    for stmt in [s.strip() for s in f.read().split(";") if s.strip()]:
        cur.execute(stmt)

# ── Counts ────────────────────────────────────────────────────────────────────
print("\nFINAL COUNTS:")
for table in ["RAW.RETAIL_PRICES", "RAW.GENERATION", "RAW.RTO_DEMAND",
              "STAGING.RETAIL_PRICES", "STAGING.GENERATION", "STAGING.RTO_DEMAND",
              "MARTS.FACT_RETAIL_PRICES", "MARTS.FACT_GENERATION", "MARTS.DIM_STATE"]:
    cur.execute(f"SELECT COUNT(*) FROM EIA_ENERGY.{table}")
    print(f"  {table}: {cur.fetchone()[0]:,}")

cur.close()
con.close()
print("\nDone.")
