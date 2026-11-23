"""
One-shot script: reads retail-prices 2020-01 from S3, loads into Snowflake RAW,
runs the staging transform, rebuilds marts. Run this directly to confirm the
full pipeline works before relying on Airflow for scheduling.
"""
import gzip, json, os
import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

# --- S3 ---
s3 = boto3.client(
    "s3",
    region_name=os.environ["AWS_REGION"],
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)
bucket = os.environ["S3_BUCKET"]

# --- Snowflake ---
con = snowflake.connector.connect(
    account=os.environ["SF_ACCOUNT"],
    user=os.environ["SF_USER"],
    password=os.environ["SF_PASSWORD"],
    warehouse=os.environ["SF_WAREHOUSE"],
    database=os.environ.get("SNOWFLAKE_DATABASE", "EIA_ENERGY"),
    schema="RAW",
)
cur = con.cursor()

# ── Step 1: Load RAW ─────────────────────────────────────────────────────────
files = [
    ("raw/retail-prices/year=2020/month=01/data.json.gz", "RETAIL_PRICES"),
    ("raw/retail-prices/year=2020/month=02/data.json.gz", "RETAIL_PRICES"),
    ("raw/retail-prices/year=2020/month=03/data.json.gz", "RETAIL_PRICES"),
]

for key, table in files:
    print(f"Reading s3://{bucket}/{key} ...")
    obj = s3.get_object(Bucket=bucket, Key=key)
    records = json.loads(gzip.decompress(obj["Body"].read()))
    print(f"  {len(records)} records")

    if not records:
        print("  Empty — skipping")
        continue

    df = pd.DataFrame({
        "PAYLOAD":     [json.dumps(r) for r in records],
        "SOURCE_FILE": key,
    })
    success, nchunks, nrows, _ = write_pandas(
        con, df, table, schema="RAW", database="EIA_ENERGY", auto_create_table=False
    )
    print(f"  write_pandas success={success} rows={nrows}")

# Verify
cur.execute("SELECT COUNT(*) FROM EIA_ENERGY.RAW.RETAIL_PRICES")
print(f"\nRAW.RETAIL_PRICES row count: {cur.fetchone()[0]}")

# ── Step 2: Staging transform ────────────────────────────────────────────────
print("\nRunning staging transform...")
with open("sql/staging/transform_retail_prices.sql") as f:
    sql = f.read()
cur.execute(sql)
print(f"  Rows merged into STAGING: {cur.rowcount}")

cur.execute("SELECT COUNT(*) FROM EIA_ENERGY.STAGING.RETAIL_PRICES")
print(f"STAGING.RETAIL_PRICES row count: {cur.fetchone()[0]}")

# ── Step 3: Marts ────────────────────────────────────────────────────────────
print("\nRebuilding marts...")
with open("sql/marts/create_marts.sql") as f:
    statements = [s.strip() for s in f.read().split(";") if s.strip()]
for stmt in statements:
    cur.execute(stmt)

cur.execute("SELECT COUNT(*) FROM EIA_ENERGY.MARTS.FACT_RETAIL_PRICES")
print(f"MARTS.FACT_RETAIL_PRICES row count: {cur.fetchone()[0]}")

cur.close()
con.close()
print("\nDone.")
