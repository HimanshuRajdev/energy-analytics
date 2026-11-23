"""
backfill.py
-----------
One-time script to pull 2020-01 through 2024-12 from EIA and upload to S3.
Also loads everything into Snowflake RAW, STAGING, and MARTS.

Run once from your terminal:
    python backfill.py

Takes ~10-15 minutes. Progress is printed for each month.
"""
import gzip, json, os, time
from datetime import date
from dateutil.relativedelta import relativedelta

import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

from src.eia_client import EIAClient

load_dotenv()

# ── Clients ──────────────────────────────────────────────────────────────────
eia = EIAClient(api_key=os.environ["EIA_API_KEY"])

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


# ── Helpers ───────────────────────────────────────────────────────────────────
def upload_to_s3(records: list, dataset: str, year: str, month: str) -> str:
    key = f"raw/{dataset}/year={year}/month={month}/data.json.gz"
    body = gzip.compress(json.dumps(records, default=str).encode("utf-8"))
    s3.put_object(Bucket=bucket, Key=key, Body=body,
                  ContentType="application/json", ContentEncoding="gzip")
    return key


def load_to_raw(records: list, table: str, key: str):
    if not records:
        return 0
    df = pd.DataFrame({
        "PAYLOAD":     [json.dumps(r) for r in records],
        "SOURCE_FILE": key,
    })
    _, _, nrows, _ = write_pandas(
        con, df, table, schema="RAW",
        database="EIA_ENERGY", auto_create_table=False
    )
    return nrows


def run_sql_file(path: str):
    with open(path) as f:
        sql = f.read()
    cur.execute(sql)
    return cur.rowcount


# ── Date range ────────────────────────────────────────────────────────────────
START = date(2020, 1, 1)
END   = date(2026, 4, 1)

months = []
d = START
while d <= END:
    months.append(d)
    d += relativedelta(months=1)

print(f"Backfilling {len(months)} months ({START.strftime('%Y-%m')} → {END.strftime('%Y-%m')})")
print("=" * 60)

# ── Pull retail prices for full range in one call (EIA paginates) ─────────────
print("\n[1/3] Fetching retail prices 2020-01 → 2024-12 ...")
all_prices = eia.get_retail_prices(start="2020-01", end="2024-12")
print(f"      {len(all_prices)} total records")

print("\n[2/3] Fetching generation 2020 → 2024 (year by year) ...")
all_generation = []
for year in range(2020, 2025):
    print(f"      Fetching generation {year}...")
    try:
        chunk = eia.get_generation_by_fuel(start=f"{year}-01", end=f"{year}-12")
        all_generation.extend(chunk)
        print(f"        {len(chunk)} records")
    except Exception as e:
        print(f"        ERROR: {e} — skipping")
    time.sleep(2)
print(f"      {len(all_generation)} total records")

# RTO demand is hourly — pull one quarter at a time to avoid connection drops
print("\n[3/3] Fetching RTO demand 2020 → 2024 (quarter by quarter) ...")
all_rto = []
quarters = [
    ("01-01", "03-31"), ("04-01", "06-30"),
    ("07-01", "09-30"), ("10-01", "12-31"),
]
for year in range(2020, 2025):
    for q_start, q_end in quarters:
        label = f"{year}-{q_start[:2]}"
        print(f"      Fetching RTO {year} Q{quarters.index((q_start,q_end))+1}...")
        try:
            chunk = eia.get_rto_demand(
                start=f"{year}-{q_start}", end=f"{year}-{q_end}"
            )
            all_rto.extend(chunk)
            print(f"        {len(chunk)} records")
        except Exception as e:
            print(f"        ERROR: {e} — skipping")
        time.sleep(2)
print(f"      {len(all_rto)} total RTO records")

# ── Upload to S3 and load into Snowflake month by month ──────────────────────
print("\nUploading to S3 and loading into Snowflake RAW...")
print("-" * 60)

for d in months:
    year  = d.strftime("%Y")
    month = d.strftime("%m")
    period = d.strftime("%Y-%m")

    # Filter each dataset to this month
    prices     = [r for r in all_prices     if r.get("period") == period]
    generation = [r for r in all_generation if r.get("period") == period]
    rto        = [r for r in all_rto        if str(r.get("period", "")).startswith(period)]

    print(f"{period}: prices={len(prices)} generation={len(generation)} rto={len(rto)}")

    # S3 upload
    if prices:
        upload_to_s3(prices, "retail-prices", year, month)
    if generation:
        upload_to_s3(generation, "generation", year, month)
    if rto:
        upload_to_s3(rto, "rto-demand", year, month)

    # RAW load
    load_to_raw(prices,     "RETAIL_PRICES", f"raw/retail-prices/year={year}/month={month}/data.json.gz")
    load_to_raw(generation, "GENERATION",    f"raw/generation/year={year}/month={month}/data.json.gz")
    load_to_raw(rto,        "RTO_DEMAND",    f"raw/rto-demand/year={year}/month={month}/data.json.gz")

# ── Staging transforms ────────────────────────────────────────────────────────
print("\nRunning staging transforms...")
for script in ["transform_retail_prices.sql", "transform_generation.sql", "transform_rto_demand.sql"]:
    rows = run_sql_file(f"sql/staging/{script}")
    print(f"  {script}: {rows} rows merged")

# ── Marts ─────────────────────────────────────────────────────────────────────
print("\nRebuilding marts...")
with open("sql/marts/create_marts.sql") as f:
    stmts = [s.strip() for s in f.read().split(";") if s.strip()]
for stmt in stmts:
    cur.execute(stmt)

# ── Final counts ──────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("FINAL ROW COUNTS:")
for table in [
    "RAW.RETAIL_PRICES", "RAW.GENERATION", "RAW.RTO_DEMAND",
    "STAGING.RETAIL_PRICES", "STAGING.GENERATION", "STAGING.RTO_DEMAND",
    "MARTS.FACT_RETAIL_PRICES", "MARTS.FACT_GENERATION", "MARTS.DIM_STATE",
]:
    cur.execute(f"SELECT COUNT(*) FROM EIA_ENERGY.{table}")
    print(f"  {table}: {cur.fetchone()[0]:,}")

cur.close()
con.close()
print("\nBackfill complete.")
