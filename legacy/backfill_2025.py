"""
backfill_2025.py
----------------
Pulls 2025-01 through 2026-03 from EIA, uploads to S3,
appends to Snowflake RAW, then re-runs staging + marts.

Run from project root (with .env present):
    python backfill_2025.py
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

# ── Clients ───────────────────────────────────────────────────────────────────
eia    = EIAClient(api_key=os.environ["EIA_API_KEY"])
s3     = boto3.client(
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

# ── Date range ────────────────────────────────────────────────────────────────
START = date(2025, 1, 1)
END   = date(2026, 3, 1)   # 2026-03 is latest typically available; adjust if needed

months = []
d = START
while d <= END:
    months.append(d)
    d += relativedelta(months=1)

print(f"Backfilling {len(months)} months ({START.strftime('%Y-%m')} → {END.strftime('%Y-%m')})")
print("=" * 60)


# ── Helpers ───────────────────────────────────────────────────────────────────
def upload_to_s3(records, dataset, year, month):
    key  = f"raw/{dataset}/year={year}/month={month}/data.json.gz"
    body = gzip.compress(json.dumps(records, default=str).encode())
    s3.put_object(Bucket=bucket, Key=key, Body=body,
                  ContentType="application/json", ContentEncoding="gzip")
    return key


def load_to_raw(records, table, key):
    if not records:
        return 0
    df = pd.DataFrame({
        "PAYLOAD":     [json.dumps(r) for r in records],
        "SOURCE_FILE": key,
    })
    _, _, nrows, _ = write_pandas(
        con, df, table, schema="RAW",
        database="EIA_ENERGY", auto_create_table=False,
    )
    return nrows


# ── Fetch data ────────────────────────────────────────────────────────────────
print("\n[1/3] Fetching retail prices 2025-01 → 2026-03 ...")
all_prices = eia.get_retail_prices(start="2025-01", end="2026-03")
print(f"      {len(all_prices)} records")

print("\n[2/3] Fetching generation 2025-01 → 2026-03 ...")
all_generation = []
for year in range(2025, 2027):
    y_start = f"{year}-01"
    y_end   = f"{year}-03" if year == 2026 else f"{year}-12"
    print(f"      Fetching {y_start} → {y_end} ...")
    try:
        chunk = eia.get_generation_by_fuel(start=y_start, end=y_end)
        all_generation.extend(chunk)
        print(f"        {len(chunk)} records")
    except Exception as e:
        print(f"        ERROR: {e} — skipping")
    time.sleep(2)
print(f"      {len(all_generation)} total")

print("\n[3/3] Fetching RTO demand 2025-01 → 2026-03 (quarter by quarter) ...")
all_rto = []
quarters = [
    ("01-01", "03-31"), ("04-01", "06-30"),
    ("07-01", "09-30"), ("10-01", "12-31"),
]
for year in range(2025, 2027):
    for i, (q_start, q_end) in enumerate(quarters):
        # Stop after Q1 2026
        if year == 2026 and i > 0:
            break
        q_end_date = f"{year}-03-31" if year == 2026 else f"{year}-{q_end}"
        print(f"      Fetching RTO {year} Q{i+1} ...")
        try:
            chunk = eia.get_rto_demand(
                start=f"{year}-{q_start}", end=q_end_date
            )
            all_rto.extend(chunk)
            print(f"        {len(chunk)} records")
        except Exception as e:
            print(f"        ERROR: {e} — skipping")
        time.sleep(2)
print(f"      {len(all_rto)} total RTO records")


# ── Upload S3 + load RAW ──────────────────────────────────────────────────────
print("\nUploading to S3 and loading into Snowflake RAW...")
print("-" * 60)

for d in months:
    year   = d.strftime("%Y")
    month  = d.strftime("%m")
    period = d.strftime("%Y-%m")

    prices     = [r for r in all_prices     if r.get("period") == period]
    generation = [r for r in all_generation if r.get("period") == period]
    rto        = [r for r in all_rto        if str(r.get("period", "")).startswith(period)]

    print(f"{period}: prices={len(prices)} generation={len(generation)} rto={len(rto)}")

    if prices:
        key = upload_to_s3(prices, "retail-prices", year, month)
        load_to_raw(prices, "RETAIL_PRICES", key)
    if generation:
        key = upload_to_s3(generation, "generation", year, month)
        load_to_raw(generation, "GENERATION", key)
    if rto:
        key = upload_to_s3(rto, "rto-demand", year, month)
        load_to_raw(rto, "RTO_DEMAND", key)


# ── Staging transforms (MERGE — idempotent, safe to run on top of existing) ──
print("\nRunning staging transforms...")
for script in ["transform_retail_prices.sql", "transform_generation.sql", "transform_rto_demand.sql"]:
    path = os.path.join("sql", "staging", script)
    with open(path) as f:
        sql = f.read()
    cur.execute(sql)
    print(f"  {script}: {cur.rowcount} rows merged")


# ── Marts ─────────────────────────────────────────────────────────────────────
print("\nRebuilding marts...")
with open("sql/marts/create_marts.sql") as f:
    stmts = [s.strip() for s in f.read().split(";") if s.strip()]
for stmt in stmts:
    non_comment = "\n".join(
        line for line in stmt.splitlines() if not line.strip().startswith("--")
    ).strip()
    if not non_comment:
        continue
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
print("\nBackfill 2025 complete.")
