"""
backfill_rto.py
---------------
Fetches RTO demand data (2020-01 → 2026-03) and loads into S3 + Snowflake.
Uses parallel fetching (4 workers) to speed up API calls.

Run from project root:
    python backfill_rto.py
"""
import gzip, json, os, time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from dateutil.relativedelta import relativedelta

import boto3
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

from src.eia_client import EIAClient

load_dotenv()

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

# ── Build list of quarters to fetch ──────────────────────────────────────────
quarters = []
for year in range(2020, 2027):
    for q, (q_start, q_end) in enumerate([
        ("01-01", "03-31"), ("04-01", "06-30"),
        ("07-01", "09-30"), ("10-01", "12-31"),
    ], 1):
        start = f"{year}-{q_start}"
        end   = f"{year}-{q_end}"
        # Stop after 2026-Q1
        if year == 2026 and q > 1:
            break
        # Cap end date at 2026-03-31
        if year == 2026:
            end = "2026-03-31"
        quarters.append((year, q, start, end))

print(f"Fetching {len(quarters)} quarters with 4 parallel workers...")
print("=" * 60)


# ── Parallel fetch ────────────────────────────────────────────────────────────
def fetch_quarter(args):
    year, q, start, end = args
    label = f"{year} Q{q} ({start} → {end})"
    for attempt in range(3):
        try:
            chunk = eia.get_rto_demand(start=start, end=end)
            print(f"  ✓ {label}: {len(chunk):,} records")
            return chunk
        except Exception as e:
            print(f"  ✗ {label} attempt {attempt+1}: {e}")
            time.sleep(3 * (attempt + 1))
    print(f"  ✗ {label}: all retries failed — skipping")
    return []


all_rto = []
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {executor.submit(fetch_quarter, q): q for q in quarters}
    for future in as_completed(futures):
        all_rto.extend(future.result())

print(f"\nTotal RTO records fetched: {len(all_rto):,}")


# ── Upload to S3 + load RAW month by month ────────────────────────────────────
print("\nUploading to S3 and loading into Snowflake RAW...")
print("-" * 60)

d = date(2020, 1, 1)
while d <= date(2026, 3, 1):
    year   = d.strftime("%Y")
    month  = d.strftime("%m")
    period = d.strftime("%Y-%m")

    rto = [r for r in all_rto if str(r.get("period", "")).startswith(period)]
    if rto:
        key  = f"raw/rto-demand/year={year}/month={month}/data.json.gz"
        body = gzip.compress(json.dumps(rto, default=str).encode())
        s3.put_object(Bucket=bucket, Key=key, Body=body,
                      ContentType="application/json", ContentEncoding="gzip")

        df = pd.DataFrame({
            "PAYLOAD":     [json.dumps(r) for r in rto],
            "SOURCE_FILE": key,
        })
        _, _, nrows, _ = write_pandas(
            con, df, "RTO_DEMAND", schema="RAW",
            database="EIA_ENERGY", auto_create_table=False,
        )
        print(f"{period}: {len(rto):,} records → S3 + {nrows:,} rows → RAW")
    else:
        print(f"{period}: no data")

    d += relativedelta(months=1)


# ── Staging transform ─────────────────────────────────────────────────────────
print("\nRunning RTO staging transform...")
with open("sql/staging/transform_rto_demand.sql") as f:
    cur.execute(f.read())
print(f"  {cur.rowcount:,} rows merged into STAGING.RTO_DEMAND")

cur.execute("SELECT COUNT(*) FROM EIA_ENERGY.STAGING.RTO_DEMAND")
print(f"  STAGING.RTO_DEMAND total: {cur.fetchone()[0]:,}")

cur.close()
con.close()
print("\nRTO backfill complete.")
