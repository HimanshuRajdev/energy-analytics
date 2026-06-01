# sample_upload.py
# Tests the full Extract → S3 path end-to-end.
# Run once to confirm boto3 credentials and bucket are wired up correctly.
# Delete this file before making the repo public.

import os
from dotenv import load_dotenv
from src.eia_client import EIAClient
from src.s3_client import S3Client

load_dotenv()

eia = EIAClient(api_key=os.environ["EIA_API_KEY"])
s3  = S3Client()

# --- Pull 3 months of retail prices ---
print("Fetching retail prices from EIA...")
prices = eia.get_retail_prices(start="2020-01", end="2020-03")
print(f"  Got {len(prices)} records")

# --- Upload to S3 ---
# One file per month keeps partitions small and makes replays cheaper.
# In the real DAG, Airflow passes the execution date so each run writes
# exactly one month's partition.
months = ["2020-01", "2020-02", "2020-03"]

for month in months:
    year, mo = month.split("-")
    month_records = [r for r in prices if r["period"] == month]
    key = s3.upload_json(
        data=month_records,
        dataset="retail-prices",
        year=year,
        month=mo,
    )
    print(f"  Uploaded {len(month_records)} records → s3://{s3.bucket}/{key}")

print("\nDone. Verify in the AWS console:")
print(f"  s3://{s3.bucket}/raw/retail-prices/")
