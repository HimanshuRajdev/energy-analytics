"""
Checks every stage of the pipeline and prints PASS / WARN / FAIL for each.

    python -m scripts.check_all
"""
import os
from datetime import date

from dotenv import load_dotenv

from src import pipeline, sheets_export
from src.bq_client import BQ
from src.s3_client import S3Client

load_dotenv()
results = []


def check(name, ok, detail="", warn=False):
    tag = "PASS" if ok else ("WARN" if warn else "FAIL")
    results.append(tag)
    print(f"[{tag}] {name}  {detail}")


def months_between(first, last):
    out, y, m = [], first.year, first.month
    while (y, m) <= (last.year, last.month):
        out.append(date(y, m, 1))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


bq = BQ()

# 1. S3 has a file for every month of every dataset
print("\n== S3 ==")
s3 = S3Client()
today = date.today()
for dataset in pipeline.DATASETS:
    have = [m for m in months_between(date(2020, 1, 1), today) if s3.read_json(dataset, f"{m:%Y}", f"{m:%m}")]
    check(f"S3 {dataset}", len(have) >= 70, f"{len(have)} months, latest {have[-1]:%Y-%m}" if have else "no files")

# 2. BigQuery tables have rows and are fresh
print("\n== BigQuery ==")
tables = {
    "eia_staging.retail_prices": "MAX(period)",
    "eia_staging.generation": "MAX(period)",
    "eia_staging.rto_demand": "CAST(MAX(period) AS STRING)",
    "eia_marts.fact_retail_prices": "MAX(period)",
    "eia_marts.fact_generation": "MAX(period)",
    "eia_marts.fact_rto_heatmap": "CAST(MAX(month) AS STRING)",
    "eia_marts.fact_rto_regional": "CAST(MAX(month) AS STRING)",
    "eia_marts.fact_anomaly_leaderboard": "'-'",
    "eia_marts.dim_state": "'-'",
}
bq_rows = {}
for t, latest in tables.items():
    r = bq.query(f"SELECT COUNT(*) n, {latest} latest FROM `{{{{project}}}}.{t}`")[0]
    bq_rows[t.split(".")[1]] = r.n
    check(f"BQ {t}", r.n > 0, f"{r.n:,} rows, latest {r.latest}")

# demand should only hold the D series, around 60+ regions and years of hours
r = bq.query("SELECT COUNT(DISTINCT region_id) regions, MIN(period) first_hr FROM `{{project}}.eia_staging.rto_demand`")[0]
check("BQ demand coverage", r.regions >= 50 and r.first_hr.year == 2020, f"{r.regions} regions, starts {r.first_hr:%Y-%m-%d}")

# nothing should expire
left = bq.query(f"""SELECT COUNT(*) n FROM `{{{{project}}}}.region-{bq.location.lower()}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
                    WHERE schema_name LIKE 'eia_%' AND option_name LIKE '%expiration%'""")[0].n
check("BQ no expiry settings", left == 0, f"{left} expiry options still set")

# 3. Google Sheet matches BigQuery
print("\n== Google Sheet ==")
try:
    session = sheets_export._service()
    sid = os.environ["SHEET_ID"]
    for tab in sheets_export.EXPORTS:
        n = sheets_export.row_count(session, sid, tab)
        check(f"Sheet tab {tab}", n > 0, f"{n:,} rows")
except Exception as e:
    check("Google Sheet", False, f"{type(e).__name__}: {str(e)[:150]}")

print(f"\n{results.count('PASS')} passed, {results.count('WARN')} warnings, {results.count('FAIL')} failed")
raise SystemExit(1 if "FAIL" in results else 0)
