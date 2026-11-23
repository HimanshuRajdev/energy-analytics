"""
One-time rebuild of BigQuery from everything already sitting in S3 (2020 to now).
Use this right after creating the BigQuery project. No EIA API calls are made.

    python -m scripts.rebuild_from_s3 --start 2020-01

Loads one year at a time so memory stays small, merging into STAGING after each year.
"""
import argparse
import logging
from datetime import date

from dotenv import load_dotenv

from src import pipeline
from src.bq_client import BQ
from src.s3_client import S3Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2020-01", help="first month, YYYY-MM")
    ap.add_argument("--datasets", nargs="+", choices=list(pipeline.DATASETS), default=list(pipeline.DATASETS))
    ap.add_argument("--skip-validate", action="store_true")
    args = ap.parse_args()

    load_dotenv()
    y, m = map(int, args.start.split("-"))
    today = date.today()
    s3, bq = S3Client(), BQ()
    pipeline.setup_tables(bq)

    for year in range(y, today.year + 1):
        months = [date(year, mo, 1) for mo in range(1, 13)
                  if date(year, mo, 1) >= date(y, m, 1) and date(year, mo, 1) <= today]
        if not months:
            continue
        print(f"--- {year}")
        print(pipeline.load_raw(s3, bq, months, args.datasets))
        pipeline.run_staging(bq, args.datasets)

    pipeline.run_marts(bq)
    print(pipeline.validate(bq))


if __name__ == "__main__":
    main()
