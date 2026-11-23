"""
Run the whole pipeline once without Airflow. Handy for testing and for catch-up.

    python -m scripts.run_pipeline --lookback 9          # re-pull the last 9 months
    python -m scripts.run_pipeline --skip-ingest         # only reload BigQuery from S3
"""
import argparse
import logging
import os
from datetime import date

from dotenv import load_dotenv

from src import pipeline
from src.bq_client import BQ
from src.eia_client import EIAClient
from src.s3_client import S3Client

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--lookback", type=int, default=3)
    ap.add_argument("--skip-ingest", action="store_true")
    ap.add_argument("--datasets", nargs="+", choices=list(pipeline.DATASETS), default=list(pipeline.DATASETS))
    ap.add_argument("--publish", action="store_true", help="also write the Google Sheet")
    args = ap.parse_args()

    load_dotenv()
    today = date.today()
    months = pipeline.month_window(today, args.lookback)
    s3, bq = S3Client(), BQ()

    if not args.skip_ingest:
        eia = EIAClient(api_key=os.environ["EIA_API_KEY"])
        for dataset in args.datasets:
            print(dataset, pipeline.ingest_dataset(dataset, eia, s3, months, today))

    pipeline.setup_tables(bq)
    print(pipeline.load_raw(s3, bq, months, args.datasets))
    pipeline.run_staging(bq, args.datasets)
    pipeline.run_marts(bq)
    print(pipeline.validate(bq))
    if args.publish:
        print(pipeline.publish_sheet(bq))


if __name__ == "__main__":
    main()
