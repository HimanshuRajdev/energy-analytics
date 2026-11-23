"""
Rewrite the Google Sheet from what is already in BigQuery. No EIA or S3 calls.

    python -m scripts.publish_sheet
"""
import logging

from dotenv import load_dotenv

from src import pipeline
from src.bq_client import BQ

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
load_dotenv()
print(pipeline.publish_sheet(BQ()))
