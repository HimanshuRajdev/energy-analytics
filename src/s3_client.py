# src/s3_client.py
import gzip
import json
import logging
import os

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)


class S3Client:
    """
    Thin wrapper around boto3 for writing raw EIA API responses to S3.
    Serializes data as gzipped JSON to keep storage costs low.
    Credentials are read from environment variables (set in .env):
        AWS_ACCESS_KEY_ID
        AWS_SECRET_ACCESS_KEY
        AWS_REGION
        S3_BUCKET
    """

    def __init__(self):
        self.bucket = os.environ["S3_BUCKET"]
        self.client = boto3.client(
            "s3",
            region_name=os.environ["AWS_REGION"],
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )

    def upload_json(self, data: list, dataset: str, year: str, month: str) -> str:
        """
        Serialize `data` as gzipped JSON and upload to:
            raw/{dataset}/year={year}/month={month}/data.json.gz

        Args:
            data:    List of record dicts from the EIA client.
            dataset: Folder name, e.g. 'retail-prices', 'generation', 'rto-demand'.
            year:    4-digit string, e.g. '2020'.
            month:   Zero-padded 2-digit string, e.g. '01'.

        Returns:
            The S3 key the file was written to.
        """
        key = f"raw/{dataset}/year={year}/month={month}/data.json.gz"

        body = gzip.compress(
            json.dumps(data, default=str).encode("utf-8")
        )

        try:
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=body,
                ContentType="application/json",
                ContentEncoding="gzip",
            )
            logger.info("Uploaded %d records to s3://%s/%s", len(data), self.bucket, key)
        except ClientError as e:
            logger.error("Failed to upload to s3://%s/%s: %s", self.bucket, key, e)
            raise

        return key
