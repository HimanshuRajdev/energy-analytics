# src/bq_client.py
"""Small BigQuery helper. Replaces the Snowflake connection code."""
import gzip
import io
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery

logger = logging.getLogger(__name__)

RAW_TABLES = ("retail_prices", "generation", "rto_demand")

# Safety net so one bad query can never burn the 1 TB monthly free tier.
DEFAULT_MAX_BYTES = 50 * 1024**3


class BQ:
    def __init__(self, project: str | None = None, location: str | None = None):
        self.project = project or os.environ["GCP_PROJECT"]
        self.location = location or os.environ.get("BQ_LOCATION", "US")
        self.client = bigquery.Client(project=self.project, location=self.location)
        self.max_bytes = int(os.environ.get("BQ_MAX_BYTES_BILLED", DEFAULT_MAX_BYTES))

    def render(self, sql: str) -> str:
        return sql.replace("{{project}}", self.project)

    def run_sql_file(self, path) -> None:
        """Run a .sql file as one BigQuery script (multiple statements are fine)."""
        sql = self.render(Path(path).read_text())
        cfg = bigquery.QueryJobConfig(maximum_bytes_billed=self.max_bytes)
        job = self.client.query(sql, job_config=cfg)
        job.result()
        logger.info("Ran %s (%s bytes billed)", Path(path).name, job.total_bytes_billed)

    def query(self, sql: str) -> list:
        cfg = bigquery.QueryJobConfig(maximum_bytes_billed=self.max_bytes)
        return list(self.client.query(self.render(sql), job_config=cfg).result())

    def truncate_raw(self, tables=None) -> None:
        for table in (tables or RAW_TABLES):
            self.client.query(f"TRUNCATE TABLE `{self.project}.eia_raw.{table}`").result()
            logger.info("Truncated eia_raw.%s", table)

    def load_raw(self, table: str, records: list, source_file: str) -> int:
        """Append API records to a RAW table as JSON strings. Load jobs are free.

        Rows are streamed into a gzip file in memory, which uploads about 10x faster
        than plain JSON. If BigQuery rejects the gzip upload, fall back to plain JSON.
        """
        if not records:
            return 0
        now = datetime.now(timezone.utc).isoformat()
        schema = [
            bigquery.SchemaField("payload", "STRING"),
            bigquery.SchemaField("source_file", "STRING"),
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        ]
        dest = f"{self.project}.eia_raw.{table}"
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            for r in records:
                line = json.dumps(
                    {"payload": json.dumps(r, default=str), "source_file": source_file, "ingested_at": now}
                )
                gz.write(line.encode("utf-8") + b"\n")
        buf.seek(0)
        cfg = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        try:
            self.client.load_table_from_file(buf, dest, job_config=cfg).result()
        except BadRequest as e:
            logger.warning("gzip upload rejected (%s), retrying as plain JSON", e)
            rows = [
                {"payload": json.dumps(r, default=str), "source_file": source_file, "ingested_at": now}
                for r in records
            ]
            cfg = bigquery.LoadJobConfig(
                schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_APPEND
            )
            self.client.load_table_from_json(rows, dest, job_config=cfg).result()
        return len(records)
