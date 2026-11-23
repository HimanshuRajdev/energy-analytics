# src/pipeline.py
"""
Shared pipeline steps. The Airflow DAG and the command line scripts both call these,
so there is one copy of the logic.

Why a rolling window?
  EIA publishes monthly retail and generation data about two months late, and it
  revises recent months. Asking only for the current month returns nothing. Every
  run re-pulls the last N months instead, overwrites those S3 files, and the MERGE
  steps upsert the changes. Re-running is always safe.
"""
import logging
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parent.parent / "sql" / "bigquery"

# S3 folder name -> BigQuery raw table
DATASETS = {
    "retail-prices": "retail_prices",
    "generation": "generation",
    "rto-demand": "rto_demand",
}

# dataset -> staging script
STAGING_SCRIPTS = {
    "retail-prices": "transform_retail_prices.sql",
    "generation": "transform_generation.sql",
    "rto-demand": "transform_rto_demand.sql",
}


def month_window(anchor: date, lookback_months: int) -> list[date]:
    """First day of each month from (anchor - lookback) up to the anchor's month."""
    idx = anchor.year * 12 + anchor.month - 1
    return [date((i // 12), (i % 12) + 1, 1) for i in range(idx - lookback_months, idx + 1)]


def split_by_month(records: list) -> dict[str, list]:
    """Group records by YYYY-MM using the period field (works for monthly and hourly)."""
    out = defaultdict(list)
    for r in records:
        period = str(r.get("period", ""))
        if len(period) >= 7:
            out[period[:7]].append(r)
    return out


def ingest_dataset(dataset: str, eia, s3, months: list[date], end_day: date) -> dict[str, int]:
    """Pull one dataset for the window and write one S3 file per month.

    Months that come back empty are skipped so a good file is never replaced by an empty one.
    """
    first_ym = months[0].strftime("%Y-%m")
    last_ym = months[-1].strftime("%Y-%m")

    if dataset == "retail-prices":
        records = eia.get_retail_prices(start=first_ym, end=last_ym)
    elif dataset == "generation":
        records = eia.get_generation_by_fuel(start=first_ym, end=last_ym)
    elif dataset == "rto-demand":
        # Hourly data is big, so pull and save one month at a time. A dropped connection
        # then costs one month, not the whole window.
        counts = {}
        for m in months:
            nxt = date(m.year + (m.month == 12), m.month % 12 + 1, 1)
            last_day = min(nxt - timedelta(days=1), end_day)
            if last_day < m:
                continue
            recs = eia.get_rto_demand(start=f"{m:%Y-%m-%d}T00", end=f"{last_day:%Y-%m-%d}T23")
            logger.info("[rto-demand] %s: %d records", f"{m:%Y-%m}", len(recs))
            if recs:
                s3.upload_json(data=recs, dataset=dataset, year=f"{m:%Y}", month=f"{m:%m}")
                counts[f"{m:%Y-%m}"] = len(recs)
        return counts
    else:
        raise ValueError(dataset)

    logger.info("[%s] %d records for %s to %s", dataset, len(records), first_ym, last_ym)
    counts = {}
    for ym, recs in sorted(split_by_month(records).items()):
        year, month = ym.split("-")
        s3.upload_json(data=recs, dataset=dataset, year=year, month=month)
        counts[ym] = len(recs)
    return counts


def setup_tables(bq) -> None:
    bq.run_sql_file(SQL_DIR / "00_create_tables.sql")


def load_raw(s3, bq, months: list[date], datasets=None) -> dict[str, int]:
    """Replace the RAW tables with the S3 files for the given months."""
    datasets = list(datasets or DATASETS)
    bq.truncate_raw([DATASETS[d] for d in datasets])
    totals = {}
    for dataset in datasets:
        table = DATASETS[dataset]
        n = 0
        for m in months:
            year, month = f"{m:%Y}", f"{m:%m}"
            records = s3.read_json(dataset, year, month)
            if not records:
                continue
            key = f"raw/{dataset}/year={year}/month={month}/data.json.gz"
            n += bq.load_raw(table, records, key)
        totals[dataset] = n
        logger.info("[load_raw] %s: %d rows", dataset, n)
    return totals


def run_staging(bq, datasets=None) -> None:
    """Run the staging MERGEs. Fails right away if RAW has rows but STAGING is still empty."""
    for dataset in (datasets or DATASETS):
        table = DATASETS[dataset]
        bq.run_sql_file(SQL_DIR / "staging" / STAGING_SCRIPTS[dataset])
        raw_n = bq.query(f"SELECT COUNT(*) n FROM `{{{{project}}}}.eia_raw.{table}`")[0].n
        stg_n = bq.query(f"SELECT COUNT(*) n FROM `{{{{project}}}}.eia_staging.{table}`")[0].n
        logger.info("[staging] %s: raw %d rows, staging %d rows", table, raw_n, stg_n)
        if raw_n > 0 and stg_n == 0:
            raise RuntimeError(
                f"{table}: {raw_n} raw rows loaded but staging is empty. "
                f"Run python -m scripts.diagnose_rto to see why."
            )


def run_marts(bq) -> None:
    bq.run_sql_file(SQL_DIR / "marts" / "create_marts.sql")


def validate(bq) -> dict:
    """Fail loudly if a mart is empty. Log how fresh each dataset is."""
    checks = {
        "fact_retail_prices": "SELECT COUNT(*) n, MAX(period) latest FROM `{{project}}.eia_marts.fact_retail_prices`",
        "fact_generation": "SELECT COUNT(*) n, MAX(period) latest FROM `{{project}}.eia_marts.fact_generation`",
        "fact_rto_regional": "SELECT COUNT(*) n, CAST(MAX(month) AS STRING) latest FROM `{{project}}.eia_marts.fact_rto_regional`",
        "fact_rto_heatmap": "SELECT COUNT(*) n, CAST(MAX(month) AS STRING) latest FROM `{{project}}.eia_marts.fact_rto_heatmap`",
    }
    result = {}
    for name, sql in checks.items():
        row = bq.query(sql)[0]
        result[name] = {"rows": row.n, "latest": row.latest}
        logger.info("[validate] %s: %s rows, latest %s", name, row.n, row.latest)
        if row.n == 0:
            raise RuntimeError(f"{name} is empty")
    return result


def publish_sheet(bq) -> dict:
    """Copy dashboard tables into the Google Sheet that Tableau Public reads."""
    from src import sheets_export
    return sheets_export.publish(bq)
