"""
Takes about a minute. Looks at the demand rows currently in eia_raw.rto_demand,
shows what they look like, tries the staging step on them, and reports what came out.

    python -m scripts.diagnose_rto
"""
import logging

from dotenv import load_dotenv

from src import pipeline
from src.bq_client import BQ

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
load_dotenv()
bq = BQ()

print("raw rows:", bq.query("SELECT COUNT(*) n FROM `{{project}}.eia_raw.rto_demand`")[0].n)
print("\nsample payloads:")
for r in bq.query("SELECT payload FROM `{{project}}.eia_raw.rto_demand` LIMIT 3"):
    print("  ", r.payload)

print("\nrows per type:")
for r in bq.query("""SELECT JSON_VALUE(payload,'$.type') AS type, COUNT(*) n
                     FROM `{{project}}.eia_raw.rto_demand` GROUP BY 1 ORDER BY 2 DESC"""):
    print("  ", r.type, r.n)

r = bq.query("""SELECT
    COUNTIF(SAFE.PARSE_TIMESTAMP('%Y-%m-%dT%H', JSON_VALUE(payload,'$.period')) IS NOT NULL) AS parsed_strict,
    COUNTIF(JSON_VALUE(payload,'$.respondent') IS NOT NULL) AS has_respondent,
    COUNTIF(SAFE_CAST(JSON_VALUE(payload,'$.value') AS FLOAT64) IS NOT NULL) AS has_value
  FROM `{{project}}.eia_raw.rto_demand`""")[0]
print("\nparsed period:", r.parsed_strict, "| has respondent:", r.has_respondent, "| numeric value:", r.has_value)

print("\nclearing sandbox expiry settings (00_create_tables.sql)...")
pipeline.setup_tables(bq)
try:
    for r in bq.query(f"""SELECT schema_name, option_name, option_value
        FROM `{{{{project}}}}.region-{bq.location.lower()}.INFORMATION_SCHEMA.SCHEMATA_OPTIONS`
        WHERE schema_name LIKE 'eia_%' AND option_name LIKE '%expiration%'"""):
        print("   still set:", r.schema_name, r.option_name, r.option_value)
except Exception as e:
    print("   (could not list dataset options:", str(e)[:120], ")")

print("\nrunning the staging step on this data...")
bq.run_sql_file(pipeline.SQL_DIR / "staging" / "transform_rto_demand.sql")
print("staging rows:", bq.query("SELECT COUNT(*) n FROM `{{project}}.eia_staging.rto_demand`")[0].n)
for r in bq.query("""SELECT region_id, COUNT(*) n, ROUND(AVG(demand_mwh)) avg_mwh
                     FROM `{{project}}.eia_staging.rto_demand` GROUP BY 1 ORDER BY 3 DESC LIMIT 5"""):
    print("  ", r.region_id, r.n, r.avg_mwh)
