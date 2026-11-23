"""
export_for_tableau.py
---------------------
Exports MARTS tables from Snowflake to CSV files for Tableau Public.
Run after backfill.py completes.
"""
import os
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

con = snowflake.connector.connect(
    account=os.environ["SF_ACCOUNT"],
    user=os.environ["SF_USER"],
    password=os.environ["SF_PASSWORD"],
    warehouse=os.environ["SF_WAREHOUSE"],
    database=os.environ.get("SNOWFLAKE_DATABASE", "EIA_ENERGY"),
)
cur = con.cursor()

exports = {
    "fact_retail_prices.csv": "SELECT * FROM EIA_ENERGY.MARTS.FACT_RETAIL_PRICES",
    "fact_generation.csv":    "SELECT * FROM EIA_ENERGY.MARTS.FACT_GENERATION",
    "dim_state.csv":          "SELECT * FROM EIA_ENERGY.MARTS.DIM_STATE",
    "staging_rto_demand.csv": """
        SELECT
            DATE_TRUNC('hour', period)                    AS hour,
            DATE_PART('hour', period)                     AS hour_of_day,
            DATE_PART('month', period)                    AS month,
            DATE_PART('dayofweek', period)                AS day_of_week,
            region_id,
            region_name,
            demand_mwh
        FROM EIA_ENERGY.STAGING.RTO_DEMAND
        WHERE demand_mwh IS NOT NULL
    """,
}

os.makedirs("tableau_data", exist_ok=True)

for filename, query in exports.items():
    print(f"Exporting {filename}...")
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=[d[0] for d in cur.description])
    path = f"tableau_data/{filename}"
    df.to_csv(path, index=False)
    print(f"  {len(df):,} rows → {path}")

cur.close()
con.close()
print("\nAll exports complete. Load the CSVs from the tableau_data/ folder into Tableau Public.")
