# US Energy Analytics Pipeline

**Live Dashboard**: [Tableau Public](https://public.tableau.com/app/profile/himanshu.rajdev/viz/USEnergyAnalyticsDashboard20202026/USEnergyAnalyticsDashboard20202026)

---

## What this is

This project pulls data from the US Energy Information Administration (EIA) API, stores it in AWS S3, transforms it through Snowflake, and visualizes it in Tableau. The end result is a live dashboard covering US electricity prices, generation mix, grid demand, and price anomalies from 2020 through early 2026.The goal was to build something that actually runs end to end, not just a notebook with some charts.

---

## Architecture

```
EIA API -> S3 (raw JSON) -> Snowflake (RAW -> STAGING -> MARTS) -> Tableau
```

Data flows daily through an Airflow pipeline. Raw JSON files land in S3, get loaded into Snowflake's RAW schema, transformed into clean STAGING tables via MERGE statements, and finally aggregated into MARTS that Tableau reads directly.

---

## Stack

- **Python 3.11** for API ingestion, S3 uploads, and Snowflake loading
- **Apache Airflow 2.9.1** running locally via Docker Compose for orchestration
- **AWS S3** for raw data storage
- **Snowflake** for all transformation and analytics
- **Tableau Desktop / Tableau Public** for visualization

---

## Data Sources

All data comes from the EIA Open Data API (free, no rate limits with an API key).

**Retail Electricity Prices** covers monthly prices across all US states and 6 customer sectors (residential, commercial, industrial, etc.) from 2020 to present. This dataset powers the price map and anomaly detection.

**Generation by Fuel Type** covers monthly electricity generation broken down by fuel type for every state. This is the largest dataset at around 1.3 million staging rows. It powers the energy mix chart and the renewable vs fossil comparison.

**RTO Demand** is hourly demand data from the major regional grid operators (MISO, PJM, ERCO, CISO, etc.). This is the heaviest dataset at around 15 million raw rows and powers the demand heatmap and regional comparison.

---

## Project Structure

```
eia-energy-pipeline/
├── dags/
│   ├── eia_ingest_dag.py       # pulls EIA API -> S3 daily at 02:00 UTC
│   └── eia_transform_dag.py    # S3 -> Snowflake RAW -> STAGING -> MARTS daily at 03:00 UTC
├── src/
│   ├── eia_client.py           # EIA API wrapper
│   └── s3_client.py            # boto3 wrapper
├── sql/
│   ├── raw/create_raw_tables.sql
│   ├── staging/
│   │   ├── transform_retail_prices.sql
│   │   ├── transform_generation.sql
│   │   └── transform_rto_demand.sql
│   └── marts/create_marts.sql
├── backfill.py                 # loads 2020-2024 historical data
├── backfill_2025.py            # loads 2025-2026 data
├── backfill_rto.py             # RTO demand backfill (parallel, quarter by quarter)
├── run_transforms.py           # reruns staging + marts transforms manually
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Snowflake Schema

The database is `EIA_ENERGY` with three schemas.

**RAW** holds raw JSON payloads exactly as they come from the EIA API. Each row is one API record stored as a JSON string in a PAYLOAD column.

**STAGING** flattens the JSON into typed columns using `PARSE_JSON()` and deduplicates via MERGE statements. This is where field name corrections and type casting happen.

**MARTS** is what Tableau reads. These are fully aggregated, analytics-ready tables:

- `FACT_RETAIL_PRICES` includes 12-month rolling z-scores and an is_anomaly flag for prices more than 2.5 standard deviations from the rolling mean
- `FACT_GENERATION` maps individual fuel type codes to 7 categories (Natural Gas, Wind, Solar & Geothermal, Nuclear, Coal, Hydro, Other) and excludes aggregate codes that would double-count generation
- `FACT_RTO_HEATMAP` aggregates hourly demand to average by hour of day and month for the heatmap chart
- `FACT_RTO_REGIONAL` aggregates to monthly average and peak demand per grid region
- `FACT_ANOMALY_LEADERBOARD` counts anomaly events per state
- `DIM_STATE` is a simple state lookup table

---

## Airflow DAGs

The ingest DAG runs at 02:00 UTC and pulls the current month from all three EIA endpoints, compressing and uploading to S3.

The transform DAG runs at 03:00 UTC and has four tasks in sequence: truncate RAW tables, load from S3, run staging MERGE transforms, rebuild MARTS. The truncate step is important because the MERGE statements will throw a duplicate row error if RAW contains multiple records for the same key.

---

## Tableau Dashboard

The dashboard has 8 charts:

1. **US Price Map** - choropleth showing average electricity price by state, filterable by year and sector. Click any state to filter the entire dashboard.
2. **Electricity Price Anomalies** - z-score line chart showing when prices spiked beyond normal ranges. Texas 2021 and Washington 2024 are the most visible anomalies.
3. **Sales per Customer** - LOD calculated field showing monthly electricity spend per customer nationally over time. (built but not included in the final dashboard)
4. **Energy Mix by State** - stacked area chart showing the share of generation by fuel category over time, filterable by state.
5. **Fossil vs Renewable Generation** - two-line chart showing the national share of fossil vs renewable generation from 2020 to 2026. Renewables crossed fossil share in 2025.
6. **Anomalies per State Map** - choropleth of how many price anomaly events each state had. Oregon and Vermont lead.
7. **Demand Heatmap** - 24x12 grid showing average grid demand by hour of day and month for the contiguous US. Summer afternoons are the peak.
8. **Regional Demand Comparison** - horizontal bar chart comparing average demand across the 7 major grid operators. PJM is by far the largest.

---

## Setup

**Prerequisites**: Python 3.11, Docker, an EIA API key, an AWS account with S3, a Snowflake account.

1. Clone the repo
2. Copy `.env.example` to `.env` and fill in your credentials
3. Start Airflow: `docker-compose up -d`
4. Run the Snowflake setup SQL in `sql/raw/` and `sql/staging/`
5. Run `python backfill.py` to load 2020-2024 historical data (takes 10-15 minutes)
6. Run `python backfill_2025.py` to extend to 2026
7. Run `python backfill_rto.py` for RTO demand history (runs in parallel, takes 15-20 minutes)
8. The daily DAGs will keep everything current from there

---

## Environment Variables

```
EIA_API_KEY=
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_REGION=us-east-1
S3_BUCKET=
SF_ACCOUNT=
SF_USER=
SF_PASSWORD=
SF_WAREHOUSE=
SNOWFLAKE_DATABASE=EIA_ENERGY
```

---

## Notes

The EIA API requires parameters to be passed as a list of tuples rather than a dict when using repeated keys like `data[]`. All endpoints need a `/data` suffix. The `_get()` method in `eia_client.py` handles this.

Generation data has many aggregate fuel type codes (ALL, FOS, REN, etc.) that sum up the individual codes. These are excluded from MARTS to avoid double counting. The blacklist is in `create_marts.sql`.

RTO demand data is hourly and large. The backfill script fetches 4 quarters in parallel using `ThreadPoolExecutor` to speed things up. Even so, expect around 15 million rows total after the full backfill.
