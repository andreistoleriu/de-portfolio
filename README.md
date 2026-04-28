# Data Engineering Portfolio

A production-grade data engineering portfolio built with modern open-source tools, demonstrating end-to-end pipeline development from ingestion to visualization across three different domains.

## Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion | dlt | Extract and load data into DuckDB |
| Storage | DuckDB | Local analytical database |
| Transformation | dbt | Staging and mart models with tests |
| Orchestration | Dagster | Pipeline scheduling and monitoring |
| Visualization | Looker Studio | Business dashboards |

## Pipelines

### 1. ⚡ Energy Consumption Analytics

Simulates smart meter data across 5 German cities — residential, commercial and industrial sites — over 30 days of hourly readings (3,600 rows). The pipeline models realistic consumption patterns including peak hours, weekend drops and site-type profiles, mimicking the kind of data handled in utility companies.

The dlt ingestion layer loads raw hourly readings into DuckDB. dbt builds a staging model that cleans and casts the raw data, and a daily mart that aggregates consumption and cost metrics per site. Eight data quality tests run automatically on every execution. Dagster orchestrates the full pipeline as a DAG scheduled at 06:00 UTC daily.

**Dashboard:** [View in Looker Studio](https://datastudio.google.com/s/mDf51wLWfjE)

---

### 2. 🛒 E-commerce Sales Analytics

Simulates 90 days of online retail transactions across 6 European countries, with 500 customers segmented into VIP, regular and occasional tiers, and 45 products across 5 categories. Order volume varies realistically by day of week and end of month peaks.

dbt builds 5 models — 3 staging views and 2 mart tables — covering daily revenue breakdowns and customer segment performance. Key metrics include net revenue, profit margin, return rate and revenue per customer. Eight data quality tests validate uniqueness, nullability and accepted values on every run.

**Dashboard:** [View in Looker Studio](https://datastudio.google.com/reporting/a82da509-d30f-44d8-99e7-87bfb631ac15)

---

### 3. 🌦️ Weather Data Analytics — Real API

Ingests real hourly weather data for 5 Romanian cities (Bucharest, Cluj-Napoca, Iași, Timișoara, Constanța) from the Open-Meteo API — no API key required. Covers temperature, humidity, wind speed, precipitation and cloud cover over the last 30 days (3,720 rows of real data).

This pipeline demonstrates working with a live REST API, handling columnar-to-row transformation, and building analytical models on top of real-world data. Four data quality tests validate city values and metric nullability.

**Dashboard:** [View in Looker Studio](https://datastudio.google.com/reporting/011208a6-86af-4f7c-870f-cbe17c482685)

---

## Project Structure

```
de-portfolio/
├── pipelines/
│   ├── energy/
│   │   └── energy_pipeline.py       # dlt ingestion — simulated smart meter data
│   ├── ecommerce/
│   │   └── ecommerce_pipeline.py    # dlt ingestion — simulated e-commerce orders
│   └── weather/
│       └── weather_pipeline.py      # dlt ingestion — real Open-Meteo API data
├── dbt_energy/
│   ├── models/
│   │   ├── staging/stg_meter_readings.sql
│   │   └── marts/mart_energy_daily.sql
│   └── dbt_project.yml
├── dbt_ecommerce/
│   ├── models/
│   │   ├── staging/                 # stg_orders, stg_customers, stg_products
│   │   └── marts/                   # mart_revenue_daily, mart_customer_segments
│   └── dbt_project.yml
├── dbt_weather/
│   ├── models/
│   │   ├── staging/stg_weather.sql
│   │   └── marts/mart_weather_daily.sql
│   └── dbt_project.yml
├── dagster_energy/
│   ├── assets.py                    # energy assets
│   ├── assets_ecommerce.py          # ecommerce assets
│   ├── assets_weather.py            # weather assets
│   └── definitions.py               # all pipelines + schedules
└── requirements.txt
```

## How to Run

```bash
# 1. Clone the repository
git clone https://github.com/andreistoleriu/de-portfolio.git
cd de-portfolio

# 2. Create virtual environment
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Launch Dagster — runs all 3 pipelines
cd dagster_energy
dagster dev -f definitions.py
# Open http://localhost:3000 → Materialize all
```

## Schedules

| Pipeline | Schedule |
|----------|----------|
| Energy | Daily at 06:00 UTC |
| E-commerce | Daily at 07:00 UTC |
| Weather | Daily at 08:00 UTC |

## Author

**Andrei Stoleriu** — Data Engineer at E.ON Software Development

Building this portfolio to demonstrate hands-on experience with modern open-source DE tools beyond the enterprise stack. Background in Microsoft Fabric and Azure Data Factory.
