# Data Engineering Portfolio

A production-grade data engineering portfolio built with modern open-source tools, demonstrating end-to-end pipeline development from ingestion to visualization.

## Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion | dlt | Extract and load data into DuckDB |
| Storage | DuckDB | Local analytical database |
| Transformation | dbt | Staging and mart models with tests |
| Orchestration | Dagster | Pipeline scheduling and monitoring |
| Visualization | Looker Studio | Business dashboards |

## Pipelines

### 1. Energy Consumption Analytics

Simulates smart meter data across 5 German cities — residential, commercial and industrial sites — over 30 days of hourly readings (3,600 rows). The pipeline models realistic consumption patterns including peak hours, weekend drops and site-type profiles, mimicking the kind of data handled in utility companies.

The dlt ingestion layer loads raw hourly readings into DuckDB. dbt then builds a staging model that cleans and casts the raw data, and a daily mart model that aggregates consumption and cost metrics per site. Eight data quality tests run automatically on every execution. Dagster orchestrates the full pipeline as a DAG with a daily schedule at 06:00 UTC.

**Dashboard:** [View in Looker Studio](https://datastudio.google.com/s/mDf51wLWfjE)

## Project Structure

```
de-portfolio/
├── pipelines/
│   └── energy/
│       └── energy_pipeline.py    # dlt ingestion script
├── dbt_energy/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_meter_readings.sql
│   │   │   └── stg_meter_readings.yml
│   │   └── marts/
│   │       ├── mart_energy_daily.sql
│   │       └── mart_energy_daily.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   └── dbt_project.yml
├── dagster_energy/
│   ├── assets.py
│   └── definitions.py
├── requirements.txt
└── README.md
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

# 4. Launch Dagster and run the pipeline
cd dagster_energy
dagster dev -f definitions.py
# Open http://localhost:3000 → click Materialize all
```

## Author

**Andrei Stoleriu** — Data Engineer at Eon Software Development

Building this portfolio to demonstrate hands-on experience with modern open-source DE tools beyond the enterprise stack.
