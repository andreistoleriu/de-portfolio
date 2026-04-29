# Data Engineering Portfolio

A production-grade data engineering portfolio demonstrating end-to-end pipeline development across four domains, from real-time LLM observability to batch analytics.

## Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Ingestion (batch) | dlt | Extract and load data into DuckDB |
| Ingestion (streaming) | Apache Kafka | Real-time event streaming |
| Local LLM | Ollama + Llama 3.2 | GPU-accelerated local inference |
| Storage | DuckDB | Local analytical database |
| Transformation | dbt | Staging and mart models with tests |
| Orchestration | Dagster | Pipeline scheduling and monitoring |
| Visualization | Looker Studio | Business dashboards |
| Containerization | Docker | Kafka infrastructure |

## Pipelines

### 1. 🤖 LLM Observability Platform — Real-Time Streaming

The most technically complex pipeline. Runs a local Llama 3.2 model (via Ollama on NVIDIA RTX 3060) and streams every LLM call through Apache Kafka into DuckDB in real-time. Tracks token usage, latency, and simulated cost per user.

**Architecture:**
```
Ollama (Llama 3.2 on RTX 3060)
        ↓
Python Producer → Kafka topic "llm_events"
        ↓
Python Consumer (always-on) → DuckDB
        ↓
dbt models (cost, latency, hourly activity)
        ↓
Dagster (runs every 30 minutes + anomaly detection)
        ↓
Looker Studio dashboard
```

Key metrics: cost per user, prompt latency distribution, token usage over time, anomaly detection. Dagster anomaly_check asset alerts when cost spikes 3x above average. Four dbt data quality tests run on every execution.

**Dashboard:** [View in Looker Studio](https://datastudio.google.com/reporting/d8a8e4ce-1610-4348-92eb-07c7f8c3199a)

---

### 2. 🌦️ Weather Data Analytics — Real API

Ingests real hourly weather data for 5 Romanian cities (Bucharest, Cluj-Napoca, Iași, Timișoara, Constanța) from the Open-Meteo API — no API key required. Covers temperature, humidity, wind speed, precipitation and cloud cover over 30 days (3,720 rows of real data).

Demonstrates working with a live REST API, columnar-to-row transformation, and building analytical models on real-world data. Dagster schedules daily refresh at 08:00 UTC.

**Dashboard:** [View in Looker Studio](https://datastudio.google.com/reporting/011208a6-86af-4f7c-870f-cbe17c482685)

---

### 3. 🛒 E-commerce Sales Analytics

Simulates 90 days of online retail transactions across 6 European countries, with 500 customers segmented into VIP, regular and occasional tiers, and 45 products across 5 categories. Order volume varies by day of week and end-of-month peaks.

dbt builds 5 models covering daily revenue breakdowns and customer segment performance. Key metrics: net revenue, profit margin, return rate and revenue per customer. Eight data quality tests validate uniqueness, nullability and accepted values.

**Dashboard:** [View in Looker Studio](https://datastudio.google.com/reporting/a82da509-d30f-44d8-99e7-87bfb631ac15)

---

### 4. ⚡ Energy Consumption Analytics

Simulates smart meter data across 5 German cities — residential, commercial and industrial sites — over 30 days of hourly readings (3,600 rows). Models realistic consumption patterns including peak hours, weekend drops and site-type profiles, mimicking data handled in utility companies like Eon.

dbt builds a staging model that cleans raw dlt data, and a daily mart aggregating consumption and cost metrics per site. Eight data quality tests run on every execution. Dagster schedules the pipeline at 06:00 UTC daily.

**Dashboard:** [View in Looker Studio](https://datastudio.google.com/s/mDf51wLWfjE)

---

## Project Structure

```
de-portfolio/
├── pipelines/
│   ├── energy/energy_pipeline.py        # dlt — simulated smart meter data
│   ├── ecommerce/ecommerce_pipeline.py  # dlt — simulated e-commerce orders
│   └── weather/weather_pipeline.py      # dlt — real Open-Meteo API
├── llm_observability/
│   └── src/
│       ├── llm_producer.py              # direct to DuckDB (etapa 1)
│       ├── llm_producer_kafka.py        # Kafka producer (etapa 2)
│       ├── llm_consumer.py              # Kafka consumer — always-on
│       └── send_prompt.py              # interactive prompt tool
├── dbt_energy/                          # 2 models, 8 tests
├── dbt_ecommerce/                       # 5 models, 8 tests
├── dbt_weather/                         # 2 models, 4 tests
├── dbt_llm/                             # 4 models, 4 tests
├── dagster_energy/
│   ├── assets.py                        # energy assets
│   ├── assets_ecommerce.py              # ecommerce assets
│   ├── assets_weather.py                # weather assets
│   ├── assets_llm.py                    # LLM observability assets
│   └── definitions.py                   # all pipelines + schedules
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
pip install -r requirements.txt

# 3. Start Kafka (for LLM pipeline)
cd llm_observability
docker compose up -d

# 4. Start Kafka consumer (always-on)
python src/llm_consumer.py &

# 5. Launch Dagster — orchestrates all 4 pipelines
cd ../dagster_energy
dagster dev -f definitions.py
# Open http://localhost:3000 → Materialize all
```

## Schedules

| Pipeline | Schedule |
|----------|----------|
| LLM Observability | Every 30 minutes |
| Energy | Daily at 06:00 UTC |
| E-commerce | Daily at 07:00 UTC |
| Weather | Daily at 08:00 UTC |

## Author

**Andrei Stoleriu** — Data Engineer at Eon Software Development

Building this portfolio to demonstrate hands-on experience with modern open-source DE tools beyond the enterprise stack. Background in Microsoft Fabric and Azure Data Factory, expanding into code-first and real-time data engineering.
