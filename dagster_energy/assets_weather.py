"""
Dagster assets for the weather pipeline.
Real hourly weather data from Open-Meteo API for 5 Romanian cities.
"""

import subprocess
from dagster import asset, AssetExecutionContext
from pathlib import Path

PROJECT_ROOT  = Path("/home/andrei/de-portfolio")
PIPELINE_PATH = PROJECT_ROOT / "pipelines/weather/weather_pipeline.py"
DBT_PATH      = PROJECT_ROOT / "dbt_weather"
PYTHON_PATH   = PROJECT_ROOT / "venv/bin/python"
DBT_BINARY    = PROJECT_ROOT / "venv/bin/dbt"


@asset(
    group_name="weather",
    description="Real hourly weather data from Open-Meteo API for 5 Romanian cities"
)
def raw_weather_data(context: AssetExecutionContext):
    context.log.info("🌦️ Fetching real weather data from Open-Meteo...")
    result = subprocess.run(
        [str(PYTHON_PATH), str(PIPELINE_PATH)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT)
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dlt pipeline failed:\n{result.stderr}")
    context.log.info("✅ Weather data loaded")


@asset(
    deps=[raw_weather_data],
    group_name="weather",
    description="Cleaned staging model for weather data"
)
def stg_weather(context: AssetExecutionContext):
    context.log.info("🔧 Running dbt staging model...")
    result = subprocess.run(
        [str(DBT_BINARY), "run", "--select", "stg_weather"],
        capture_output=True,
        text=True,
        cwd=str(DBT_PATH)
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt staging failed:\n{result.stderr}")
    context.log.info("✅ Staging model ready")


@asset(
    deps=[stg_weather],
    group_name="weather",
    description="Daily weather summary per city — ready for Looker Studio"
)
def mart_weather_daily(context: AssetExecutionContext):
    context.log.info("📊 Running dbt mart model...")
    result = subprocess.run(
        [str(DBT_BINARY), "run", "--select", "mart_weather_daily"],
        capture_output=True,
        text=True,
        cwd=str(DBT_PATH)
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt mart failed:\n{result.stderr}")
    context.log.info("✅ Weather mart ready for Looker Studio")
