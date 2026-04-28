"""
Dagster assets for the energy pipeline.
"""

import subprocess
from dagster import asset, AssetExecutionContext
from pathlib import Path

PROJECT_ROOT   = Path("/home/andrei/de-portfolio")
PIPELINE_PATH  = PROJECT_ROOT / "pipelines/energy/energy_pipeline.py"
DBT_PATH       = PROJECT_ROOT / "dbt_energy"
PYTHON_PATH    = PROJECT_ROOT / "venv/bin/python"
DBT_BINARY     = PROJECT_ROOT / "venv/bin/dbt"


@asset(
    group_name="energy",
    description="Raw hourly meter readings loaded via dlt into DuckDB"
)
def raw_meter_readings(context: AssetExecutionContext):
    context.log.info("🔌 Starting dlt ingestion...")
    result = subprocess.run(
        [str(PYTHON_PATH), str(PIPELINE_PATH)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT)
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dlt pipeline failed:\n{result.stderr}")
    context.log.info("✅ Raw meter readings loaded")


@asset(
    deps=[raw_meter_readings],
    group_name="energy",
    description="Cleaned staging model built with dbt"
)
def stg_meter_readings(context: AssetExecutionContext):
    context.log.info("🔧 Running dbt staging model...")
    result = subprocess.run(
        [str(DBT_BINARY), "run", "--select", "stg_meter_readings"],
        capture_output=True,
        text=True,
        cwd=str(DBT_PATH)
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt staging failed:\n{result.stderr}")
    context.log.info("✅ Staging model ready")


@asset(
    deps=[stg_meter_readings],
    group_name="energy",
    description="Daily aggregated mart table ready for Looker Studio"
)
def mart_energy_daily(context: AssetExecutionContext):
    context.log.info("📊 Running dbt mart model...")
    result = subprocess.run(
        [str(DBT_BINARY), "run", "--select", "mart_energy_daily"],
        capture_output=True,
        text=True,
        cwd=str(DBT_PATH)
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt mart failed:\n{result.stderr}")
    context.log.info("✅ Mart ready for Looker Studio")
