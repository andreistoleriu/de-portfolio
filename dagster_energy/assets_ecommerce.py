"""
Dagster assets for the e-commerce pipeline.
"""

import subprocess
from dagster import asset, AssetExecutionContext
from pathlib import Path

PROJECT_ROOT  = Path("/home/andrei/de-portfolio")
PIPELINE_PATH = PROJECT_ROOT / "pipelines/ecommerce/ecommerce_pipeline.py"
DBT_PATH      = PROJECT_ROOT / "dbt_ecommerce"
PYTHON_PATH   = PROJECT_ROOT / "venv/bin/python"
DBT_BINARY    = PROJECT_ROOT / "venv/bin/dbt"


@asset(
    group_name="ecommerce",
    description="Raw orders, customers and products loaded via dlt"
)
def raw_ecommerce_data(context: AssetExecutionContext):
    context.log.info("🛒 Starting e-commerce dlt ingestion...")
    result = subprocess.run(
        [str(PYTHON_PATH), str(PIPELINE_PATH)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT)
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dlt pipeline failed:\n{result.stderr}")
    context.log.info("✅ Raw e-commerce data loaded")


@asset(
    deps=[raw_ecommerce_data],
    group_name="ecommerce",
    description="Cleaned staging models for orders, customers and products"
)
def stg_ecommerce(context: AssetExecutionContext):
    context.log.info("🔧 Running dbt staging models...")
    result = subprocess.run(
        [str(DBT_BINARY), "run", "--select", "staging"],
        capture_output=True,
        text=True,
        cwd=str(DBT_PATH)
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt staging failed:\n{result.stderr}")
    context.log.info("✅ Staging models ready")


@asset(
    deps=[stg_ecommerce],
    group_name="ecommerce",
    description="Revenue daily and customer segments marts"
)
def marts_ecommerce(context: AssetExecutionContext):
    context.log.info("📊 Running dbt mart models...")
    result = subprocess.run(
        [str(DBT_BINARY), "run", "--select", "marts"],
        capture_output=True,
        text=True,
        cwd=str(DBT_PATH)
    )
    context.log.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt marts failed:\n{result.stderr}")
    context.log.info("✅ Marts ready for Looker Studio")
