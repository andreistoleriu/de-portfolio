"""
Dagster definitions — both energy and ecommerce pipelines.
"""

from dagster import Definitions, ScheduleDefinition, define_asset_job
from assets import raw_meter_readings, stg_meter_readings, mart_energy_daily
from assets_ecommerce import raw_ecommerce_data, stg_ecommerce, marts_ecommerce

# --- energy job ---
energy_job = define_asset_job(
    name="energy_pipeline_job",
    selection=[raw_meter_readings, stg_meter_readings, mart_energy_daily]
)

energy_schedule = ScheduleDefinition(
    job=energy_job,
    cron_schedule="0 6 * * *",
    name="energy_daily_schedule"
)

# --- ecommerce job ---
ecommerce_job = define_asset_job(
    name="ecommerce_pipeline_job",
    selection=[raw_ecommerce_data, stg_ecommerce, marts_ecommerce]
)

ecommerce_schedule = ScheduleDefinition(
    job=ecommerce_job,
    cron_schedule="0 7 * * *",
    name="ecommerce_daily_schedule"
)

# --- definitions ---
defs = Definitions(
    assets=[
        raw_meter_readings, stg_meter_readings, mart_energy_daily,
        raw_ecommerce_data, stg_ecommerce, marts_ecommerce,
    ],
    jobs=[energy_job, ecommerce_job],
    schedules=[energy_schedule, ecommerce_schedule],
)
