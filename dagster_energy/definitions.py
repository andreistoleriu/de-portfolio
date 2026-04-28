"""
Dagster definitions — entry point for the Dagster UI.
"""

from dagster import Definitions, ScheduleDefinition, define_asset_job
from assets import raw_meter_readings, stg_meter_readings, mart_energy_daily

# Group all assets into one job
energy_job = define_asset_job(
    name="energy_pipeline_job",
    selection=[raw_meter_readings, stg_meter_readings, mart_energy_daily]
)

# Run every day at 6am
daily_schedule = ScheduleDefinition(
    job=energy_job,
    cron_schedule="0 6 * * *",  # 6:00 AM every day
    name="energy_daily_schedule"
)

defs = Definitions(
    assets=[raw_meter_readings, stg_meter_readings, mart_energy_daily],
    jobs=[energy_job],
    schedules=[daily_schedule],
)
