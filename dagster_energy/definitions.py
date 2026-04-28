"""
Dagster definitions — all 3 pipelines: energy, ecommerce, weather.
"""

from dagster import Definitions, ScheduleDefinition, define_asset_job
from assets import raw_meter_readings, stg_meter_readings, mart_energy_daily
from assets_ecommerce import raw_ecommerce_data, stg_ecommerce, marts_ecommerce
from assets_weather import raw_weather_data, stg_weather, mart_weather_daily

# --- energy ---
energy_job = define_asset_job(
    name="energy_pipeline_job",
    selection=[raw_meter_readings, stg_meter_readings, mart_energy_daily]
)
energy_schedule = ScheduleDefinition(
    job=energy_job,
    cron_schedule="0 6 * * *",
    name="energy_daily_schedule"
)

# --- ecommerce ---
ecommerce_job = define_asset_job(
    name="ecommerce_pipeline_job",
    selection=[raw_ecommerce_data, stg_ecommerce, marts_ecommerce]
)
ecommerce_schedule = ScheduleDefinition(
    job=ecommerce_job,
    cron_schedule="0 7 * * *",
    name="ecommerce_daily_schedule"
)

# --- weather ---
weather_job = define_asset_job(
    name="weather_pipeline_job",
    selection=[raw_weather_data, stg_weather, mart_weather_daily]
)
weather_schedule = ScheduleDefinition(
    job=weather_job,
    cron_schedule="0 8 * * *",
    name="weather_daily_schedule"
)

# --- definitions ---
defs = Definitions(
    assets=[
        raw_meter_readings, stg_meter_readings, mart_energy_daily,
        raw_ecommerce_data, stg_ecommerce, marts_ecommerce,
        raw_weather_data, stg_weather, mart_weather_daily,
    ],
    jobs=[energy_job, ecommerce_job, weather_job],
    schedules=[energy_schedule, ecommerce_schedule, weather_schedule],
)
