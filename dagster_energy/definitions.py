"""
Dagster definitions — toate pipeline-urile.
"""

from dagster import Definitions, ScheduleDefinition, define_asset_job
from assets import raw_meter_readings, stg_meter_readings, mart_energy_daily
from assets_ecommerce import raw_ecommerce_data, stg_ecommerce, marts_ecommerce
from assets_weather import raw_weather_data, stg_weather, mart_weather_daily
from assets_llm import llm_traffic, llm_dbt_models, anomaly_check

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

# --- llm observability ---
llm_job = define_asset_job(
    name="llm_observability_job",
    selection=[llm_traffic, llm_dbt_models, anomaly_check]
)
llm_schedule = ScheduleDefinition(
    job=llm_job,
    cron_schedule="*/30 * * * *",  # la fiecare 30 minute
    name="llm_observability_schedule"
)

# --- definitions ---
defs = Definitions(
    assets=[
        raw_meter_readings, stg_meter_readings, mart_energy_daily,
        raw_ecommerce_data, stg_ecommerce, marts_ecommerce,
        raw_weather_data, stg_weather, mart_weather_daily,
        llm_traffic, llm_dbt_models, anomaly_check,
    ],
    jobs=[energy_job, ecommerce_job, weather_job, llm_job],
    schedules=[energy_schedule, ecommerce_schedule, weather_schedule, llm_schedule],
)
