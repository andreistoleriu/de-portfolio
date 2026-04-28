-- models/marts/mart_energy_daily.sql
-- Daily energy consumption summary per site
-- This is the model analysts and Looker Studio will query

WITH staging AS (
    SELECT * FROM {{ ref('stg_meter_readings') }}
),

daily_aggregated AS (
    SELECT
        -- date dimension
        DATE(reading_timestamp)             AS reading_date,
        day_of_week,
        is_weekend,

        -- site dimension
        site_id,
        city,
        site_type,

        -- metrics
        COUNT(*)                            AS total_readings,
        ROUND(SUM(consumption_kwh), 3)      AS total_kwh,
        ROUND(AVG(consumption_kwh), 3)      AS avg_kwh_per_hour,
        ROUND(MAX(consumption_kwh), 3)      AS peak_kwh,
        ROUND(MIN(consumption_kwh), 3)      AS min_kwh,
        ROUND(SUM(cost_eur), 2)             AS total_cost_eur

    FROM staging
    GROUP BY
        DATE(reading_timestamp),
        day_of_week,
        is_weekend,
        site_id,
        city,
        site_type
)

SELECT * FROM daily_aggregated
ORDER BY reading_date, site_id
