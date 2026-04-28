-- models/staging/stg_meter_readings.sql
-- Cleans raw dlt data: removes internal columns, casts types, renames fields

WITH source AS (
    SELECT * FROM raw_energy.meter_readings
),

cleaned AS (
    SELECT
        -- identifiers
        reading_id,
        site_id,

        -- dimensions
        city,
        site_type,
        day_of_week,

        -- time
        CAST(timestamp AS TIMESTAMP)    AS reading_timestamp,
        hour                            AS reading_hour,
        CAST(is_weekend AS BOOLEAN)     AS is_weekend,

        -- metrics
        CAST(consumption_kwh AS DOUBLE) AS consumption_kwh,
        CAST(cost_eur AS DOUBLE)        AS cost_eur

        -- note: _dlt_id and _dlt_load_id intentionally excluded
    FROM source
)

SELECT * FROM cleaned