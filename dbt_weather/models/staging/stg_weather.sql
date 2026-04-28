-- Cleans raw weather data from Open-Meteo API
-- Casts types, renames fields for clarity

WITH source AS (
    SELECT * FROM raw_weather.hourly_weather
),

cleaned AS (
    SELECT
        -- identifiers
        city,
        latitude,
        longitude,

        -- time
        CAST(timestamp AS TIMESTAMP)    AS reading_timestamp,
        CAST(date AS DATE)              AS reading_date,
        hour                            AS reading_hour,
        day_of_week,
        CAST(is_day AS BOOLEAN)         AS is_day,

        -- metrics
        CAST(temperature_c AS DOUBLE)   AS temperature_c,
        CAST(humidity_pct AS DOUBLE)    AS humidity_pct,
        CAST(wind_speed_kmh AS DOUBLE)  AS wind_speed_kmh,
        CAST(precipitation_mm AS DOUBLE)AS precipitation_mm,
        CAST(cloud_cover_pct AS DOUBLE) AS cloud_cover_pct

    FROM source
)

SELECT * FROM cleaned
