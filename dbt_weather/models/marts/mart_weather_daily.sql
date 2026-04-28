-- Daily weather summary per city
-- Ready for Looker Studio and correlation analysis with energy data

WITH staging AS (
    SELECT * FROM {{ ref('stg_weather') }}
),

daily AS (
    SELECT
        reading_date,
        day_of_week,
        city,
        latitude,
        longitude,

        -- temperature
        ROUND(AVG(temperature_c), 1)    AS avg_temp_c,
        ROUND(MAX(temperature_c), 1)    AS max_temp_c,
        ROUND(MIN(temperature_c), 1)    AS min_temp_c,

        -- other metrics
        ROUND(AVG(humidity_pct), 1)     AS avg_humidity_pct,
        ROUND(AVG(wind_speed_kmh), 1)   AS avg_wind_kmh,
        ROUND(MAX(wind_speed_kmh), 1)   AS max_wind_kmh,
        ROUND(SUM(precipitation_mm), 1) AS total_precipitation_mm,
        ROUND(AVG(cloud_cover_pct), 1)  AS avg_cloud_cover_pct,

        -- derived
        COUNT(*)                        AS hourly_readings,
        SUM(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END) AS rainy_hours

    FROM staging
    GROUP BY reading_date, day_of_week, city, latitude, longitude
)

SELECT * FROM daily
ORDER BY reading_date, city
