-- Cleans raw customer base

WITH source AS (
    SELECT * FROM raw_ecommerce.customers
),

cleaned AS (
    SELECT
        customer_id,
        country,
        segment,
        CAST(registration_date AS DATE) AS registration_date
    FROM source
)

SELECT * FROM cleaned
