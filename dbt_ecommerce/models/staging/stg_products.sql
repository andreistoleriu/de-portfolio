-- Cleans raw product catalog

WITH source AS (
    SELECT * FROM raw_ecommerce.products
),

cleaned AS (
    SELECT
        product_id,
        name            AS product_name,
        category,
        brand,
        CAST(price_eur   AS DOUBLE) AS price_eur,
        CAST(cost_eur    AS DOUBLE) AS cost_eur,
        CAST(margin_pct  AS DOUBLE) AS margin_pct
    FROM source
)

SELECT * FROM cleaned
