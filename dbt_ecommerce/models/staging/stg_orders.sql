-- Cleans raw orders: casts types, excludes dlt internals, adds derived fields

WITH source AS (
    SELECT * FROM raw_ecommerce.orders
),

cleaned AS (
    SELECT
        -- identifiers
        order_id,
        customer_id,
        product_id,

        -- dimensions
        country,
        segment,
        category,
        brand,
        status,
        CAST(is_weekend AS BOOLEAN)     AS is_weekend,

        -- time
        CAST(order_date AS DATE)        AS order_date,

        -- metrics
        CAST(quantity AS INTEGER)       AS quantity,
        CAST(unit_price_eur AS DOUBLE)  AS unit_price_eur,
        CAST(total_eur AS DOUBLE)       AS total_eur,
        CAST(total_cost_eur AS DOUBLE)  AS total_cost_eur,
        CAST(margin_eur AS DOUBLE)      AS margin_eur,

        -- derived
        CASE WHEN status = 'completed' THEN total_eur  ELSE 0 END AS revenue_eur,
        CASE WHEN status = 'completed' THEN margin_eur ELSE 0 END AS profit_eur

    FROM source
)

SELECT * FROM cleaned
