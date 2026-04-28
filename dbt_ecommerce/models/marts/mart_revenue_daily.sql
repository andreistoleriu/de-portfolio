-- Daily revenue breakdown by category and country
-- Primary mart for Looker Studio dashboard

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

daily AS (
    SELECT
        order_date,
        country,
        category,
        brand,
        segment,
        is_weekend,

        COUNT(*)                        AS total_orders,
        SUM(quantity)                   AS total_units,
        ROUND(SUM(total_eur), 2)        AS gross_revenue_eur,
        ROUND(SUM(revenue_eur), 2)      AS net_revenue_eur,
        ROUND(SUM(profit_eur), 2)       AS profit_eur,
        ROUND(AVG(unit_price_eur), 2)   AS avg_order_value_eur,

        COUNT(CASE WHEN status = 'completed' THEN 1 END)  AS completed_orders,
        COUNT(CASE WHEN status = 'returned'  THEN 1 END)  AS returned_orders,
        COUNT(CASE WHEN status = 'cancelled' THEN 1 END)  AS cancelled_orders

    FROM orders
    GROUP BY
        order_date, country, category,
        brand, segment, is_weekend
)

SELECT * FROM daily
ORDER BY order_date, net_revenue_eur DESC
