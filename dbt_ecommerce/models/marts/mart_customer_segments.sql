-- Customer segment performance summary
-- Shows value of VIP vs regular vs occasional customers

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

segment_summary AS (
    SELECT
        c.segment,
        c.country,
        COUNT(DISTINCT o.customer_id)   AS total_customers,
        COUNT(o.order_id)               AS total_orders,
        ROUND(SUM(o.revenue_eur), 2)    AS total_revenue_eur,
        ROUND(SUM(o.profit_eur), 2)     AS total_profit_eur,
        ROUND(AVG(o.revenue_eur), 2)    AS avg_order_value_eur,
        ROUND(
            SUM(o.revenue_eur) / NULLIF(COUNT(DISTINCT o.customer_id), 0)
        , 2)                            AS revenue_per_customer_eur

    FROM customers c
    LEFT JOIN orders o USING (customer_id)
    WHERE o.status = 'completed'
    GROUP BY c.segment, c.country
)

SELECT * FROM segment_summary
ORDER BY total_revenue_eur DESC
