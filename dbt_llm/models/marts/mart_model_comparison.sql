-- Compară performanța și costul per model
-- Răspunde la: "Which model gives the best value?"

WITH staging AS (
    SELECT * FROM {{ ref('stg_llm_logs') }}
)

SELECT
    model,

    COUNT(*)                            AS total_requests,
    ROUND(SUM(cost_usd), 6)             AS total_cost_usd,
    ROUND(AVG(cost_usd), 6)             AS avg_cost_usd,
    ROUND(MIN(cost_usd), 6)             AS min_cost_usd,
    ROUND(MAX(cost_usd), 6)             AS max_cost_usd,

    ROUND(AVG(latency_ms), 0)           AS avg_latency_ms,
    ROUND(MIN(latency_ms), 0)           AS min_latency_ms,
    ROUND(MAX(latency_ms), 0)           AS max_latency_ms,

    ROUND(AVG(total_tokens), 0)         AS avg_tokens,
    ROUND(SUM(total_tokens), 0)         AS total_tokens,

    -- cost efficiency: tokens per dollar
    ROUND(
        SUM(total_tokens) / NULLIF(SUM(cost_usd), 0)
    , 0)                                AS tokens_per_dollar,

    -- speed category
    CASE
        WHEN AVG(latency_ms) < 5000  THEN 'fast'
        WHEN AVG(latency_ms) < 10000 THEN 'medium'
        ELSE                              'slow'
    END                                 AS speed_category

FROM staging
GROUP BY model
ORDER BY avg_cost_usd ASC
