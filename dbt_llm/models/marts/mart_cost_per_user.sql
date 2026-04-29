-- Cost și utilizare per user
-- Răspunde la: "Cine generează cele mai multe costuri?"

WITH staging AS (
    SELECT * FROM {{ ref('stg_llm_logs') }}
)

SELECT
    user_id,

    COUNT(*)                            AS total_requests,
    SUM(total_tokens)                   AS total_tokens,
    ROUND(SUM(cost_usd), 4)             AS total_cost_usd,
    ROUND(AVG(cost_usd), 4)             AS avg_cost_per_request,
    ROUND(AVG(latency_ms), 0)           AS avg_latency_ms,
    ROUND(AVG(total_tokens), 0)         AS avg_tokens_per_request,

    -- cel mai scump request
    ROUND(MAX(cost_usd), 4)             AS max_cost_usd,

    -- distributia latentei
    COUNT(CASE WHEN latency_category = 'fast'   THEN 1 END) AS fast_requests,
    COUNT(CASE WHEN latency_category = 'medium' THEN 1 END) AS medium_requests,
    COUNT(CASE WHEN latency_category = 'slow'   THEN 1 END) AS slow_requests

FROM staging
GROUP BY user_id
ORDER BY total_cost_usd DESC
