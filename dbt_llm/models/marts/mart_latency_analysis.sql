-- Analiza latentei per prompt
-- Răspunde la: "Care întrebări sunt cele mai lente și mai scumpe?"

WITH staging AS (
    SELECT * FROM {{ ref('stg_llm_logs') }}
)

SELECT
    prompt_preview,
    COUNT(*)                            AS times_asked,
    ROUND(AVG(latency_ms), 0)           AS avg_latency_ms,
    ROUND(AVG(latency_sec), 2)          AS avg_latency_sec,
    ROUND(AVG(total_tokens), 0)         AS avg_tokens,
    ROUND(AVG(cost_usd), 4)             AS avg_cost_usd,
    ROUND(SUM(cost_usd), 4)             AS total_cost_usd,
    latency_category

FROM staging
GROUP BY prompt_preview, latency_category
ORDER BY avg_latency_ms DESC
