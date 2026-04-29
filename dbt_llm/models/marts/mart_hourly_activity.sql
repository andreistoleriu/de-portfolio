-- Activitate pe ore si zile
-- Răspunde la: "Când e cel mai mult trafic?"

WITH staging AS (
    SELECT * FROM {{ ref('stg_llm_logs') }}
)

SELECT
    request_date,
    request_hour,
    COUNT(*)                            AS total_requests,
    COUNT(DISTINCT user_id)             AS unique_users,
    SUM(total_tokens)                   AS total_tokens,
    ROUND(SUM(cost_usd), 4)             AS total_cost_usd,
    ROUND(AVG(latency_ms), 0)           AS avg_latency_ms,
    ROUND(AVG(kafka_lag_ms), 0)         AS avg_kafka_lag_ms

FROM staging
GROUP BY request_date, request_hour
ORDER BY request_date, request_hour
