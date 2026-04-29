-- Curăță logurile brute din Kafka
-- Adaugă coloane derivate utile pentru analiză

WITH source AS (
    SELECT * FROM llm_logs_kafka
),

cleaned AS (
    SELECT
        -- identifiers
        user_id,
        model,

        -- time
        CAST(timestamp AS TIMESTAMP)        AS request_timestamp,
        CAST(consumed_at AS TIMESTAMP)      AS consumed_timestamp,

        -- derived time fields
        DATE(CAST(timestamp AS TIMESTAMP))  AS request_date,
        HOUR(CAST(timestamp AS TIMESTAMP))  AS request_hour,

        -- prompt
        prompt,
        LEFT(prompt, 50)                    AS prompt_preview,

        -- metrics
        CAST(prompt_tokens AS INTEGER)      AS prompt_tokens,
        CAST(output_tokens AS INTEGER)      AS output_tokens,
        CAST(total_tokens AS INTEGER)       AS total_tokens,
        CAST(latency_ms AS INTEGER)         AS latency_ms,
        ROUND(latency_ms / 1000.0, 2)       AS latency_sec,
        CAST(cost_usd AS DOUBLE)            AS cost_usd,

        -- kafka lag = timp între request și procesare
        DATEDIFF(
            'millisecond',
            CAST(timestamp AS TIMESTAMP),
            CAST(consumed_at AS TIMESTAMP)
        )                                   AS kafka_lag_ms,

        -- clasificare latență
        CASE
            WHEN latency_ms < 3000  THEN 'fast'
            WHEN latency_ms < 6000  THEN 'medium'
            ELSE                         'slow'
        END                               AS latency_category

    FROM source
)

SELECT * FROM cleaned
