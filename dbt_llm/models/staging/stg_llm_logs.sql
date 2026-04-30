-- Curăță logurile brute din Kafka
-- Recalculează costurile uniform per model

WITH source AS (
    SELECT * FROM llm_logs_kafka
),

cleaned AS (
    SELECT
        user_id,
        model,
        CAST(timestamp AS TIMESTAMP)        AS request_timestamp,
        CAST(consumed_at AS TIMESTAMP)      AS consumed_timestamp,
        DATE(CAST(timestamp AS TIMESTAMP))  AS request_date,
        HOUR(CAST(timestamp AS TIMESTAMP))  AS request_hour,
        prompt,
        LEFT(prompt, 50)                    AS prompt_preview,
        CAST(prompt_tokens AS INTEGER)      AS prompt_tokens,
        CAST(output_tokens AS INTEGER)      AS output_tokens,
        CAST(total_tokens AS INTEGER)       AS total_tokens,
        CAST(latency_ms AS INTEGER)         AS latency_ms,
        ROUND(latency_ms / 1000.0, 2)       AS latency_sec,

        -- recalculam costul uniform per model
        ROUND(
            CASE model
                WHEN 'phi3'     THEN (prompt_tokens/1000.0)*0.0001 + (output_tokens/1000.0)*0.0002
                WHEN 'mistral'  THEN (prompt_tokens/1000.0)*0.002  + (output_tokens/1000.0)*0.006
                WHEN 'llama3.2' THEN (prompt_tokens/1000.0)*0.001  + (output_tokens/1000.0)*0.002
                ELSE cost_usd
            END
        , 6)                                AS cost_usd,

        DATEDIFF(
            'millisecond',
            CAST(timestamp AS TIMESTAMP),
            CAST(consumed_at AS TIMESTAMP)
        )                                   AS kafka_lag_ms,

        CASE
            WHEN latency_ms < 3000  THEN 'fast'
            WHEN latency_ms < 10000 THEN 'medium'
            ELSE                         'slow'
        END                               AS latency_category

    FROM source
)

SELECT * FROM cleaned
