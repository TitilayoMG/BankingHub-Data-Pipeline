{{ config(materialized='view') }}

WITH clean AS (
    SELECT
        CAST(customer_id AS STRING) AS customer_id,
        first_name,
        last_name,
        email,
        created_at,
        op,
        ts_ms,
        TO_TIMESTAMP(ts_ms / 1000) AS event_time,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY ts_ms DESC
        ) AS rn
    FROM {{ source('raw', 'customers') }}
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    op,
    ts_ms,
    event_time
FROM clean
WHERE rn = 1