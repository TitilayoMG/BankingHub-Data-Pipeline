


{{ config(materialized='table') }}

WITH final_table AS (
    SELECT
        account_id,
        customer_id,
        account_type,
        balance,
        currency,
        TO_TIMESTAMP(ts_ms / 1000) AS event_time
    FROM {{ ref('account_snapshot') }}
)

SELECT *
FROM final_table