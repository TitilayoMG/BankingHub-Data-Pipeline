{{ config(materialized='view') }}

WITH clean AS (

    SELECT
        CAST(account_id AS STRING) AS account_id,
        CAST(customer_id AS STRING) AS customer_id,
        account_type,
        balance,
        currency,
        created_at,
        op,
        ts_ms,
        TO_TIMESTAMP(ts_ms / 1000) AS event_time,

        ROW_NUMBER() OVER (
            PARTITION BY account_id
            ORDER BY ts_ms DESC
        ) AS rn

    FROM {{ source('raw', 'account') }}

)

SELECT
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    created_at,
    op,
    ts_ms,
    event_time

FROM clean
WHERE rn = 1