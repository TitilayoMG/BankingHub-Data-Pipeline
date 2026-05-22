{{ config(materialized='view') }}

WITH clean AS (

    SELECT
        CAST(transaction_id AS STRING) AS transaction_id,
        CAST(account_id AS STRING) AS account_id,
        run_type AS transaction_type,
        amount,
        CAST(related_account_id AS STRING) AS related_account_id,
        status,
        created_at,
        op,
        ts_ms,
        TO_TIMESTAMP(ts_ms / 1000) AS event_time,

        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY ts_ms DESC
        ) AS rn

    FROM {{ source('raw', 'transactions') }}

)

SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    related_account_id,
    status,
    created_at,
    op,
    ts_ms,
    event_time

FROM clean
WHERE rn = 1