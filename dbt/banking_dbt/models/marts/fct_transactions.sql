
{{ config(materialized='incremental', unique_key='transaction_id') }}

    SELECT
        t.transaction_id,
        t.account_id,
        a.customer_id,
        t.transaction_type,
        t.amount,
        t.related_account_id,
        t.status,
        TO_TIMESTAMP(t.ts_ms / 1000) AS event_time

    FROM {{ ref('stg_transactions') }} t
    left join {{ ref('stg_account') }} a
        on t.account_id = a.account_id


