

{{ config(materialized='table') }}

WITH final_table AS (

    SELECT
        customer_id,
        first_name,
        last_name,
        email,
        TO_TIMESTAMP(ts_ms / 1000) AS event_time

    FROM {{ ref('customers_snapshot') }}
)
SELECT *
FROM final_table