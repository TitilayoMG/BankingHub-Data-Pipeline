CREATE DATABASE banking
CREATE SCHEMA banking.raw


USE DATABASE banking

CREATE OR REPLACE STAGE BANKING_STAGE;

-- list the files for customers table
LIST @BANKING_STAGE/customers;

-- to show the content in one file from customers table
SELECT *
FROM @BANKING_STAGE/customers
(
    FILE_FORMAT => 'parquet_format',
    PATTERN => '.*customers_20260511_223447_001421.parquet'
)
LIMIT 5;


CREATE OR REPLACE TABLE RAW.CUSTOMERS (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    created_at TIMESTAMP_TZ,
    op STRING,
    ts_ms NUMBER,
    event_time TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.ACCOUNT (
    account_id INTEGER,
    customer_id INTEGER,
    account_type STRING,
    balance NUMBER(10,2),
    currency STRING,
    created_at TIMESTAMP_TZ,
    op STRING,
    ts_ms NUMBER,
    event_time TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.TRANSACTIONS (
    transaction_id NUMBER(38,0),
    account_id INTEGER,
    run_type STRING,
    amount NUMBER(18,2),
    related_account_id INTEGER,
    status STRING,
    created_at TIMESTAMP_TZ,
    op STRING,
    ts_ms NUMBER,
    event_time TIMESTAMP_TZ
);

TRUNCATE TABLE dim_CUSTOMERS;
TRUNCATE TABLE dim_ACCOUNT;
TRUNCATE TABLE fct_TRANSACTIONS;

drop table fct_transactions


SELECT distinct(op) FROM RAW.CUSTOMERS LIMIT 10;

SELECT count(*) FROM raw.account

SELECT count(*) FROM stg_account WHERE TRANSACTION_ID = 1

-- this should be automatically created in dbt init
CREATE SCHEMA IF NOT EXISTS BANKING.ANALYTICS;

REMOVE @BANKING_STAGE/customers;
REMOVE @BANKING_STAGE/account;
REMOVE @BANKING_STAGE/transactions;

SELECT distinct(customer_id)
FROM raw.customers -- 60 rows

SELECT *
FROM stg_customers
WHERE customer_id = 18;

SELECT distinct(account_id) --120 rows
FROM raw.account

SELECT *
FROM stg_customers
WHERE customer_id = 18;


-- changed id = 18 email from jermainemiranda@example.com to 'new_email@example.com' in postgres
select * from BANKING.ANALYTICS.customers_snapshot -- LIMIT 10
where customer_id in (18, 62, 26)






