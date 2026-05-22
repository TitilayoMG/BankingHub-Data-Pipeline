# BankingHub Data Pipeline

A full end-to-end streaming data engineering project that ingests simulated banking data from PostgreSQL through Kafka and MinIO into Snowflake, transforms it with dbt, orchestrates everything with Apache Airflow, and surfaces insights in a Power BI dashboard.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Tech Stack](#tech-stack)
3. [Project Structure](#project-structure)
4. [Setup & Prerequisites](#setup--prerequisites)
5. [Layer 1 — Data Generation (PostgreSQL)](#layer-1--data-generation-postgresql)
6. [Layer 2 — Change Data Capture (Debezium + Kafka)](#layer-2--change-data-capture-debezium--kafka)
7. [Layer 3 — Object Storage (MinIO)](#layer-3--object-storage-minio)
8. [Layer 4 — Data Warehouse (Snowflake)](#layer-4--data-warehouse-snowflake)
9. [Layer 5 — Transformations (dbt)](#layer-5--transformations-dbt)
10. [Layer 6 — Orchestration (Airflow)](#layer-6--orchestration-airflow)
11. [Layer 7 — Visualization (Power BI)](#layer-7--visualization-power-bi)
12. [Environment Variables](#environment-variables)
13. [Running the Pipeline](#running-the-pipeline)
14. [Data Quality Checks](#data-quality-checks)

---

## Architecture Overview

```
┌─────────────────┐     CDC      ┌──────────────┐    Consume    ┌──────────┐
│   PostgreSQL    │ ──────────► │   Kafka +    │ ────────────► │  MinIO   │
│  (Source DB)    │  (Debezium) │   Debezium   │  (Parquet)    │ (S3-like)│
└─────────────────┘             └──────────────┘               └────┬─────┘
                                                                     │
                                                              PUT files
                                                                     │
                                                                     ▼
                                                             ┌───────────────┐
                                                             │   Snowflake   │
                                                             │  RAW Schema   │
                                                             └───────┬───────┘
                                                                     │
                                                                  dbt run
                                                                     │
                                                       ┌─────────────▼──────────────┐
                                                       │       ANALYTICS Schema      │
                                                       │  Staging → Snapshots → Marts│
                                                       └─────────────┬──────────────┘
                                                                     │
                                                              Power BI
                                                                     │
                                                                     ▼
                                                          ┌─────────────────┐
                                                          │  Power BI       │
                                                          │  Dashboard      │
                                                          └─────────────────┘
```

The entire pipeline is orchestrated daily by **Apache Airflow**.

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Source | PostgreSQL | Operational banking database |
| CDC | Debezium | Captures row-level changes from Postgres WAL |
| Streaming | Apache Kafka | Transports CDC events between services |
| Object Storage | MinIO | Stores Parquet files (S3-compatible) |
| Data Warehouse | Snowflake | Central analytics store |
| Transformation | dbt | Staging views, SCD2 snapshots, mart tables |
| Orchestration | Apache Airflow | Schedules and monitors the pipeline |
| Visualization | Power BI | Executive banking dashboard |

---

## Project Structure

```
banking-datastack/
│
├── dags/
│   └── bankinghub_dag.py          # Airflow DAG definition
│
├── scripts/
│   ├── generate_bank_data.py      # Simulates banking transactions in PostgreSQL
│   ├── kafka_connector.py         # Registers Debezium connector via REST API
│   └── kafka_to_minio.py          # Consumes Kafka topics → Parquet → MinIO
│
├── snowflake/
│   └── snowflake_script.sql       # Snowflake setup: DB, schemas, stage, raw tables
│
├── dbt/banking_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   ├── stg_customers.sql
│   │   │   ├── stg_account.sql
│   │   │   └── stg_transactions.sql
│   │   └── marts/
│   │       ├── dim_customers.sql
│   │       ├── dim_account.sql
│   │       └── fct_transactions.sql
│   └── snapshots/
│       ├── customers_snapshot.sql
│       └── account_snapshot.sql
│
├── airflow/
│   ├── load_to_snowflake.py       # MinIO → Snowflake stage → COPY INTO raw tables
│   └── dbt_layers.py              # dbt run helpers + data quality checks
│   └── kafka_connector.py
|   └── kafka_to_minio.py
|    
└── .env                           # Environment variables (never commit this)
└── .gitignore
└── Dockerfile
└── docker-compose.yml
└── requirements.txt
└── README.md
```

---

## Setup & Prerequisites

### Requirements

- Docker & Docker Compose
- Python 3.9+
- A Snowflake account (free trial works)
- Power BI Desktop

### Python Dependencies

```bash
pip install \
  faker psycopg2-binary python-dotenv \
  minio confluent-kafka pandas pyarrow \
  snowflake-connector-python apache-airflow
```

### Infrastructure (Docker Compose)

Your `docker-compose.yml` should spin up the following services:

- `postgres` — source database
- `zookeeper` — required by Kafka
- `kafka` — message broker
- `debezium` — Kafka Connect with Debezium plugin
- `minio` — object storage
- `airflow` — workflow orchestration (webserver + scheduler)

---

## Layer 1 — Data Generation (PostgreSQL)

**File:** `generate_bank_data.py`

This script seeds a PostgreSQL database with realistic banking data and then runs in an infinite loop, inserting batches of transactions every few seconds to simulate a live operational system.

### Schema

```sql
-- customers
customer_id  SERIAL PRIMARY KEY
first_name   TEXT
last_name    TEXT
email        TEXT
created_at   TIMESTAMP DEFAULT NOW()

-- account
account_id   SERIAL PRIMARY KEY
customer_id  INTEGER REFERENCES customers(customer_id)
account_type TEXT          -- 'current' or 'savings'
balance      NUMERIC(10,2)
currency     TEXT          -- 'USD'
created_at   TIMESTAMP DEFAULT NOW()

-- transactions
transaction_id      SERIAL PRIMARY KEY
account_id          INTEGER REFERENCES account(account_id)
run_type            TEXT   -- 'deposit', 'withdrawal', 'transfer'
amount              NUMERIC(18,2)
related_account_id  INTEGER
status              TEXT   -- 'COMPLETED'
created_at          TIMESTAMP DEFAULT NOW()
```

### Configuration (top of `generate_bank_data.py`)

| Variable | Default | Description |
|---|---|---|
| `NUM_CUSTOMERS` | 10 | Number of customers to seed |
| `ACCOUNTS_PER_CUSTOMER` | 2 | One current + one savings per customer |
| `NUM_TRANSACTIONS` | 50 | Transactions per batch |
| `SLEEP_SECONDS` | 4 | Pause between batches |

### Run

```bash
python generate_bank_data.py
```

---

## Layer 2 — Change Data Capture (Debezium + Kafka)

**File:** `kafka_connector.py`

Debezium monitors the PostgreSQL Write-Ahead Log (WAL) and publishes every INSERT/UPDATE/DELETE on the three tables to dedicated Kafka topics.

### Topics Created

| Kafka Topic | Source Table |
|---|---|
| `banking.public.customers` | `public.customers` |
| `banking.public.account` | `public.account` |
| `banking.public.transactions` | `public.transactions` |

### Connector Configuration Highlights

- **Plugin:** `pgoutput` (native PostgreSQL logical replication)
- **Snapshot mode:** `initial` — on first run, Debezium snapshots all existing rows before streaming new changes
- **Decimal handling:** `double` — avoids schema complexity with `NUMERIC` columns
- **Tombstones:** disabled — no null-value delete markers are published

### Register the Connector

```bash
python kafka_connector.py
```

This sends a `PUT` request to the Debezium REST API at `http://localhost:8083`. On success you'll see a `✅ Connector created successfully` message.

> **Note:** Ensure PostgreSQL is configured for logical replication. Set `wal_level = logical` in `postgresql.conf` and restart the database.

---

## Layer 3 — Object Storage (MinIO)

**File:** `kafka_to_minio.py`

A Kafka consumer that reads from all three banking topics and writes micro-batches to MinIO as Parquet files.

### How it Works

1. Subscribes to all three Kafka topics simultaneously
2. Extracts the `after` payload from each Debezium message (representing the latest row state)
3. Buffers records in memory per table
4. Every **20 records** (`BATCH_SIZE`), flushes to MinIO as a timestamped Parquet file

### MinIO File Layout

```
banking-bucket/
├── customers/
│   ├── customers_20260511_223447_001421.parquet
│   └── customers_20260511_223451_884201.parquet
├── account/
│   └── account_20260511_223447_119843.parquet
└── transactions/
    ├── transactions_20260511_223447_332100.parquet
    └── transactions_20260511_223451_004211.parquet
```

### Run

```bash
python kafka_to_minio.py
```

Keep this running continuously alongside `generate_bank_data.py`. Press `Ctrl+C` to stop; any buffered records will be flushed before exit.

---

## Layer 4 — Data Warehouse (Snowflake)

**File:** `snowflake_script.sql`

### Initial Setup

Run the following in your Snowflake worksheet to provision the database, schemas, internal stage, and raw tables:

```sql
-- Create the database and schemas
CREATE DATABASE banking;
CREATE SCHEMA banking.raw;
CREATE SCHEMA banking.analytics;

-- Switch context
USE DATABASE banking;

-- Create an internal stage to hold Parquet files
CREATE OR REPLACE STAGE BANKING_STAGE;
```

### Raw Tables

Three raw tables mirror the structure of the Parquet files written by MinIO. They include the original columns plus Debezium metadata (`op`, `ts_ms`, `event_time`):

```sql
CREATE OR REPLACE TABLE RAW.CUSTOMERS (
    customer_id   STRING,
    first_name    STRING,
    last_name     STRING,
    email         STRING,
    created_at    TIMESTAMP_TZ,
    op            STRING,
    ts_ms         NUMBER,
    event_time    TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.ACCOUNT (
    account_id    INTEGER,
    customer_id   INTEGER,
    account_type  STRING,
    balance       NUMBER(10,2),
    currency      STRING,
    created_at    TIMESTAMP_TZ,
    op            STRING,
    ts_ms         NUMBER,
    event_time    TIMESTAMP_TZ
);

CREATE OR REPLACE TABLE RAW.TRANSACTIONS (
    transaction_id      NUMBER(38,0),
    account_id          INTEGER,
    run_type            STRING,
    amount              NUMBER(18,2),
    related_account_id  INTEGER,
    status              STRING,
    created_at          TIMESTAMP_TZ,
    op                  STRING,
    ts_ms               NUMBER,
    event_time          TIMESTAMP_TZ
);
```

### Inspecting Staged Files

```sql
-- List what's in the stage for a given table
LIST @BANKING_STAGE/customers;

-- Preview a specific Parquet file
SELECT *
FROM @BANKING_STAGE/customers
(
    FILE_FORMAT => 'parquet_format',
    PATTERN => '.*customers_20260511_223447_001421.parquet'
)
LIMIT 5;
```

---

## Layer 5 — Transformations (dbt)

dbt transforms raw Snowflake data through three ordered layers: **Staging → Snapshots → Marts**.

### dbt Project Setup

```bash
# Install dbt with Snowflake adapter
pip install dbt-snowflake

# Initialise the project
dbt init banking_dbt
cd banking_dbt

# Test connection
dbt debug
```

Configure `profiles.yml` with your Snowflake credentials (warehouse, database, role, etc.).

---

### Staging Layer — Deduplication

Staging models are **views** that deduplicate the raw CDC data. Because Debezium can emit multiple events per row (e.g., multiple updates), each model uses `ROW_NUMBER()` partitioned by the entity's primary key, ordered by `ts_ms DESC`, and keeps only `rn = 1` — the most recent version.

**`stg_customers.sql`** — deduplicates customers, casts `customer_id` to STRING.

**`stg_account.sql`** — deduplicates accounts, casts IDs to STRING, recalculates `event_time` from epoch milliseconds.

**`stg_transactions.sql`** — deduplicates transactions, renames `run_type` to `transaction_type`.

```bash
dbt run --select staging
```

---

### Snapshot Layer — SCD Type 2

Snapshots implement **Slowly Changing Dimension Type 2 (SCD2)** using dbt's built-in snapshot functionality with the `check` strategy. When a tracked column changes, dbt closes the old record (sets `dbt_valid_to`) and inserts a new one — giving you a full history of every change.

**`customers_snapshot.sql`** — tracks changes to `first_name`, `last_name`, and `email`.

**`account_snapshot.sql`** — tracks changes to `customer_id`, `account_type`, and `currency`.

Both snapshots write to the `ANALYTICS` schema.

```bash
dbt snapshot
```


---

### Marts Layer — Final Tables

Mart models are **materialised as tables** in the `ANALYTICS` schema and are the layer consumed by Power BI.

**`dim_customers.sql`** — reads from `customers_snapshot`. Provides one record per customer version with `customer_id`, name, email, and `event_time`.

**`dim_account.sql`** — reads from `account_snapshot`. Provides account dimension data including type, balance, and currency.

**`fct_transactions.sql`** — an **incremental** model (deduplicates on `transaction_id`) that joins `stg_transactions` with `stg_account` to enrich each transaction with `customer_id`. This is the primary fact table for analytics.

```bash
dbt run --select marts
```

---

### Full dbt Run Order

```
stg_customers  ─┐
stg_account    ─┤─► customers_snapshot ─► dim_customers
                │   account_snapshot   ─► dim_account
                │
stg_transactions─┤
stg_account    ─┘─► fct_transactions
```

---

## Layer 6 — Orchestration (Airflow)

**Files:** `bankinghub_dag.py`, `load_to_snowflake.py`, `dbt_layers.py`

### DAG: `banking_datastack_pipeline`

Runs **daily** and executes three tasks in sequence:

```
load_minio_to_snowflake  ──►  dbt_transformations  ──►  data_quality_checks
```

#### Task 1: `load_minio_to_snowflake`

**File:** `load_to_snowflake.py`

Performs a three-step ELT load:

1. **Extract** — Lists all `.parquet` objects in MinIO under `customers/`, `account/`, and `transactions/` prefixes and downloads them to a temp directory.
2. **Stage** — Uploads each file to the corresponding Snowflake internal stage path (`@BANKING_STAGE/<table>/`) using `PUT`.
3. **Load** — Executes `COPY INTO` for each raw table, matching Parquet columns to table columns case-insensitively. Files that fail are skipped (`ON_ERROR = SKIP_FILE`) rather than failing the whole load.

#### Task 2: `dbt_transformations`

**File:** `dbt_layers.py` → `run_dbt_pipeline()`

Calls three dbt commands in strict order using `subprocess`:

```
1. dbt run --select staging
2. dbt snapshot
3. dbt run --select marts
```

#### Task 3: `data_quality_checks`

**File:** `dbt_layers.py` → `check_data_quality()`

Connects directly to Snowflake and runs duplicate-detection queries against the three mart tables. If any duplicates are found, the task raises an exception and the DAG is marked as failed.

| Check | Table | Column |
|---|---|---|
| `duplicate_transaction_id` | `FCT_TRANSACTIONS` | `transaction_id` |
| `duplicate_customer_id` | `DIM_CUSTOMERS` | `customer_id` |
| `duplicate_account_id` | `DIM_ACCOUNT` | `account_id` |

### Airflow Setup

```bash
# Start Airflow (inside your Docker environment)
airflow db init
airflow users create --role Admin --username admin --password admin \
  --firstname Admin --lastname Admin --email admin@example.com
airflow webserver --port 8080 &
airflow scheduler &
```

Place `bankinghub_dag.py`, `load_to_snowflake.py`, and `dbt_layers.py` in your `$AIRFLOW_HOME/dags/` folder.

---

## Layer 7 — Visualization (Power BI)

### Connecting Power BI to Snowflake

1. Open **Power BI Desktop**
2. Click **Get Data → Snowflake**
3. Enter your Snowflake **Server** (e.g., `abc12345.snowflakecomputing.com`) and **Warehouse**
4. Set **Database** to `BANKING` and **Schema** to `ANALYTICS`
5. Select the three mart tables: `DIM_CUSTOMERS`, `DIM_ACCOUNT`, `FCT_TRANSACTIONS`
6. Click **Load**

### Data Model (Relationships)

In the Power BI Model view, set the following relationships:

```
DIM_CUSTOMERS (customer_id)  ◄──  FCT_TRANSACTIONS (customer_id)
DIM_ACCOUNT   (account_id)   ◄──  FCT_TRANSACTIONS (account_id)
```

Both are many-to-one from the fact table to the dimension tables.

---

### Dashboard Layout

The dashboard contains **4 KPI cards** and **4 charts**.

---

#### KPI Cards

| KPI | DAX Measure | Description |
|---|---|---|
| **Total Transaction Volume** | `SUM(FCT_TRANSACTIONS[amount])` | Sum of all transaction amounts |
| **Total Customers** | `DISTINCTCOUNT(DIM_CUSTOMERS[customer_id])` | Unique customers |
| **Total Transactions** | `COUNTROWS(FCT_TRANSACTIONS)` | Count of all transaction records |
| **Total Accounts** | `DISTINCTCOUNT(DIM_ACCOUNT[account_id])` | Unique accounts |

---

#### Chart 1 — Top Customers by Transaction Volume and Account Type

**Visual type:** Clustered Bar Chart (or Stacked Bar Chart)

| Field | Well |
|---|---|
| `DIM_CUSTOMERS[first_name] & " " & DIM_CUSTOMERS[last_name]` | Y-Axis |
| `SUM(FCT_TRANSACTIONS[amount])` | X-Axis |
| `DIM_ACCOUNT[account_type]` | Legend |


**Insight:** Identifies the highest-value customers and reveals whether their activity is concentrated in current or savings accounts.

---

#### Chart 2 — Most Active Customers

**Visual type:** Horizontal Bar Chart

| Field | Well |
|---|---|
| `DIM_CUSTOMERS[first_name] & " " & DIM_CUSTOMERS[last_name]` | Y-Axis |
| `COUNTROWS(FCT_TRANSACTIONS)` | X-Axis |


**Insight:** Shows which customers generate the most transaction events, useful for engagement and fraud monitoring.

---

#### Chart 3 — Money Flow by Transaction Type

**Visual type:** Clustered Column Chart or Line Chart

| Field | Well |
|---|---|
| `FCT_TRANSACTIONS[event_time]` (by Month or Day) | X-Axis |
| `SUM(FCT_TRANSACTIONS[amount])` | Y-Axis |
| `FCT_TRANSACTIONS[transaction_type]` | Legend |

This breaks down the total monetary flow over time into `deposit`, `withdrawal`, and `transfer` streams.

**Insight:** Reveals whether outflows (withdrawals + transfers) are outpacing inflows (deposits) over time.

---

#### Chart 4 — Transaction Type Distribution

**Visual type:** Donut Chart or Pie Chart

| Field | Well |
|---|---|
| `FCT_TRANSACTIONS[transaction_type]` | Legend |
| `COUNTROWS(FCT_TRANSACTIONS)` | Values |

**Insight:** Shows the proportional split between deposits, withdrawals, and transfers — useful for understanding customer behaviour patterns at a glance.

---

### Recommended Slicers

Add slicers to make the dashboard interactive:

- **Date range** — `FCT_TRANSACTIONS[event_time]`
- **Account type** — `DIM_ACCOUNT[account_type]`
- **Transaction type** — `FCT_TRANSACTIONS[transaction_type]`

---

## Environment Variables

Create a `.env` file in the project root. **Never commit this file.**

```env
# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_HOST_LOCAL=localhost
POSTGRES_PORT=5432
POSTGRES_DB=banking
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ENDPOINT_LOCAL=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=banking-bucket

# Snowflake
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=abc12345
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=BANKING
SNOWFLAKE_SCHEMA=RAW
SNOWFLAKE_ROLE=SYSADMIN
SNOWFLAKE_STAGE=BANKING_STAGE
```

---

## Running the Pipeline

### Step 1 — Start Infrastructure

```bash
docker-compose up -d
```

Verify all services are healthy:
- PostgreSQL: `localhost:5432`
- Kafka: `localhost:9092`
- Debezium: `http://localhost:8083`
- MinIO Console: `http://localhost:9001`
- Airflow UI: `http://localhost:8080`

### Step 2 — Register the Debezium Connector

```bash
python kafka_connector.py
```

### Step 3 — Start the Kafka → MinIO Consumer

```bash
python kafka_to_minio.py
```

### Step 4 — Generate Banking Data

```bash
python generate_bank_data.py
```

Watch the consumer terminal — you should see Parquet files being uploaded to MinIO every 20 records.

### Step 5 — Trigger the Airflow DAG

Open the Airflow UI at `http://localhost:8080`, enable the `banking_datastack_pipeline` DAG, and trigger a manual run. The three tasks will execute sequentially.

Alternatively, run the pipeline steps manually:

```bash
# Load MinIO → Snowflake
python -c "from load_to_snowflake import load_minio_to_snowflake; load_minio_to_snowflake()"

# Run dbt
cd dbt/banking_dbt
dbt run --select staging
dbt snapshot
dbt run --select marts
```

### Step 6 — Connect Power BI

Follow the [Power BI connection steps](#connecting-power-bi-to-snowflake) above and build the dashboard.

---

## Data Quality Checks

After each dbt run, Airflow automatically verifies the mart tables in Snowflake. The checks query for duplicate primary keys and raise an exception (failing the DAG) if any are found.

You can also run these manually in Snowflake:

```sql
-- Check for duplicate transactions
SELECT transaction_id
FROM BANKING.ANALYTICS.FCT_TRANSACTIONS
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Check for duplicate customers
SELECT customer_id
FROM BANKING.ANALYTICS.DIM_CUSTOMERS
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- Check for duplicate accounts
SELECT account_id
FROM BANKING.ANALYTICS.DIM_ACCOUNT
GROUP BY account_id
HAVING COUNT(*) > 1;
```

A clean pipeline returns zero rows from all three queries.