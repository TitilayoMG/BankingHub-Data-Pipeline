
from dotenv import load_dotenv
import snowflake.connector
import os
import subprocess
import logging

log = logging.getLogger(__name__)

load_dotenv()

DBT_PROJECT_DIR = "/opt/airflow/dbt/banking_dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt/banking_dbt"


# ---------------------------
# SNOWFLAKE CONNECTION
# ---------------------------
def get_snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE"),
    )


# ---------------------------
# DBT RUN HELPERS (SIMPLIFIED)
# ---------------------------
def run_cmd(command: str, error_msg: str):
    result = subprocess.run(
        command,
        shell=True,
        capture_output=True,
        text=True,
        cwd="/opt/airflow/dbt"
    )

    log.info(result.stdout)

    if result.stderr:
        log.warning(result.stderr)

    if result.returncode != 0:
        raise Exception(error_msg)


def run_dbt_staging():
    run_cmd(
        f"dbt run --select staging "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}",
        "dbt staging models failed"
    )


def run_dbt_snapshots():
    # IMPORTANT: snapshots do NOT use --select
    run_cmd(
        f"dbt snapshot "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}",
        "dbt snapshots failed"
    )


def run_dbt_marts():
    run_cmd(
        f"dbt run --select marts "
        f"--project-dir {DBT_PROJECT_DIR} "
        f"--profiles-dir {DBT_PROFILES_DIR}",
        "dbt marts models failed"
    )


# ---------------------------
# PIPELINE (FIXED ORDER)
# ---------------------------
def run_dbt_pipeline():
    """
    Correct order:
    1. staging
    2. snapshots
    3. marts
    """

    log.info("Running staging models")
    run_dbt_staging()

    log.info("Running snapshots")
    run_dbt_snapshots()

    log.info("Running marts")
    run_dbt_marts()


# ---------------------------
# DATA QUALITY CHECKS (Snowflake)
# ---------------------------
def check_data_quality():
    conn = get_snowflake_conn()
    cursor = conn.cursor()

    checks = {
        "duplicate_transaction_id": """
            SELECT transaction_id
            FROM BANKING.ANALYTICS.FCT_TRANSACTIONS
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        """,

        "duplicate_customer_id": """
            SELECT customer_id
            FROM BANKING.ANALYTICS.DIM_CUSTOMERS
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        """,

        "duplicate_account_id": """
            SELECT account_id
            FROM BANKING.ANALYTICS.DIM_ACCOUNT
            GROUP BY account_id
            HAVING COUNT(*) > 1
        """
    }

    for check_name, query in checks.items():
        cursor.execute(query)
        result = cursor.fetchall()

        log.info(f"{check_name}: {result}")

        if result:
            raise ValueError(
                f"Data quality failed: {check_name} has duplicates"
            )

    cursor.close()
    conn.close()