from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from load_to_snowflake import load_minio_to_snowflake
from dbt_layers import run_dbt_pipeline, check_data_quality

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="banking_datastack_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["snowflake", "minio", "banking"],
) as dag:

    load_task = PythonOperator(
        task_id="load_minio_to_snowflake",
        python_callable=load_minio_to_snowflake
    )

    dbt_layers = PythonOperator(
        task_id="dbt_transformations",
        python_callable=run_dbt_pipeline
    )

    run_data_quality_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=check_data_quality
    )

    load_task >> dbt_layers >> run_data_quality_checks