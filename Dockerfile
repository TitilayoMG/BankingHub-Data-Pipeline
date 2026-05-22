FROM apache/airflow:2.10.5

# switch to airflow user for pip installs
USER airflow

# RUN pip install --default-timeout=300 --retries 2 --no-cache-dir dbt-core dbt-snowflake
RUN pip install --default-timeout=300 --retries 2 --no-cache-dir \
    dbt-core \
    dbt-snowflake \
    psycopg2-binary \
    faker \
    python-dotenv \
    requests \
    confluent-kafka \
    boto3 \
    fastparquet \
    pandas \
    minio