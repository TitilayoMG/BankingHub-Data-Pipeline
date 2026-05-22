from minio import Minio
import snowflake.connector
import tempfile
import logging
import os
from dotenv import load_dotenv


load_dotenv()
# =====================================
# LOGGING
# =====================================
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# =====================================
# CONFIG
# =====================================
BUCKET_NAME = os.getenv("MINIO_BUCKET")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE")

TABLES = ["customers", "account", "transactions"]

TABLE_MAPPING = {
    "customers": "RAW.CUSTOMERS",
    "account": "RAW.ACCOUNT",
    "transactions": "RAW.TRANSACTIONS",
}


# =====================================
# HELPERS
# =====================================
def get_minio_client():
    return Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )


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


# =====================================
# MAIN TASK
# =====================================
def load_minio_to_snowflake():
    logger.info("Starting MinIO -> Snowflake pipeline")

    client = get_minio_client()
    downloaded_files = {}

    total_downloaded = 0
    total_skipped = 0
    total_uploaded = 0

    try:
        # ---------------------------------
        # STEP 1: Extract files from MinIO
        # ---------------------------------
        logger.info("Step 1: Extracting parquet files from MinIO")

        for table in TABLES:
            prefix = f"{table}/"
            objects = client.list_objects(
                BUCKET_NAME,
                prefix=prefix,
                recursive=True,
            )

            table_files = []
            downloaded_count = 0
            skipped_count = 0

            for obj in objects:
                object_name = obj.object_name

                if not object_name.endswith(".parquet"):
                    skipped_count += 1
                    total_skipped += 1
                    continue

                response = client.get_object(BUCKET_NAME, object_name)
                file_data = response.read()

                filename = os.path.basename(object_name)
                local_path = os.path.join(
                    tempfile.gettempdir(),
                    filename
                )

                with open(local_path, "wb") as file:
                    file.write(file_data)

                table_files.append(local_path)
                downloaded_count += 1
                total_downloaded += 1

            downloaded_files[table] = table_files

            logger.info(
                f"{table}: downloaded={downloaded_count}, skipped={skipped_count}"
            )

        logger.info(
            f"Extraction complete | total_downloaded={total_downloaded}, total_skipped={total_skipped}"
        )

        # ---------------------------------
        # STEP 2: Upload to Snowflake stage
        # ---------------------------------
        logger.info("Step 2: Uploading files to Snowflake internal stage")

        with get_snowflake_conn() as conn:
            with conn.cursor() as cursor:

                for table, files in downloaded_files.items():
                    uploaded_count = 0

                    for file_path in files:
                        put_sql = f"""
                        PUT file://{file_path}
                        @{SNOWFLAKE_STAGE}/{table}
                        AUTO_COMPRESS=FALSE
                        OVERWRITE=TRUE;
                        """

                        cursor.execute(put_sql)
                        uploaded_count += 1
                        total_uploaded += 1

                    logger.info(
                        f"{table}: uploaded={uploaded_count}"
                    )

        logger.info(
            f"Upload complete | total_uploaded={total_uploaded}"
        )

        # ---------------------------------
        # STEP 3: COPY INTO raw tables
        # ---------------------------------
        logger.info("Step 3: Loading staged files into raw tables")

        with get_snowflake_conn() as conn:
            with conn.cursor() as cursor:

                for table, target_table in TABLE_MAPPING.items():
                    copy_sql = f"""
                    COPY INTO {target_table}
                    FROM @{SNOWFLAKE_STAGE}/{table}
                    FILE_FORMAT = (TYPE = PARQUET)
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    PATTERN='.*\\.parquet'
                    ON_ERROR = SKIP_FILE;
                    """

                    cursor.execute(copy_sql)

                    logger.info(
                        f"{table}: loaded successfully into {target_table}"
                    )

        logger.info("Pipeline completed successfully")

    except Exception as error:
        logger.exception(f"Pipeline failed: {str(error)}")
        raise
