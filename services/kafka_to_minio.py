

import os
import json
from io import BytesIO
from datetime import datetime
from collections import defaultdict

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from confluent_kafka import Consumer, KafkaException

load_dotenv()

# ----------------------------------
# CONFIG
# ----------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT_LOCAL")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = os.getenv("MINIO_BUCKET")

BATCH_SIZE = 20

TOPICS = [
    "banking.public.customers",
    "banking.public.account",
    "banking.public.transactions",
]

# ----------------------------------
# MINIO CLIENT
# ----------------------------------
minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# create bucket if missing
if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)
    print(f"Created bucket: {BUCKET_NAME}")


# ----------------------------------
# KAFKA CONSUMER
# ----------------------------------
consumer_config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": "kafka_debz",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
}
consumer = Consumer(consumer_config)
consumer.subscribe(TOPICS)

print(f"Subscribed to topics: {TOPICS}")
print("Listening for Kafka messages...")


# store records by table
buffers = defaultdict(list)


# ----------------------------------
# HELPERS
# ----------------------------------
def extract_table_name(topic):
    """
    banking.public.transactions -> transactions
    """
    return topic.split(".")[-1]


def process_debezium_message(message):
    """
    Extract actual row data from Debezium payload
    """
    payload = message.get("payload", {})

    # use 'after' for inserts/updates
    after = payload.get("after")
    record = {
        **after,
        "op": payload.get("op"),
        "ts_ms": payload.get("ts_ms"),
        "event_time": datetime.fromtimestamp(payload.get("ts_ms") / 1000.0)
    }

    return record


def upload_batch(table_name, records):
    if not records:
        return

    df = pd.DataFrame(records)

    # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    file_name = f"{table_name}_{timestamp}.parquet"
    object_path = f"{table_name}/{file_name}"

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
    parquet_buffer.seek(0)

    minio_client.put_object(
        BUCKET_NAME,
        object_path,
        parquet_buffer,
        length=parquet_buffer.getbuffer().nbytes,
        content_type="application/octet-stream"
    )

    print(f"Uploaded: {BUCKET_NAME}/{object_path}")


# ----------------------------------
# MAIN LOOP
# ----------------------------------
try:
    while True:
        msg = consumer.poll(1.0)  # wait 1 sec

        if msg is None:
            continue

        if msg.error():
            raise KafkaException(msg.error())

        print(f"RAW MESSAGE: {msg.value()}")

        message_value = json.loads(msg.value().decode("utf-8"))
        topic = msg.topic()

        table_name = topic.split(".")[-1]
        record = process_debezium_message(message_value)

        if not record:
            continue

        buffers[table_name].append(record)
        print(f"{table_name}: {len(buffers[table_name])}")

        if len(buffers[table_name]) >= BATCH_SIZE:
            upload_batch(table_name, buffers[table_name])
            buffers[table_name] = []

except KeyboardInterrupt:
    print("Stopping consumer...")

finally:
    print("Flushing remaining records...")

    for table_name, records in buffers.items():
        if records:
            upload_batch(table_name, records)

    consumer.close()
    print("Consumer closed.")
    print("Current buffers:", {k: len(v) for k, v in buffers.items()})


