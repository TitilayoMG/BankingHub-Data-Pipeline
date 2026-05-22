
import os
import json
import requests
from dotenv import load_dotenv

load_dotenv()

# --------------------------------
# ENV VARIABLES
# --------------------------------
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

DEBEZIUM_URL = "http://localhost:8083" #"http://debezium:8083" 

CONNECTOR_NAME = "banking-postgres-connector"

# --------------------------------
# CONNECTOR CONFIG
# --------------------------------
connector_config = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "plugin.name": "pgoutput",

        "database.hostname": POSTGRES_HOST,
        "database.port": POSTGRES_PORT,
        "database.user": POSTGRES_USER,
        "database.password": POSTGRES_PASSWORD,
        "database.dbname": POSTGRES_DB,

        # "database.server.name": "banking_server",

        # replication slot
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered", # Debezium auto-create PUBLICATION banking_publication with this

        # history
        "topic.prefix": "banking",
        # "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
        # "schema.history.internal.kafka.topic": "schema-changes.banking",

        # tables to capture
        "table.include.list": "public.transactions,public.account,public.customers",

        # snapshots existing data first time
        "snapshot.mode": "initial",

        "include.schema.changes": "false",

        # transforms
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double"
    }
}

# --------------------------------
# REGISTER CONNECTOR
# --------------------------------
url = f"{DEBEZIUM_URL}/connectors/{CONNECTOR_NAME}/config"

try:
    response = requests.put(
        url,
        headers={"Content-Type": "application/json"},
        data=json.dumps(connector_config["config"])
    )

    if response.status_code in [200, 201]:
        print("✅ Connector created successfully")
        print(response.json())

    else:
        print(f"❌ Failed to create connector")
        print(f"Status: {response.status_code}")
        print(response.text)

except Exception as e:
    print(f"Connection error: {e}")