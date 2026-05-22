

import random
import time
from decimal import Decimal
from faker import Faker
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# -----------------------------
# CONFIG
# -----------------------------
NUM_CUSTOMERS = 10
ACCOUNTS_PER_CUSTOMER = 2
NUM_TRANSACTIONS = 50
MAX_TRANSACTION_AMOUNT = Decimal("1000.00")
CURRENCY = "USD"
INITIAL_BALANCE_MIN = Decimal("10.00")
INITIAL_BALANCE_MAX = Decimal("1000.00")
SLEEP_SECONDS = 4


fake = Faker()

ACCOUNT_TYPES = ["current", "savings"]
TRANSACTION_TYPES = ["deposit", "withdrawal", "transfer"]


# -----------------------------
# DB CONNECTION
# -----------------------------
def get_connection():
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host = os.getenv("POSTGRES_HOST_LOCAL"),
        database = os.getenv("POSTGRES_DB"),
        user= os.getenv("POSTGRES_USER"),
        password= os.getenv("POSTGRES_PASSWORD"),
        port = os.getenv("POSTGRES_PORT")
    )

    conn.autocommit = False
    print("Connected successfully.\n")
    return conn


# -----------------------------
# INSERT CUSTOMERS
# -----------------------------
def generate_customers(cursor):
    customer_ids = []

    print(f"Generating {NUM_CUSTOMERS} customers...")

    for i in range(NUM_CUSTOMERS):
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = fake.email()

        cursor.execute("""
            INSERT INTO customers (first_name, last_name, email)
            VALUES (%s, %s, %s)
            RETURNING customer_id;
        """, (first_name, last_name, email))

        customer_id = cursor.fetchone()[0]
        customer_ids.append(customer_id)

        # print(f"Inserted customer {customer_id}: {first_name} {last_name}")

    print("Customers generation completed.\n")
    return customer_ids


# -----------------------------
# INSERT ACCOUNTS
# -----------------------------
def generate_accounts(cursor, customer_ids):
    account_data = {}

    print("Generating accounts...")

    for customer_id in customer_ids:
        account_data[customer_id] = []

        for account_type in ACCOUNT_TYPES:
            balance = round(
                random.uniform(
                    float(INITIAL_BALANCE_MIN),
                    float(INITIAL_BALANCE_MAX)
                ),
                2
            )

            cursor.execute("""
                INSERT INTO account (customer_id, account_type, balance, currency)
                VALUES (%s, %s, %s, %s)
                RETURNING account_id;
            """, (customer_id, account_type, balance, CURRENCY))

            account_id = cursor.fetchone()[0]

            account_data[customer_id].append({
                "account_id": account_id,
                "balance": Decimal(str(balance)),
                "account_type": account_type
            })

    print("Accounts generation completed.\n")
    return account_data


# -----------------------------
# UPDATE BALANCE
# -----------------------------
def update_balance(cursor, account_id, new_balance):
    cursor.execute("""
        UPDATE account
        SET balance = %s
        WHERE account_id = %s;
    """, (new_balance, account_id))


# -----------------------------
# INSERT TRANSACTIONS
# -----------------------------
def generate_transactions(cursor, conn, account_data):
    all_accounts = []
    for accounts in account_data.values():
        all_accounts.extend(accounts)

    batch_number = 1

    while True:
        print(f"\nStarting batch {batch_number}...")
        successful_transactions = 0

        while successful_transactions < NUM_TRANSACTIONS:
            txn_type = random.choice(TRANSACTION_TYPES)
            source_account = random.choice(all_accounts)

            amount = Decimal(
                str(round(random.uniform(1, float(MAX_TRANSACTION_AMOUNT)), 2))
            )

            related_account_id = None

            # -------------------
            # DEPOSIT
            # -------------------
            if txn_type == "deposit":
                source_account["balance"] += amount
                update_balance(
                    cursor,
                    source_account["account_id"],
                    source_account["balance"]
                )

            # -------------------
            # WITHDRAWAL
            # -------------------
            elif txn_type == "withdrawal":
                if source_account["balance"] < amount:
                    continue

                source_account["balance"] -= amount
                update_balance(
                    cursor,
                    source_account["account_id"],
                    source_account["balance"]
                )

            # -------------------
            # TRANSFER
            # -------------------
            else:
                if source_account["balance"] < amount:
                    continue

                target_account = random.choice(
                    [
                        acc for acc in all_accounts
                        if acc["account_id"] != source_account["account_id"]
                    ]
                )

                source_account["balance"] -= amount
                target_account["balance"] += amount
                related_account_id = target_account["account_id"]

                update_balance(
                    cursor,
                    source_account["account_id"],
                    source_account["balance"]
                )
                update_balance(
                    cursor,
                    target_account["account_id"],
                    target_account["balance"]
                )

            cursor.execute("""
                INSERT INTO transactions
                (account_id, run_type, amount, related_account_id, status)
                VALUES (%s, %s, %s, %s, %s);
            """, (
                source_account["account_id"],
                txn_type,
                amount,
                related_account_id,
                "COMPLETED"
            ))

            successful_transactions += 1

            # print(
            #     f"Batch {batch_number} | "
            #     f"Transaction {successful_transactions}/{NUM_TRANSACTIONS} | "
            #     f"{txn_type.upper()} | "
            #     f"Account {source_account['account_id']} | "
            #     f"Amount {amount}"
            # )

        # commit after each batch
        conn.commit()
        print(
            f"\nBatch {batch_number} completed. "
            f"{NUM_TRANSACTIONS} transactions inserted successfully."
        )

        print(f"Sleeping {SLEEP_SECONDS} seconds before next batch...\n")
        time.sleep(SLEEP_SECONDS)

        batch_number += 1

# -----------------------------
# MAIN
# -----------------------------
def main():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        customers = generate_customers(cursor)
        accounts = generate_accounts(cursor, customers)
        generate_transactions(cursor, conn, accounts)

        conn.commit()
        print("All data inserted successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error occurred: {e}")

    finally:
        cursor.close()
        conn.close()
        print("Database connection closed.")


if __name__ == "__main__":
    main()