import asyncio
import csv
import json
import logging
import os
import random
import uuid

from faker import Faker
from fastapi import HTTPException

from config import BASEDIR, MOUNT_PATH, TABLE_CUSTOMERS, TABLE_TRANSACTIONS, TOPIC_TRANSACTIONS, STREAM_INCOMING
from store import ClusterConfig, get_cluster_name, ensure_cluster_name
from services import streams

logger = logging.getLogger("mock")

fake = Faker(["en_GB"])


def fake_customer() -> dict:
    profile = fake.profile()
    customer = {
        "_id": uuid.uuid4().hex,
        **profile,
        "account_number": fake.iban(),
        "county": fake.county(),
        "country_code": fake.current_country_code(),
    }
    del customer["website"]
    del customer["residence"]
    customer["address"] = customer["address"].replace("\n", " ").replace("\r", " ")
    return customer


def fake_transaction(sender: str, receiver: str) -> dict:
    return {
        "_id": uuid.uuid4().hex,
        "sender_account": sender,
        "receiver_account": receiver,
        "amount": round(fake.pyint(0, 10_000), 2),
        "transaction_date": fake.past_datetime(start_date="-12M").timestamp(),
    }


def _customers_path(cluster_name: str) -> str:
    return f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{TABLE_CUSTOMERS}.csv"


def _transactions_path(cluster_name: str) -> str:
    return f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{TABLE_TRANSACTIONS}.csv"


async def create_customers(config: ClusterConfig, count: int = 200) -> dict:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        raise HTTPException(status_code=400, detail="Cluster not connected — connect first")

    csvfile = _customers_path(cluster_name)
    customers = []

    if os.path.isfile(csvfile):
        with open(csvfile, "r", newline="") as f:
            customers += list(csv.DictReader(f))

    for _ in range(count):
        customers.append(fake_customer())

    try:
        with open(csvfile, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fake_customer().keys())
            writer.writeheader()
            writer.writerows(customers)
    except Exception as error:
        logger.warning("create_customers error: %s", error)
        return {"status": "error", "message": str(error)}

    logger.info("%d customers created at %s", count, csvfile)
    return {"status": "ok", "count": count, "total": len(customers), "filepath": csvfile}


async def create_transactions(config: ClusterConfig, count: int = 100) -> dict:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        raise HTTPException(status_code=400, detail="Cluster not connected — connect first")

    customers_path = _customers_path(cluster_name)
    if not os.path.isfile(customers_path):
        return {"status": "error", "message": "Create customers first"}

    with open(customers_path, "r", newline="") as f:
        customers = list(csv.DictReader(f))

    if not customers:
        return {"status": "error", "message": "No customers found"}

    transactions = []
    for _ in range(count):
        sender = customers[random.randrange(len(customers))]["account_number"]
        receiver = customers[random.randrange(len(customers))]["account_number"]
        transactions.append(fake_transaction(sender, receiver))

    filepath = _transactions_path(cluster_name) if count < 101 else _transactions_path(cluster_name).replace(".csv", "-bulk.csv")

    try:
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fake_transaction("X", "Y").keys())
            writer.writeheader()
            writer.writerows(transactions)
    except Exception as error:
        logger.warning("create_transactions error: %s", error)
        return {"status": "error", "message": str(error)}

    logger.info("%d transactions created at %s", count, filepath)
    return {"status": "ok", "count": count, "filepath": filepath}


async def get_new_transactions(config: ClusterConfig, count: int = 10) -> list:
    cluster_name = get_cluster_name(config)
    if not cluster_name:
        return []

    customers_path = _customers_path(cluster_name)
    if not os.path.isfile(customers_path):
        return []

    with open(customers_path, "r", newline="") as f:
        customers = list(csv.DictReader(f))

    transactions = []
    for _ in range(count):
        sender = customers[random.randrange(len(customers))]["account_number"]
        receiver = customers[random.randrange(len(customers))]["account_number"]
        transactions.append(fake_transaction(sender, receiver))
    return transactions


async def publish_transactions(config: ClusterConfig, count: int = 10) -> dict:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        raise HTTPException(status_code=400, detail="Cluster not connected — connect first")

    stream_path = f"{BASEDIR}/{STREAM_INCOMING}"
    full_stream = f"{MOUNT_PATH}/{cluster_name}{stream_path}"

    if not os.path.lexists(full_stream):
        return {"status": "error", "message": f"Stream not found: {stream_path}"}

    transactions = await get_new_transactions(config, count)
    published = 0

    for txn in transactions:
        if streams.produce(stream_path, TOPIC_TRANSACTIONS, json.dumps(txn)):
            published += 1
        else:
            logger.warning("Failed to send transaction %s", txn["_id"])

    return {"status": "ok", "count": published}


async def dummy_fraud_score() -> int:
    await asyncio.sleep(0.001)
    return random.randint(0, 100)


async def upload_to_s3(file: str, s3_server: str, access_key: str, secret_key: str, bucket: str) -> dict:
    from minio import Minio

    try:
        client = Minio(endpoint=s3_server, access_key=access_key, secret_key=secret_key, secure=False)

        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

        client.fput_object(bucket, os.path.basename(file), file)
        return {"status": "ok"}

    except Exception as error:
        logger.warning("S3 upload error: %s", error)
        return {"status": "error", "message": str(error)}


def read_customers_preview(config: ClusterConfig) -> list:
    cluster_name = get_cluster_name(config)
    if not cluster_name:
        return []
    path = _customers_path(cluster_name)
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))[:15]


def read_transactions_preview(config: ClusterConfig) -> list:
    cluster_name = get_cluster_name(config)
    if not cluster_name:
        return []
    path = _transactions_path(cluster_name)
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))[:15]
