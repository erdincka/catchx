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
from asyncutil import to_thread
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

    # Always the same filename. This used to switch to "-bulk.csv" above 100
    # rows, but nothing ever read that file — so the preview, the S3 upload and
    # step-completion detection all silently missed larger generations.
    filepath = _transactions_path(cluster_name)

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


async def publish_transactions(config: ClusterConfig, count: int = 0) -> dict:
    """Publish the generated transactions.csv onto the incoming stream.

    Publishing the file that step 1 produced (rather than inventing fresh rows)
    is what lets the counts line up across the whole demo: generate 500, publish
    500, ingest 500.  `count` caps the batch; 0 means publish the whole file.
    """
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        raise HTTPException(status_code=400, detail="Cluster not connected — connect first")

    stream_path = f"{BASEDIR}/{STREAM_INCOMING}"
    if not os.path.lexists(f"{MOUNT_PATH}/{cluster_name}{stream_path}"):
        return {"status": "error", "message": "Stream not found — provision the demo artefacts first."}

    csvpath = _transactions_path(cluster_name)
    if not os.path.isfile(csvpath):
        return {"status": "error", "message": "transactions.csv not found — run Generate first."}

    def _read_and_publish() -> tuple:
        with open(csvpath, "r", newline="") as f:
            rows = list(csv.DictReader(f))
        if count and count > 0:
            rows = rows[:count]
        payload = [json.dumps(row) for row in rows]
        return len(rows), streams.produce_many(stream_path, TOPIC_TRANSACTIONS, payload)

    total, published = await to_thread(_read_and_publish)

    if published == 0 and total > 0:
        return {"status": "error", "message": "Could not publish to the stream"}

    logger.info("Published %d/%d transactions to %s", published, total, stream_path)
    return {"status": "ok", "count": published, "total": total}


async def dummy_fraud_score() -> int:
    await asyncio.sleep(0.001)
    return random.randint(0, 100)


async def upload_to_s3(file: str, s3_endpoint: str, access_key: str, secret_key: str, bucket: str) -> dict:
    """Put a file into the fabric's S3 object store.

    `s3_endpoint` may be a full URL; minio wants host:port plus a secure flag,
    so the scheme is split off here rather than assumed to be http.
    """
    from urllib.parse import urlparse
    from minio import Minio

    parsed = urlparse(s3_endpoint if "://" in s3_endpoint else f"//{s3_endpoint}")
    host_port = parsed.netloc or parsed.path
    secure = parsed.scheme == "https"

    try:
        client = Minio(endpoint=host_port, access_key=access_key, secret_key=secret_key, secure=secure)

        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

        client.fput_object(bucket, os.path.basename(file), file)
        return {"status": "ok"}

    except Exception as error:
        logger.warning("S3 upload error: %s", error)
        return {"status": "error", "message": str(error)}


def read_customers_preview(config: ClusterConfig, cluster_name: str) -> list:
    if not cluster_name:
        return []
    path = _customers_path(cluster_name)
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))[:15]


def read_transactions_preview(config: ClusterConfig, cluster_name: str) -> list:
    if not cluster_name:
        return []
    path = _transactions_path(cluster_name)
    if not os.path.isfile(path):
        return []
    with open(path, "r", newline="") as f:
        return list(csv.DictReader(f))[:15]
