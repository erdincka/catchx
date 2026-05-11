import csv
import json
import logging
import os

import pandas as pd

from config import (
    BASEDIR, MOUNT_PATH, VOLUME_BRONZE, TABLE_CUSTOMERS, TABLE_TRANSACTIONS,
    STREAM_INCOMING, TOPIC_TRANSACTIONS,
)
from store import ClusterConfig, ensure_cluster_name
from services import streams, tables, iceberger
from services.functions import upsert_profile

logger = logging.getLogger("ingestion")


async def ingest_transactions(config: ClusterConfig) -> dict:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    input_stream = f"{BASEDIR}/{STREAM_INCOMING}"
    output_table = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}"

    records_raw = streams.consume(stream=input_stream, topic=TOPIC_TRANSACTIONS, consumer_group="ingestion")
    transactions = []

    for record in records_raw:
        txn = json.loads(record)
        transactions.append(txn)
        await upsert_profile(config, txn)

    if not transactions:
        return {"status": "ok", "count": 0, "message": "No messages in stream"}

    if await tables.upsert_documents(config, output_table, transactions):
        logger.info("Saved %d transactions to %s", len(transactions), output_table)
        return {"status": "ok", "count": len(transactions)}
    else:
        return {"status": "error", "message": f"Failed to write to {output_table}"}


async def ingest_customers_iceberg(config: ClusterConfig) -> dict:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    csvpath = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{TABLE_CUSTOMERS}.csv"

    if not os.path.isfile(csvpath):
        return {"status": "error", "message": f"Customers file not found: {csvpath}"}

    try:
        with open(csvpath, "r", newline="") as f:
            customers = list(csv.DictReader(f))

        if iceberger.write(cluster_name=cluster_name, tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS, records=customers):
            logger.info("Ingested %d customers into Iceberg", len(customers))
            return {"status": "ok", "count": len(customers)}
        else:
            return {"status": "error", "message": "Failed to write to Iceberg table"}

    except Exception as error:
        logger.warning("ingest_customers_iceberg error: %s", error)
        return {"status": "error", "message": str(error)}
