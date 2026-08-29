"""Bronze-tier ingestion: streaming (Kafka → DocumentDB) and batch (CSV → Iceberg)."""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os

from config import (
    BASEDIR, MOUNT_PATH, VOLUME_BRONZE, TABLE_CUSTOMERS, TABLE_TRANSACTIONS,
    STREAM_INCOMING, TOPIC_TRANSACTIONS,
)
from asyncutil import to_thread
from store import ClusterConfig, ensure_cluster_name
from services import streams, tables, iceberger

logger = logging.getLogger("ingestion")


async def ingest_transactions(config: ClusterConfig) -> dict:
    """Drain the incoming stream into bronze, then rebuild silver profiles.

    The stream read and the bronze write happen on one worker thread so the
    offsets are only committed once the records are safely stored — a failed
    write leaves the messages on the stream and the step can simply be retried.

    Customer risk profiles are built during refine, not here: they are a silver
    artefact and they need bronze customers, which this step cannot assume have
    been ingested yet.
    """
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    input_stream = f"{BASEDIR}/{STREAM_INCOMING}"
    output_table = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}"

    parsed: list = []

    def _sink(raw_records: list) -> bool:
        parsed.clear()
        for record in raw_records:
            try:
                parsed.append(json.loads(record))
            except json.JSONDecodeError:
                logger.warning("Skipping malformed stream record")
        if not parsed:
            return True
        return tables.upsert_documents_blocking(config, output_table, parsed)

    def _drain():
        return streams.consume_batch(input_stream, TOPIC_TRANSACTIONS, "ingestion", _sink)

    raw, stored = await to_thread(_drain)

    if not raw:
        return {
            "status": "ok",
            "count": 0,
            "message": "No new messages on the stream — publish transactions first.",
        }

    if not stored:
        return {
            "status": "error",
            "message": f"Could not write to {output_table}. The messages were left on "
                       "the stream, so this step can be retried.",
        }

    logger.info("Saved %d transactions to %s", len(parsed), output_table)
    return {"status": "ok", "count": len(parsed)}


def _read_customers_csv(csvpath: str) -> list[dict]:
    with open(csvpath, "r", newline="") as f:
        return list(csv.DictReader(f))


async def ingest_customers_iceberg(config: ClusterConfig) -> dict:
    """Batch-load the generated customers CSV into the bronze Iceberg table."""
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    csvpath = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{TABLE_CUSTOMERS}.csv"

    if not os.path.isfile(csvpath):
        return {
            "status": "error",
            "message": "Customers CSV not found — run Generate first.",
        }

    try:
        customers = await to_thread(_read_customers_csv, csvpath)
        wrote = await to_thread(
            iceberger.write,
            cluster_name, VOLUME_BRONZE, TABLE_CUSTOMERS, customers,
        )
        if wrote:
            logger.info("Ingested %d customers into Iceberg", len(customers))
            return {"status": "ok", "count": len(customers)}
        return {"status": "error", "message": "Failed to write to the Iceberg table"}

    except Exception as error:
        logger.warning("ingest_customers_iceberg error: %s", error)
        return {"status": "error", "message": str(error)}
