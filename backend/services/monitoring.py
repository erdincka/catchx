"""Live pipeline telemetry, sourced entirely from the Data Fabric cluster itself.

Stream throughput and consumer lag come from the cluster REST API on :8443;
tier row counts come from DocumentDB / Iceberg / Delta directly. No Grafana,
OpenTSDB or Fluentd is involved — the fabric already exposes what we need.

Two costs are managed here:

  * Stream stats are cheap REST calls and run on every poll.
  * Tier counts touch real tables, so they are cached (COUNT_TTL) and the
    cache is invalidated explicitly when a pipeline step writes data. Without
    that, a 3-second UI poll re-counted every table continuously for the whole
    demo.
"""

import asyncio
import datetime
import logging
import os
import time
from typing import Optional

import httpx

import settings as settings_module
from config import (
    BASEDIR, MOUNT_PATH, VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD,
    TABLE_CUSTOMERS, TABLE_TRANSACTIONS, TABLE_PROFILES, TABLE_FRAUD,
    STREAM_INCOMING, TOPIC_TRANSACTIONS, MONITORING_METRICS,
)
from asyncutil import to_thread
from store import ClusterConfig, ensure_cluster_name
from services import tables, iceberger

logger = logging.getLogger("monitoring")

# How long a tier count stays fresh. The UI polls faster than this; an explicit
# invalidate() after each pipeline write keeps the numbers responsive anyway.
COUNT_TTL = 15.0

_counts_cache: dict = {}
_counts_at: float = 0.0
_counts_lock = asyncio.Lock()


def invalidate() -> None:
    """Force the next metrics poll to recount. Called after pipeline writes."""
    global _counts_at
    _counts_at = 0.0


# ── Stream telemetry (cluster REST API) ────────────────────────────────────────


async def _rest_get(config: ClusterConfig, path: str, params: dict) -> Optional[dict]:
    try:
        async with httpx.AsyncClient(verify=settings_module.ssl_verify(), timeout=3.0) as client:
            r = await client.get(
                f"https://{config.host}:8443/rest/{path}",
                auth=(config.user, config.password),
                params=params,
            )
        if r.status_code != 200:
            return None
        data = r.json()
        return None if data.get("status") == "ERROR" else data
    except Exception as error:
        logger.debug("REST %s failed: %s", path, error)
        return None


async def incoming_topic_stats(config: ClusterConfig) -> Optional[dict]:
    """Published vs consumed offsets for the incoming transactions topic."""
    metrics = await _rest_get(
        config, "stream/topic/info",
        {"path": f"{BASEDIR}/{STREAM_INCOMING}", "topic": TOPIC_TRANSACTIONS},
    )
    if not metrics or not metrics.get("data"):
        return None

    series, ingested, processed = [], 0, 0
    for m in metrics["data"]:
        pub = m["maxoffset"] + 1
        con = m["minoffsetacrossconsumers"]
        series.append({"publishedMsgs": pub})
        series.append({"consumedMsgs": con})
        ingested, processed = pub, con

    return {
        "name": "Incoming",
        "time": datetime.datetime.fromtimestamp(metrics["timestamp"] / 1000).strftime("%H:%M:%S"),
        "values": series,
        "transactions_ingested": ingested,
        "transactions_processed": processed,
    }


async def txn_consumer_stats(config: ClusterConfig) -> Optional[dict]:
    """Per-consumer-group lag — the number that makes streaming feel real."""
    metrics = await _rest_get(
        config, "stream/cursor/list",
        {"path": f"{BASEDIR}/{STREAM_INCOMING}", "topic": TOPIC_TRANSACTIONS},
    )
    if not metrics or not metrics.get("data"):
        return None

    series = []
    for m in metrics["data"]:
        tag = f"{m['consumergroup']}_{m['partitionid']}"
        series.append({f"{tag}_lag(s)": float(m["consumerlagmillis"]) / 1000})
        series.append({f"{tag}_offsetBehind": int(m["produceroffset"]) + 1 - int(m["committedoffset"])})

    return {
        "name": "Consumers",
        "time": datetime.datetime.fromtimestamp(metrics["timestamp"] / 1000).strftime("%H:%M:%S"),
        "values": series,
    }


# ── Tier row counts ────────────────────────────────────────────────────────────


def _exists(cluster_name: str, path: str) -> bool:
    return os.path.lexists(f"{MOUNT_PATH}/{cluster_name}{path}")


async def _count_table(config: ClusterConfig, cluster_name: str, path: str) -> int:
    if not _exists(cluster_name, path):
        return 0
    return await tables.count_documents(config, path)


async def _collect_counts(config: ClusterConfig, cluster_name: str) -> dict:
    """One pass over every tier. Runs at most once per COUNT_TTL."""
    # Counted one after another, not with asyncio.gather: opening and querying
    # several different stores concurrently on a single OJAI connection is not
    # safe — one of the four would intermittently come back empty, so a tier
    # that clearly held data displayed as "—". They are cached anyway, so the
    # sequential cost is paid once per TTL rather than per poll.
    bronze_txn = await _count_table(config, cluster_name, f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}")
    silver_profiles = await _count_table(config, cluster_name, f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}")
    silver_txn = await _count_table(config, cluster_name, f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}")
    silver_cust = await _count_table(config, cluster_name, f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}")

    bronze_cust = len(await to_thread(
        iceberger.find_all, cluster_name, VOLUME_BRONZE, TABLE_CUSTOMERS
    ))

    # Delta reads go through the filesystem, not OJAI, so these are safe to
    # overlap.
    gold_cust_df, gold_txn_df = await asyncio.gather(
        tables.delta_table_get(cluster_name, f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_CUSTOMERS}"),
        tables.delta_table_get(cluster_name, f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}"),
    )

    gold_fraud = 0
    if not gold_txn_df.empty and "fraud" in gold_txn_df.columns:
        gold_fraud = int(gold_txn_df[gold_txn_df["fraud"] == True].shape[0])  # noqa: E712

    # Whether the generated source CSVs exist yet. The UI derives "step 1 done"
    # from real state rather than from whether a button was clicked, so progress
    # survives a page reload and reflects the cluster, not the session.
    base = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}"
    source_customers = os.path.isfile(f"{base}/{TABLE_CUSTOMERS}.csv")
    source_transactions = os.path.isfile(f"{base}/{TABLE_TRANSACTIONS}.csv")

    return {
        "source_customers": source_customers,
        "source_transactions": source_transactions,
        "bronze_transactions": bronze_txn,
        "bronze_customers": bronze_cust,
        "silver_profiles": silver_profiles,
        "silver_transactions": silver_txn,
        "silver_customers": silver_cust,
        "gold_customers": 0 if gold_cust_df.empty else int(gold_cust_df.shape[0]),
        "gold_transactions": 0 if gold_txn_df.empty else int(gold_txn_df.shape[0]),
        "gold_fraud": gold_fraud,
    }


async def tier_counts(config: ClusterConfig) -> dict:
    """Cached tier counts. Recounts on TTL expiry or after invalidate()."""
    global _counts_cache, _counts_at

    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {}

    async with _counts_lock:
        if _counts_cache and (time.monotonic() - _counts_at) < COUNT_TTL:
            return _counts_cache
        try:
            _counts_cache = await _collect_counts(config, cluster_name)
            _counts_at = time.monotonic()
        except Exception as error:
            logger.warning("tier_counts error: %s", error)
        return _counts_cache


# ── Public entry point ─────────────────────────────────────────────────────────


async def collect_all_metrics(config: ClusterConfig) -> dict:
    metrics: dict = {m: 0 for m in MONITORING_METRICS}
    metrics["timestamp"] = datetime.datetime.now().strftime("%H:%M:%S")
    metrics["series"] = {}

    incoming, consumers, counts = await asyncio.gather(
        incoming_topic_stats(config),
        txn_consumer_stats(config),
        tier_counts(config),
        return_exceptions=True,
    )

    if isinstance(incoming, dict):
        metrics["transactions_ingested"] = incoming.get("transactions_ingested", 0)
        metrics["transactions_processed"] = incoming.get("transactions_processed", 0)
        metrics["series"]["incoming"] = {
            "name": "Incoming", "time": incoming["time"], "values": incoming["values"],
        }

    if isinstance(consumers, dict):
        metrics["series"]["consumers"] = {
            "name": "Consumers", "time": consumers["time"], "values": consumers["values"],
        }

    if isinstance(counts, dict):
        metrics.update(counts)
        now = metrics["timestamp"]
        metrics["series"]["bronze"] = {"name": "bronze", "time": now, "values": [
            {TABLE_TRANSACTIONS: counts.get("bronze_transactions", 0)},
            {TABLE_CUSTOMERS: counts.get("bronze_customers", 0)},
        ]}
        metrics["series"]["silver"] = {"name": "silver", "time": now, "values": [
            {TABLE_PROFILES: counts.get("silver_profiles", 0)},
            {TABLE_TRANSACTIONS: counts.get("silver_transactions", 0)},
            {TABLE_CUSTOMERS: counts.get("silver_customers", 0)},
        ]}
        metrics["series"]["gold"] = {"name": "gold", "time": now, "values": [
            {TABLE_CUSTOMERS: counts.get("gold_customers", 0)},
            {TABLE_TRANSACTIONS: counts.get("gold_transactions", 0)},
            {TABLE_FRAUD: counts.get("gold_fraud", 0)},
        ]}

    return metrics
