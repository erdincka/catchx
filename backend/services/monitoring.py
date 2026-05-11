import datetime
import logging
import os
import timeit
from typing import Optional

import httpx

import settings as settings_module
from config import (
    BASEDIR, MOUNT_PATH, VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD,
    TABLE_CUSTOMERS, TABLE_TRANSACTIONS, TABLE_PROFILES, TABLE_FRAUD,
    STREAM_INCOMING, TOPIC_TRANSACTIONS, MONITORING_METRICS,
)
from store import ClusterConfig, ensure_cluster_name
from services import tables, iceberger

logger = logging.getLogger("monitoring")


async def incoming_topic_stats(config: ClusterConfig) -> Optional[dict]:
    stream_path = f"{BASEDIR}/{STREAM_INCOMING}"
    auth = (config.user, config.password)

    try:
        URL = f"https://{config.host}:8443/rest/stream/topic/info?path={stream_path}&topic={TOPIC_TRANSACTIONS}"
        async with httpx.AsyncClient(verify=settings_module.ssl_verify()) as client:
            response = await client.get(URL, auth=auth, timeout=2.0)

        if response.status_code != 200:
            return None

        metrics = response.json()
        if metrics.get("status") == "ERROR":
            return None

        series = []
        ingested = 0
        processed = 0
        for m in metrics["data"]:
            pub = m["maxoffset"] + 1
            con = m["minoffsetacrossconsumers"]
            series.append({"publishedMsgs": pub})
            series.append({"consumedMsgs": con})
            ingested = pub
            processed = con

        return {
            "name": "Incoming",
            "time": datetime.datetime.fromtimestamp(metrics["timestamp"] / 1000).strftime("%H:%M:%S"),
            "values": series,
            "transactions_ingested": ingested,
            "transactions_processed": processed,
        }

    except Exception as error:
        logger.warning("incoming_topic_stats error: %s", error)
        return None


async def txn_consumer_stats(config: ClusterConfig) -> Optional[dict]:
    stream_path = f"{BASEDIR}/{STREAM_INCOMING}"
    auth = (config.user, config.password)

    try:
        URL = f"https://{config.host}:8443/rest/stream/cursor/list?path={stream_path}&topic={TOPIC_TRANSACTIONS}"
        async with httpx.AsyncClient(verify=settings_module.ssl_verify()) as client:
            response = await client.get(URL, auth=auth, timeout=2.0)

        if response.status_code != 200:
            return None

        metrics = response.json()
        if metrics.get("status") == "ERROR":
            return None

        series = []
        for m in metrics["data"]:
            series.append({f"{m['consumergroup']}_{m['partitionid']}_lag(s)": float(m["consumerlagmillis"]) / 1000})
            series.append({f"{m['consumergroup']}_{m['partitionid']}_offsetBehind": int(m["produceroffset"]) + 1 - int(m["committedoffset"])})

        return {
            "name": "Consumers",
            "time": datetime.datetime.fromtimestamp(metrics["timestamp"] / 1000).strftime("%H:%M:%S"),
            "values": series,
        }

    except Exception as error:
        logger.warning("txn_consumer_stats error: %s", error)
        return None


async def bronze_stats(config: ClusterConfig) -> Optional[dict]:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return None

    series = []
    result = {"bronze_transactions": 0, "bronze_customers": 0}

    try:
        tick = timeit.default_timer()
        ttable = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}"

        if os.path.lexists(f"{MOUNT_PATH}/{cluster_name}{ttable}"):
            n = len(await tables.get_documents(config, ttable, limit=None))
            series.append({"transactions": n})
            result["bronze_transactions"] = n

        iceberg_df = iceberger.find_all(cluster_name, VOLUME_BRONZE, TABLE_CUSTOMERS)
        if not iceberg_df.empty:
            n = len(iceberg_df)
            series.append({"customers": n})
            result["bronze_customers"] = n

        logger.debug("Bronze stat time: %.3f", timeit.default_timer() - tick)

        if not series:
            return None

    except Exception as error:
        logger.warning("bronze_stats error: %s", error)
        return None

    return {
        "name": VOLUME_BRONZE,
        "time": datetime.datetime.now().strftime("%H:%M:%S"),
        "values": series,
        **result,
    }


async def silver_stats(config: ClusterConfig) -> Optional[dict]:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return None

    series = []
    result = {"silver_profiles": 0, "silver_transactions": 0, "silver_customers": 0}

    try:
        tick = timeit.default_timer()

        for key, tablename in [
            ("silver_profiles", TABLE_PROFILES),
            ("silver_transactions", TABLE_TRANSACTIONS),
            ("silver_customers", TABLE_CUSTOMERS),
        ]:
            path = f"{BASEDIR}/{VOLUME_SILVER}/{tablename}"
            if os.path.lexists(f"{MOUNT_PATH}/{cluster_name}{path}"):
                n = len(await tables.get_documents(config, path, limit=None))
                series.append({tablename: n})
                result[key] = n

        logger.debug("Silver stat time: %.3f", timeit.default_timer() - tick)

        if not series:
            return None

    except Exception as error:
        logger.warning("silver_stats error: %s", error)
        return None

    return {
        "name": VOLUME_SILVER,
        "time": datetime.datetime.now().strftime("%H:%M:%S"),
        "values": series,
        **result,
    }


async def gold_stats(config: ClusterConfig) -> Optional[dict]:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return None

    series = []
    result = {"gold_customers": 0, "gold_transactions": 0, "gold_fraud": 0}

    try:
        tick = timeit.default_timer()

        customers_df = await tables.delta_table_get(cluster_name, f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_CUSTOMERS}")
        transactions_df = await tables.delta_table_get(cluster_name, f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}")

        if not customers_df.empty:
            result["gold_customers"] = customers_df.shape[0]
            series.append({TABLE_CUSTOMERS: customers_df.shape[0]})

        if not transactions_df.empty:
            result["gold_transactions"] = transactions_df.shape[0]
            series.append({TABLE_TRANSACTIONS: transactions_df.shape[0]})
            fraud_df = transactions_df[transactions_df["fraud"] == True]
            result["gold_fraud"] = fraud_df.shape[0]
            series.append({TABLE_FRAUD: fraud_df.shape[0]})

        logger.debug("Gold stat time: %.3f", timeit.default_timer() - tick)

        if not series:
            return None

    except Exception as error:
        logger.warning("gold_stats error: %s", error)
        return None

    return {
        "name": VOLUME_GOLD,
        "time": datetime.datetime.now().strftime("%H:%M:%S"),
        "values": series,
        **result,
    }


async def collect_all_metrics(config: ClusterConfig) -> dict:
    import asyncio

    metrics = {m: 0 for m in MONITORING_METRICS}
    metrics["timestamp"] = datetime.datetime.now().strftime("%H:%M:%S")
    metrics["series"] = {}

    results = await asyncio.gather(
        incoming_topic_stats(config),
        txn_consumer_stats(config),
        bronze_stats(config),
        silver_stats(config),
        gold_stats(config),
        return_exceptions=True,
    )

    incoming, consumers, bronze, silver, gold = results

    if isinstance(incoming, dict):
        metrics["transactions_ingested"] = incoming.get("transactions_ingested", 0)
        metrics["transactions_processed"] = incoming.get("transactions_processed", 0)
        metrics["series"]["incoming"] = {"name": "Incoming", "time": incoming["time"], "values": incoming["values"]}

    if isinstance(consumers, dict):
        metrics["series"]["consumers"] = {"name": "Consumers", "time": consumers["time"], "values": consumers["values"]}

    if isinstance(bronze, dict):
        metrics["bronze_transactions"] = bronze.get("bronze_transactions", 0)
        metrics["bronze_customers"] = bronze.get("bronze_customers", 0)
        metrics["series"]["bronze"] = {"name": "bronze", "time": bronze["time"], "values": bronze["values"]}

    if isinstance(silver, dict):
        metrics["silver_profiles"] = silver.get("silver_profiles", 0)
        metrics["silver_transactions"] = silver.get("silver_transactions", 0)
        metrics["silver_customers"] = silver.get("silver_customers", 0)
        metrics["series"]["silver"] = {"name": "silver", "time": silver["time"], "values": silver["values"]}

    if isinstance(gold, dict):
        metrics["gold_customers"] = gold.get("gold_customers", 0)
        metrics["gold_transactions"] = gold.get("gold_transactions", 0)
        metrics["gold_fraud"] = gold.get("gold_fraud", 0)
        metrics["series"]["gold"] = {"name": "gold", "time": gold["time"], "values": gold["values"]}

    return metrics
