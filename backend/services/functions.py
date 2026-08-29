"""Pipeline business logic: bronze → silver refinement, gold consolidation, fraud scoring.

Blocking libraries (pyiceberg, pandas, deltalake) are pushed onto worker threads
via `to_thread`; see services/tables.py for why that matters.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
from functools import lru_cache
from typing import Optional

import pandas as pd

from config import (
    BASEDIR, MOUNT_PATH, VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD,
    TABLE_CUSTOMERS, TABLE_TRANSACTIONS, TABLE_PROFILES, TRANSACTION_CATEGORIES,
    FRAUD_SCORE_THRESHOLD,
)
from asyncutil import to_thread
from store import ClusterConfig, get_cluster_name, ensure_cluster_name
from services import tables, iceberger

logger = logging.getLogger("functions")


def get_customer_id(config: ClusterConfig, from_account: str) -> Optional[str]:
    """Look up a single customer id by account number (one Iceberg scan per call).

    Kept for reference and ad-hoc use — the ingestion path uses
    `build_account_index` instead, which resolves every account in one scan.
    """
    cluster_name = get_cluster_name(config)
    found = iceberger.find_by_field(
        cluster_name=cluster_name,
        tier=VOLUME_BRONZE,
        tablename=TABLE_CUSTOMERS,
        field="account_number",
        value=from_account,
    )
    if found is not None and len(found) > 0:
        return found[0][0].as_py()
    return None


def build_account_index(cluster_name: str) -> dict[str, str]:
    """Map account_number → customer _id with a single scan of bronze customers.

    Scanning once and indexing in memory replaces what was one full Iceberg
    table scan per transaction — the difference between a demo that streams
    and one that appears to hang.
    """
    df = iceberger.find_all(cluster_name, VOLUME_BRONZE, TABLE_CUSTOMERS)
    if df.empty or "account_number" not in df.columns:
        return {}
    id_col = "_id" if "_id" in df.columns else "id"
    if id_col not in df.columns:
        return {}
    return {
        str(acct): str(cid)
        for acct, cid in zip(df["account_number"], df[id_col])
        if acct is not None and cid is not None
    }


async def upsert_profiles(config: ClusterConfig, transactions: list[dict]) -> int:
    """Score each transaction's receiver and write all profiles in one batch."""
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return 0

    index = await to_thread(build_account_index, cluster_name)
    if not index:
        return 0

    profiles: dict[str, dict] = {}
    for txn in transactions:
        customer_id = index.get(str(txn.get("receiver_account")))
        if customer_id is None:
            continue
        # Last write wins, matching the previous per-record upsert behaviour.
        profiles[customer_id] = {"_id": customer_id, "score": random.randint(0, 100)}

    if not profiles:
        return 0

    table_path = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}"
    await tables.upsert_documents(config, table_path, list(profiles.values()))
    return len(profiles)


async def refine_transactions(config: ClusterConfig) -> dict:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    tier = VOLUME_BRONZE
    tablename = TABLE_TRANSACTIONS
    silver_table = f"{BASEDIR}/{VOLUME_SILVER}/{tablename}"

    docs = await tables.get_documents(config, f"{BASEDIR}/{tier}/{tablename}", limit=None)
    df = pd.DataFrame.from_dict(docs)
    if df.empty:
        return {"status": "error", "message": "No transactions in the bronze tier — run Ingest first."}

    df["category"] = [random.choice(TRANSACTION_CATEGORIES) for _ in range(len(df))]

    if not await tables.upsert_documents(config, silver_table, df.to_dict("records")):
        return {"status": "error", "message": f"Failed to save records in {silver_table}"}

    # Profiles are silver too, and building them here means bronze customers
    # are already present to resolve accounts against.
    profiles = await upsert_profiles(config, docs)
    logger.info("Wrote %d records to %s (%d profiles)", df.shape[0], silver_table, profiles)
    return {"status": "ok", "count": int(df.shape[0]), "profiles": profiles}


def _enrich_customers_blocking(cluster_name: str) -> pd.DataFrame:
    """Country/subdivision enrichment and PII masking — pure CPU + Iceberg scan."""
    import country_converter as coco

    df = iceberger.find_all(cluster_name, VOLUME_BRONZE, TABLE_CUSTOMERS)
    if df.empty:
        return df

    cc = coco.CountryConverter()
    df.drop_duplicates(subset="_id", keep="last", ignore_index=True, inplace=True)
    df["country"] = cc.pandas_convert(df["country_code"], src="ISO2", to="name_short")

    @lru_cache()
    def to_iso3166_2(c):
        try:
            import pycountry
            subdiv = pycountry.subdivisions.search_fuzzy(str(c))
            return subdiv[0].code if subdiv else ""
        except Exception:
            return ""

    df["iso3166_2"] = df["county"].map(to_iso3166_2)
    df["birthdate"] = "*" * 8
    df["current_location"] = "*" * 4
    df.rename(columns={"id": "_id"}, inplace=True)
    return df


async def refine_customers(config: ClusterConfig) -> dict:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    silver_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}"
    df = await to_thread(_enrich_customers_blocking, cluster_name)

    if df.empty:
        return {"status": "error", "message": "No customers in the bronze tier — run Ingest first."}

    if await tables.upsert_documents(config, silver_table, df.to_dict("records")):
        logger.info("Wrote %d customers to %s", df.shape[0], silver_table)
        return {"status": "ok", "count": int(df.shape[0])}
    return {"status": "error", "message": f"Failed to save records in {silver_table}"}


async def create_golden(config: ClusterConfig) -> dict:
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    profile_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}"
    customers_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}"
    transactions_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}"

    # Read the tables rather than stat-ing them first. Table creation is visible
    # over NFS a moment before the directory entry is, so a stat immediately
    # after Refine could report "missing" for a table that had just been written.
    profiles_df = pd.DataFrame.from_dict(await tables.get_documents(config, profile_table, limit=None))
    customers_df = pd.DataFrame.from_dict(await tables.get_documents(config, customers_table, limit=None))
    transactions_df = pd.DataFrame.from_dict(await tables.get_documents(config, transactions_table, limit=None))

    missing = [
        name for name, df in (
            ("profiles", profiles_df),
            ("customers", customers_df),
            ("transactions", transactions_df),
        ) if df.empty
    ]
    if missing:
        return {
            "status": "error",
            "message": f"Silver tier has no {', '.join(missing)} yet — run Refine first.",
        }

    updated_customers = pd.merge(customers_df, profiles_df, on="_id", how="left").fillna({"score": 0})
    pii_cols = ["name", "birthdate", "current_location", "mail", "username", "address", "account_number", "ssn"]
    updated_customers.drop([c for c in pii_cols if c in updated_customers.columns], axis=1, inplace=True)
    updated_customers["score"] = pd.to_numeric(updated_customers["score"], errors="coerce").convert_dtypes()

    transactions_df.drop(
        [c for c in ["sender_account", "receiver_account"] if c in transactions_df.columns],
        axis=1, inplace=True,
    )
    transactions_df["fraud"] = False

    results = {
        "customers": await tables.delta_table_upsert(
            cluster_name, f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_CUSTOMERS}", updated_customers
        ),
        "transactions": await tables.delta_table_upsert(
            cluster_name, f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}", transactions_df
        ),
    }

    if all(results.values()):
        return {
            "status": "ok",
            "message": "Gold tier created",
            "customers": int(updated_customers.shape[0]),
            "transactions": int(transactions_df.shape[0]),
        }
    failed = [k for k, v in results.items() if not v]
    return {"status": "error", "message": f"Failed to write: {failed}"}


async def fraud_detection(config: ClusterConfig) -> dict:
    """Score bronze transactions and write every flagged row in a single merge.

    The scored rows are batched into one Delta upsert; writing them one at a
    time meant a full Delta merge per flagged transaction.
    """
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    output_table = f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}"

    records = await tables.get_documents(
        config, f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}", limit=None
    )
    if not records:
        return {"status": "error", "message": "No transactions in the bronze tier — run Ingest first."}

    flagged = [r for r in records if random.randint(0, 100) > FRAUD_SCORE_THRESHOLD]
    fraud_count = len(flagged)
    non_fraud_count = len(records) - fraud_count

    if flagged:
        df = pd.DataFrame.from_dict(flagged)
        df["fraud"] = True
        df.drop(
            [c for c in ["sender_account", "receiver_account"] if c in df.columns],
            axis=1, inplace=True,
        )
        if not await tables.delta_table_upsert(cluster_name, output_table, df):
            return {"status": "error", "message": f"Failed to write flagged rows to {output_table}"}

    return {
        "status": "ok",
        "fraud_count": fraud_count,
        "non_fraud_count": non_fraud_count,
        "scanned": len(records),
    }


def _purge_local_artefacts(cluster_name: str) -> list[str]:
    import shutil

    messages: list[str] = []
    basedir = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}"
    try:
        if os.path.exists(f"{basedir}/iceberg.db"):
            catalog = iceberger.get_catalog(cluster_name)
            for tier in [VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD]:
                if (tier,) in catalog.list_namespaces():
                    for tbl in catalog.list_tables(tier):
                        try:
                            catalog.purge_table(tbl)
                        except Exception:
                            pass
                try:
                    catalog.drop_namespace(tier)
                except Exception:
                    pass
            os.unlink(f"{basedir}/iceberg.db")
            messages.append("Iceberg tables purged")

        if os.path.isdir(basedir):
            shutil.rmtree(basedir, ignore_errors=True)
            messages.append(f"{basedir} removed")
    except Exception as e:
        messages.append(f"Cleanup error: {e}")
    return messages


async def delete_volumes_and_streams(config: ClusterConfig) -> dict:
    import httpx
    import settings as settings_module
    from config import (
        STREAM_INCOMING, STREAM_CHANGELOG,
        CATCHX_VOL_PARENT, CATCHX_VOL_BRONZE, CATCHX_VOL_SILVER, CATCHX_VOL_GOLD,
    )

    cluster_name = get_cluster_name(config)
    auth = (config.user, config.password)
    messages: list[str] = []

    # Volume *names* on the cluster, not the mount-path suffixes — cleanup used
    # to ask for "bronze" while the volume is called "catchx-bronze", so every
    # removal failed with "No such file or directory" and the demo could never
    # be fully reset. Children first: the parent cannot go while they are under it.
    volume_names = [CATCHX_VOL_BRONZE, CATCHX_VOL_SILVER, CATCHX_VOL_GOLD, CATCHX_VOL_PARENT]

    async with httpx.AsyncClient(verify=settings_module.ssl_verify(), timeout=30) as client:
        for stream in [STREAM_INCOMING, STREAM_CHANGELOG]:
            try:
                response = await client.post(
                    f"https://{config.host}:8443/rest/stream/delete",
                    auth=auth, params={"path": f"{BASEDIR}/{stream}"},
                )
                res = response.json()
                if res.get("status") == "OK":
                    messages.append(f"Stream '{stream}' deleted")
                else:
                    messages.append(f"Stream {stream}: {res.get('errors', [{}])[0].get('desc', 'unknown error')}")
            except Exception as e:
                messages.append(f"Failed to delete stream {stream}: {e}")

        for vol in volume_names:
            try:
                response = await client.post(
                    f"https://{config.host}:8443/rest/volume/remove", auth=auth, params={"name": vol}
                )
                res = response.json()
                if res.get("status") == "OK":
                    messages.append(f"Volume '{vol}' deleted")
                else:
                    desc = res.get("errors", [{}])[0].get("desc", "unknown error")
                    # A missing volume is a fine outcome for a cleanup.
                    if "no such" in desc.lower() or "not found" in desc.lower():
                        messages.append(f"Volume '{vol}' was not present")
                    else:
                        messages.append(f"{vol}: {desc}")
            except Exception as e:
                messages.append(f"Failed to delete {vol}: {e}")

    if cluster_name:
        messages += await to_thread(_purge_local_artefacts, cluster_name)

    return {"status": "ok", "messages": messages}
