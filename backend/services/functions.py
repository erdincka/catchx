import logging
import os
import random
from functools import lru_cache

import pandas as pd

from config import (
    BASEDIR, MOUNT_PATH, VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD,
    TABLE_CUSTOMERS, TABLE_TRANSACTIONS, TABLE_PROFILES, TRANSACTION_CATEGORIES,
)
from store import ClusterConfig, get_cluster_name
from services import tables, iceberger
from services.mock import dummy_fraud_score

logger = logging.getLogger("functions")


def get_customer_id(config: ClusterConfig, from_account: str) -> str | None:
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


async def upsert_profile(config: ClusterConfig, transaction: dict):
    cluster_name = get_cluster_name(config)
    profile = {
        "_id": get_customer_id(config, transaction["receiver_account"]),
        "score": await dummy_fraud_score(),
    }
    if profile["_id"] is None:
        return
    table_path = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}"
    tables.upsert_document(config, table_path, profile)


async def refine_transactions(config: ClusterConfig) -> dict:
    cluster_name = get_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    tier = VOLUME_BRONZE
    tablename = TABLE_TRANSACTIONS
    silver_table = f"{BASEDIR}/{VOLUME_SILVER}/{tablename}"

    if not os.path.lexists(f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{tier}/{tablename}"):
        return {"status": "error", "message": f"Input table not found: {tablename} in {tier}"}

    docs = await tables.get_documents(config, f"{BASEDIR}/{tier}/{tablename}", limit=None)
    df = pd.DataFrame.from_dict(docs)
    if df.empty:
        return {"status": "error", "message": "No records found in bronze transactions"}

    df["category"] = df.apply(lambda _: random.choice(TRANSACTION_CATEGORIES), axis=1)

    if await tables.upsert_documents(config, silver_table, df.to_dict("records")):
        logger.info("Wrote %d records to %s", df.shape[0], silver_table)
        return {"status": "ok", "count": df.shape[0]}
    else:
        return {"status": "error", "message": f"Failed to save records in {silver_table}"}


async def refine_customers(config: ClusterConfig) -> dict:
    import country_converter as coco

    cluster_name = get_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    silver_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}"

    if not os.path.lexists(f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{VOLUME_BRONZE}/{TABLE_CUSTOMERS}"):
        return {"status": "error", "message": f"Input table not found: {TABLE_CUSTOMERS} in {VOLUME_BRONZE}"}

    cc = coco.CountryConverter()
    df = iceberger.find_all(cluster_name, VOLUME_BRONZE, TABLE_CUSTOMERS)

    if df.empty:
        return {"status": "error", "message": "No customers found in bronze tier"}

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

    if await tables.upsert_documents(config, silver_table, df.to_dict("records")):
        logger.info("Wrote %d customers to %s", df.shape[0], silver_table)
        return {"status": "ok", "count": df.shape[0]}
    else:
        return {"status": "error", "message": f"Failed to save records in {silver_table}"}


async def create_golden(config: ClusterConfig) -> dict:
    cluster_name = get_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    profile_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}"
    customers_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}"
    transactions_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}"

    for path in [profile_table, customers_table, transactions_table]:
        if not os.path.lexists(f"{MOUNT_PATH}/{cluster_name}{path}"):
            return {"status": "error", "message": f"Input table not found: {path}"}

    profiles_df = pd.DataFrame.from_dict(await tables.get_documents(config, profile_table, limit=None))
    customers_df = pd.DataFrame.from_dict(await tables.get_documents(config, customers_table, limit=None))
    transactions_df = pd.DataFrame.from_dict(await tables.get_documents(config, transactions_table, limit=None))

    if profiles_df.empty or customers_df.empty or transactions_df.empty:
        return {"status": "error", "message": "Not all silver tables are populated"}

    updated_customers = pd.merge(customers_df, profiles_df, on="_id", how="left").fillna({"score": 0})
    pii_cols = ["name", "birthdate", "current_location", "mail", "username", "address", "account_number", "ssn"]
    updated_customers.drop([c for c in pii_cols if c in updated_customers.columns], axis=1, inplace=True)
    updated_customers["score"] = pd.to_numeric(updated_customers["score"], errors="coerce").convert_dtypes()

    transactions_df.drop([c for c in ["sender_account", "receiver_account"] if c in transactions_df.columns], axis=1, inplace=True)
    transactions_df["fraud"] = False

    results = {}
    results["customers"] = await tables.delta_table_upsert(cluster_name, f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_CUSTOMERS}", updated_customers)
    results["transactions"] = await tables.delta_table_upsert(cluster_name, f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}", transactions_df)

    if all(results.values()):
        return {"status": "ok", "message": "Gold tier created"}
    else:
        failed = [k for k, v in results.items() if not v]
        return {"status": "error", "message": f"Failed to write: {failed}"}


async def fraud_detection(config: ClusterConfig) -> dict:
    cluster_name = get_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    input_table = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}"
    output_table = f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}"

    if not os.path.lexists(input_table):
        return {"status": "error", "message": f"No transactions in bronze tier"}

    fraud_count = 0
    non_fraud_count = 0

    records = await tables.get_documents(config, f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}", limit=None)

    for record in records:
        score = await dummy_fraud_score()
        if score > 85:
            possible_fraud = pd.DataFrame.from_dict([record])
            possible_fraud["fraud"] = True
            possible_fraud.drop(
                [c for c in ["sender_account", "receiver_account"] if c in possible_fraud.columns],
                axis=1, inplace=True,
            )
            if await tables.delta_table_upsert(cluster_name, output_table, possible_fraud):
                fraud_count += 1
        else:
            non_fraud_count += 1

    return {"status": "ok", "fraud_count": fraud_count, "non_fraud_count": non_fraud_count}


async def delete_volumes_and_streams(config: ClusterConfig) -> dict:
    import shutil
    import httpx
    from config import VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD, STREAM_INCOMING, STREAM_CHANGELOG

    cluster_name = get_cluster_name(config)
    auth = (config.user, config.password)
    messages = []

    for vol in [VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD]:
        URL = f"https://{config.host}:8443/rest/volume/remove?name={vol}"
        async with httpx.AsyncClient(verify=False) as client:
            try:
                response = await client.post(URL, auth=auth)
                res = response.json()
                if res["status"] == "OK":
                    messages.append(f"Volume '{vol}' deleted")
                else:
                    messages.append(f"{vol}: {res['errors'][0]['desc']}")
            except Exception as e:
                messages.append(f"Failed to delete {vol}: {e}")

    for stream in [STREAM_INCOMING, STREAM_CHANGELOG]:
        URL = f"https://{config.host}:8443/rest/stream/delete?path={BASEDIR}/{stream}"
        async with httpx.AsyncClient(verify=False) as client:
            try:
                response = await client.post(URL, auth=auth)
                res = response.json()
                if res["status"] == "OK":
                    messages.append(f"Stream '{stream}' deleted")
                else:
                    messages.append(f"Stream {stream}: {res['errors'][0]['desc']}")
            except Exception as e:
                messages.append(f"Failed to delete stream {stream}: {e}")

    if cluster_name:
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

    return {"status": "ok", "messages": messages}
