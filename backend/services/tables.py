import logging
import os

from config import MOUNT_PATH
from store import ClusterConfig

logger = logging.getLogger("tables")

# OJAI connection singletons keyed by host
_connections: dict = {}


def get_connection(config: ClusterConfig):
    if config.host in _connections:
        return _connections[config.host]

    from mapr.ojai.storage.ConnectionFactory import ConnectionFactory

    connection_str = (
        f"{config.host}:5678?auth=basic;"
        f"user={config.user};"
        f"password={config.password};"
        "ssl=true;"
        "sslCA=/opt/mapr/conf/ssl_truststore.pem;"
        "sslTargetNameOverride=node1.mapr.com"
    )

    try:
        conn = ConnectionFactory.get_connection(connection_str=connection_str)
        _connections[config.host] = conn
        return conn
    except Exception as error:
        logger.warning("OJAI connection failed: %s", error)
        return None


def upsert_document(config: ClusterConfig, table_path: str, json_dict: dict) -> bool:
    conn = get_connection(config)
    if conn is None:
        return False
    try:
        store = conn.get_or_create_store(table_path)
        doc = conn.new_document(dictionary=json_dict)
        store.insert_or_replace(doc)
        return True
    except Exception as error:
        logger.warning("upsert_document error for %s: %s", table_path, error)
        return False


async def upsert_documents(config: ClusterConfig, table_path: str, docs: list) -> bool:
    success = True
    for doc in docs:
        if not upsert_document(config, table_path, doc):
            success = False
    return success


async def get_documents(config: ClusterConfig, table_path: str, limit: int | None = None) -> list:
    conn = get_connection(config)
    if conn is None:
        return []
    try:
        store = conn.get_or_create_store(table_path)
        query = conn.new_query()
        if limit:
            query = query.limit(limit)
        query = query.build()
        result = store.find(query)
        return [dict(doc) for doc in result]
    except Exception as error:
        logger.warning("get_documents error for %s: %s", table_path, error)
        return []


async def delta_table_get(cluster_name: str, table_path: str, query: str | None = None):
    import pandas as pd
    from deltalake import DeltaTable

    full_path = f"{MOUNT_PATH}/{cluster_name}{table_path}"
    if not os.path.lexists(full_path):
        logger.warning("Delta table not found: %s", full_path)
        return pd.DataFrame()

    try:
        dt = DeltaTable(full_path)
        df = dt.to_pandas()
        if query:
            df = df.query(query)
        return df
    except Exception as error:
        logger.warning("delta_table_get error for %s: %s", full_path, error)
        return pd.DataFrame()


async def delta_table_upsert(cluster_name: str, table_path: str, df) -> bool:
    import pyarrow as pa
    from deltalake import DeltaTable
    from deltalake.writer import write_deltalake

    full_path = f"{MOUNT_PATH}/{cluster_name}{table_path}"

    try:
        table = pa.Table.from_pandas(df)

        if os.path.lexists(full_path):
            dt = DeltaTable(full_path)
            dt.merge(
                source=table,
                predicate="s._id = t._id",
                source_alias="s",
                target_alias="t",
            ).when_matched_update_all().when_not_matched_insert_all().execute()
        else:
            write_deltalake(full_path, table)

        return True

    except Exception as error:
        logger.warning("delta_table_upsert error for %s: %s", full_path, error)
        return False
