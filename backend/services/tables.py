"""HPE Data Fabric DocumentDB (OJAI) and Delta Lake I/O.

The OJAI client and deltalake are synchronous, blocking libraries. Every
public coroutine here therefore hands the blocking work to a worker thread via
`to_thread` so the event loop stays free — without that, a single
ingest or consolidate freezes the whole API, including the metrics poll that
drives the live counters in the UI.
"""

import asyncio
import ipaddress
import logging
import os
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

from config import MOUNT_PATH
from asyncutil import to_thread
from store import ClusterConfig

logger = logging.getLogger("tables")

# OJAI connection singletons keyed by host
_connections: dict = {}


OJAI_PORT = 5678
TRUSTSTORE_PEM = "/opt/mapr/conf/ssl_truststore.pem"

_target_name_cache: dict = {}


def _cert_common_name(host: str) -> Optional[str]:
    """Read the CN the cluster presents on the OJAI port.

    openssl is already in the image and this is a fixed, local command with no
    caller-supplied input.
    """
    try:
        proc = subprocess.run(
            ["openssl", "s_client", "-connect", f"{host}:{OJAI_PORT}"],
            input="", capture_output=True, text=True, timeout=10,
        )
        cert = subprocess.run(
            ["openssl", "x509", "-noout", "-subject"],
            input=proc.stdout, capture_output=True, text=True, timeout=10,
        )
        m = re.search(r"CN\s*=\s*([^\s,/]+)", cert.stdout)
        return m.group(1) if m else None
    except Exception as error:
        logger.debug("Could not read certificate CN from %s: %s", host, error)
        return None


def resolve_target_name(config: ClusterConfig) -> str:
    """TLS name to present to the OJAI endpoint.

    Data Fabric clusters typically use a wildcard certificate (CN=*.example.com). An
    IP address can never match one, so connecting by IP fails the gRPC hostname
    check unless we override the name. Order of preference:

      1. An explicit ojai_target_name in settings.
      2. The configured host, when it is already a hostname.
      3. The CN from the cluster's own certificate — with a wildcard turned
         into a concrete label, which is what the match actually needs.
      4. The host, so behaviour is unchanged when detection fails.
    """
    import settings as settings_module

    configured = settings_module.load().ojai_target_name.strip()
    if configured:
        return configured

    host = config.host
    if host in _target_name_cache:
        return _target_name_cache[host]

    try:
        ipaddress.ip_address(host)
        is_ip = True
    except ValueError:
        is_ip = False

    if not is_ip:
        _target_name_cache[host] = host
        return host

    resolved = host
    cn = _cert_common_name(host)
    if cn:
        # Any label satisfies a wildcard, so "mapr.example.com" matches
        # "*.example.com" — the point is to send a name, not an address.
        resolved = f"mapr.{cn[2:]}" if cn.startswith("*.") else cn
        logger.info("OJAI TLS name for %s resolved to %s (cert CN %s)", host, resolved, cn)
    else:
        logger.warning(
            "Could not determine the OJAI TLS name for %s; using the address. "
            "If DocumentDB fails to connect, set ojai_target_name in settings.",
            host,
        )

    _target_name_cache[host] = resolved
    return resolved


def get_connection(config: ClusterConfig):
    if config.host in _connections:
        return _connections[config.host]

    from mapr.ojai.storage.ConnectionFactory import ConnectionFactory

    target_name = resolve_target_name(config)
    connection_str = (
        f"{config.host}:{OJAI_PORT}?auth=basic;"
        f"user={config.user};"
        f"password={config.password};"
        "ssl=true;"
        f"sslCA={TRUSTSTORE_PEM};"
        f"sslTargetNameOverride={target_name}"
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


# Each OJAI write is a separate RPC costing roughly a round trip, so throughput
# is latency-bound rather than CPU-bound. The client's doc_stream argument does
# not actually batch on the wire (measured: no gain), but issuing writes
# concurrently does — about 5x on a LAN cluster. Past ~16 in-flight writes
# contention makes it worse again, so keep this modest.
WRITE_CONCURRENCY = 8


def _upsert_documents_blocking(config: ClusterConfig, table_path: str, docs: list) -> bool:
    """Write documents through one store handle, several requests in flight.

    The first row is written on its own before the pool starts. A table is
    created lazily on first write, and firing eight concurrent writes at a
    table that does not exist yet loses a batch of them to "DocumentStore not
    found" and err 19 — intermittently, and only on the first run after a
    reset, which is the worst way for a demo to fail. Warming the table with
    one serial write removes that race.

    Anything that still fails is retried once, serially, before the batch is
    reported as failed: a handful of transient errors should not discard an
    otherwise good write of several hundred rows.
    """
    if not docs:
        return True

    conn = get_connection(config)
    if conn is None:
        return False
    try:
        store = conn.get_or_create_store(table_path)
    except Exception as error:
        logger.warning("get_or_create_store failed for %s: %s", table_path, error)
        return False

    last_error: list = [None]

    def _write(row) -> bool:
        try:
            store.insert_or_replace(conn.new_document(dictionary=row))
            return True
        except Exception as error:
            last_error[0] = error
            logger.debug("upsert error for %s: %s", table_path, error)
            return False

    # Warm the table before going parallel; this write also forces creation.
    #
    # A volume that was only just provisioned needs a few seconds before
    # DocumentDB will serve it — until then both reads and writes fail with
    # err 19. Back off across roughly fifteen seconds rather than giving up,
    # so a reset followed immediately by a demo run just works.
    for delay in (0, 0.5, 1, 2, 4, 8):
        if delay:
            time.sleep(delay)
        if _write(docs[0]):
            break
    else:
        logger.warning("Could not write to %s: %s", table_path, last_error[0])
        return False

    rest = docs[1:]
    if not rest:
        return True

    failed: list = []
    workers = min(WRITE_CONCURRENCY, len(rest))
    if workers <= 1:
        failed = [r for r in rest if not _write(r)]
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for row, ok in zip(rest, pool.map(_write, rest)):
                if not ok:
                    failed.append(row)

    if failed:
        logger.info("Retrying %d/%d writes for %s", len(failed), len(docs), table_path)
        still_failed = [r for r in failed if not _write(r)]
        if still_failed:
            logger.warning(
                "%d/%d writes failed for %s after retry", len(still_failed), len(docs), table_path
            )
            return False

    return True


# Public alias: ingestion calls this directly from inside a worker thread so the
# stream commit can be sequenced after the write on that same thread.
upsert_documents_blocking = _upsert_documents_blocking


async def upsert_documents(config: ClusterConfig, table_path: str, docs: list) -> bool:
    return await to_thread(_upsert_documents_blocking, config, table_path, docs)


def _get_documents_blocking(config: ClusterConfig, table_path: str, limit: Optional[int]) -> list:
    """Read documents, retrying briefly on error.

    A just-provisioned volume serves err 19 for a few seconds. Without a retry
    the read returns empty and the caller reports "no records", which sends the
    presenter back to re-run a step that had in fact worked.
    """
    conn = get_connection(config)
    if conn is None:
        return []

    last_error = None
    for delay in (0, 0.5, 2):
        if delay:
            time.sleep(delay)
        try:
            store = conn.get_or_create_store(table_path)
            query = conn.new_query()
            if limit:
                query = query.limit(limit)
            return [dict(doc) for doc in store.find(query.build())]
        except Exception as error:
            last_error = error

    logger.warning("get_documents error for %s: %s", table_path, last_error)
    return []


async def get_documents(config: ClusterConfig, table_path: str, limit: Optional[int]) -> list:
    return await to_thread(_get_documents_blocking, config, table_path, limit)


def _count_documents_blocking(config: ClusterConfig, table_path: str) -> int:
    """Count rows by projecting only _id.

    Counting used to pull every field of every document on a 3-second timer;
    projecting a single column keeps the row count without the payload.
    """
    conn = get_connection(config)
    if conn is None:
        return 0
    try:
        store = conn.get_or_create_store(table_path)
        query = conn.new_query().select("_id").build()
        return sum(1 for _ in store.find(query))
    except Exception as error:
        logger.warning("count_documents error for %s: %s", table_path, error)
        return 0


async def count_documents(config: ClusterConfig, table_path: str) -> int:
    return await to_thread(_count_documents_blocking, config, table_path)


def _delta_get_blocking(cluster_name: str, table_path: str, query: Optional[str]):
    import pandas as pd
    from deltalake import DeltaTable

    full_path = f"{MOUNT_PATH}/{cluster_name}{table_path}"
    if not os.path.lexists(full_path):
        logger.warning("Delta table not found: %s", full_path)
        return pd.DataFrame()

    try:
        df = DeltaTable(full_path).to_pandas()
        return df.query(query) if query else df
    except Exception as error:
        logger.warning("delta_table_get error for %s: %s", full_path, error)
        return pd.DataFrame()


async def delta_table_get(cluster_name: str, table_path: str, query: Optional[str] = None):
    return await to_thread(_delta_get_blocking, cluster_name, table_path, query)


def _delta_upsert_blocking(cluster_name: str, table_path: str, df) -> bool:
    import pyarrow as pa
    from deltalake import DeltaTable
    from deltalake.writer import write_deltalake

    full_path = f"{MOUNT_PATH}/{cluster_name}{table_path}"

    try:
        table = pa.Table.from_pandas(df)

        if os.path.lexists(full_path):
            DeltaTable(full_path).merge(
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


async def delta_table_upsert(cluster_name: str, table_path: str, df) -> bool:
    return await to_thread(_delta_upsert_blocking, cluster_name, table_path, df)
