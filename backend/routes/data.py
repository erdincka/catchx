"""Pipeline data routes: generate, publish, ingest, refine, consolidate, detect.

Every route that writes invalidates the metrics count cache, so the live
counters in the UI update on the next poll instead of waiting out the TTL.
"""

import asyncio
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Query

import settings as settings_module
from config import (
    BASEDIR, MOUNT_PATH, VOLUME_BRONZE, VOLUME_GOLD,
    TABLE_CUSTOMERS, TABLE_TRANSACTIONS, FETCH_RECORD_NUM,
)
from asyncutil import to_thread
from store import ClusterConfig, get_cluster_config, ensure_cluster_name
from services import tables, iceberger, monitoring
from services import mock as mock_svc
from services import functions, ingestion

logger = logging.getLogger("routes.data")

router = APIRouter()


def _touch(result: dict) -> dict:
    """Mark tier counts stale after a write so the UI reflects it immediately."""
    monitoring.invalidate()
    return result


# ── Source data ────────────────────────────────────────────────────────────────


@router.post("/customers/create")
async def create_customers(
    count: int = Query(default=200, ge=1, le=10_000),
    config: ClusterConfig = Depends(get_cluster_config),
):
    return _touch(await mock_svc.create_customers(config, count))


@router.post("/transactions/create")
async def create_transactions(
    count: int = Query(default=100, ge=1, le=10_000),
    config: ClusterConfig = Depends(get_cluster_config),
):
    return _touch(await mock_svc.create_transactions(config, count))


@router.post("/transactions/publish")
async def publish_transactions(
    # 0 publishes the whole generated file, which is what makes the counts
    # line up across the demo. A positive value caps the batch.
    count: int = Query(default=0, ge=0, le=100_000),
    config: ClusterConfig = Depends(get_cluster_config),
):
    return _touch(await mock_svc.publish_transactions(config, count))


@router.get("/customers/preview")
async def preview_customers(config: ClusterConfig = Depends(get_cluster_config)):
    cluster_name = await ensure_cluster_name(config)
    records = await to_thread(mock_svc.read_customers_preview, config, cluster_name)
    return {"records": records, "count": len(records)}


@router.get("/transactions/preview")
async def preview_transactions(config: ClusterConfig = Depends(get_cluster_config)):
    cluster_name = await ensure_cluster_name(config)
    records = await to_thread(mock_svc.read_transactions_preview, config, cluster_name)
    return {"records": records, "count": len(records)}


# ── Pipeline stages ────────────────────────────────────────────────────────────


@router.post("/ingest/customers")
async def ingest_customers(config: ClusterConfig = Depends(get_cluster_config)):
    return _touch(await ingestion.ingest_customers_iceberg(config))


@router.post("/ingest/transactions")
async def ingest_transactions(config: ClusterConfig = Depends(get_cluster_config)):
    return _touch(await ingestion.ingest_transactions(config))


@router.post("/refine/customers")
async def refine_customers(config: ClusterConfig = Depends(get_cluster_config)):
    return _touch(await functions.refine_customers(config))


@router.post("/refine/transactions")
async def refine_transactions(config: ClusterConfig = Depends(get_cluster_config)):
    return _touch(await functions.refine_transactions(config))


@router.post("/consolidate")
async def consolidate(config: ClusterConfig = Depends(get_cluster_config)):
    return _touch(await functions.create_golden(config))


@router.post("/fraud")
async def detect_fraud(config: ClusterConfig = Depends(get_cluster_config)):
    return _touch(await functions.fraud_detection(config))


# ── Inspection ─────────────────────────────────────────────────────────────────


@router.get("/peek/{tier}/{table}")
async def peek_data(
    tier: str,
    table: str,
    limit: int = Query(default=FETCH_RECORD_NUM, ge=1, le=500),
    # Gold transactions is a single Delta table carrying a `fraud` flag rather
    # than a separate flagged table — consolidation writes every row with
    # fraud=false and detection merges the suspected ones back as true. This
    # filters to those, so "flagged" shows what its count claims.
    fraud_only: bool = Query(default=False),
    config: ClusterConfig = Depends(get_cluster_config),
):
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected", "records": []}

    table_path = f"{BASEDIR}/{tier}/{table}"
    if not os.path.lexists(f"{MOUNT_PATH}/{cluster_name}{table_path}"):
        return {
            "status": "error",
            "message": f"{tier}/{table} does not exist yet — run the earlier steps first.",
            "records": [],
        }

    if tier == VOLUME_GOLD:
        query = "fraud == True" if fraud_only else None
        df = await tables.delta_table_get(cluster_name, table_path, query)
        return {"records": df.head(limit).to_dict("records") if not df.empty else [], "count": int(len(df))}

    if tier == VOLUME_BRONZE and table == TABLE_CUSTOMERS:
        df = await to_thread(iceberger.find_all, cluster_name, tier, table)
        return {"records": df.head(limit).to_dict("records") if not df.empty else [], "count": int(len(df))}

    records = await tables.get_documents(config, table_path, limit=limit)
    return {"records": records, "count": len(records)}


@router.get("/iceberg/{tier}/{table}/history")
async def iceberg_history(
    tier: str, table: str, config: ClusterConfig = Depends(get_cluster_config),
):
    """Iceberg snapshot history — the time-travel story for the bronze table."""
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected", "history": []}

    if not os.path.exists(f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{tier}/{table}"):
        return {"status": "error", "message": f"Table not found: {tier}/{table}", "history": []}

    history = await to_thread(lambda: list(iceberger.history(cluster_name, tier, table)))
    return {"history": history}


@router.get("/iceberg/{tier}/{table}/tail")
async def iceberg_tail(
    tier: str, table: str, config: ClusterConfig = Depends(get_cluster_config),
):
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected", "records": []}

    df = await to_thread(iceberger.tail, cluster_name, tier, table)
    return {
        "records": df.tail(100).to_dict("records") if not df.empty else [],
        "total": int(len(df)),
    }


# ── Global namespace / object store ────────────────────────────────────────────


@router.get("/fs/list")
async def fs_list(
    path: str = Query(...),
    config: ClusterConfig = Depends(get_cluster_config),
):
    """List a directory in the global namespace.

    The path is confined to the NFS mount and passed as an argv element, never
    interpolated into a shell string — this endpoint takes a caller-supplied
    path, so a shell here would be a command-injection hole.
    """
    resolved = os.path.realpath(path)
    if resolved != MOUNT_PATH and not resolved.startswith(MOUNT_PATH + os.sep):
        raise HTTPException(status_code=400, detail=f"Path must be inside {MOUNT_PATH}")
    if not os.path.isdir(resolved):
        return {"output": f"{resolved} is not a directory (or is not mounted).", "path": resolved}

    proc = await asyncio.create_subprocess_exec(
        "ls", "-lA", "--", resolved,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
    return {"output": stdout.decode(errors="replace"), "path": resolved}


@router.get("/fs/stat")
async def fs_stat(
    target: str = Query(default="customers", pattern="^(customers|transactions)$"),
    config: ClusterConfig = Depends(get_cluster_config),
):
    """Show that generated data landed in the global namespace via NFS.

    Generation writes straight through the /mapr mount, so this reports the
    real path, size and mtime as proof the file is visible cluster-wide.
    """
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    name = TABLE_CUSTOMERS if target == "customers" else TABLE_TRANSACTIONS
    path = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{name}.csv"

    if not os.path.isfile(path):
        return {
            "status": "error",
            "message": f"{name}.csv not found in the global namespace — run Generate first.",
            "path": path,
        }

    st = await to_thread(os.stat, path)
    return {
        "status": "ok",
        "path": path,
        "bytes": st.st_size,
        "modified": int(st.st_mtime),
        "mount": MOUNT_PATH,
        "cluster": cluster_name,
    }


@router.post("/s3/upload")
async def s3_upload(config: ClusterConfig = Depends(get_cluster_config)):
    """Upload the generated transactions CSV to the fabric's S3 object store.

    Endpoint and keys come from settings. They used to arrive as query
    parameters, which put the S3 secret into request logs and browser history.
    """
    s = settings_module.load()
    endpoint = settings_module.resolve_endpoint(s.endpoints.s3_endpoint, s.cluster_host)
    access_key = s.credentials.s3_access_key
    secret_key = s.credentials.s3_secret

    if not access_key or not secret_key:
        raise HTTPException(
            status_code=400,
            detail="No S3 keys yet — generate them on the Setup page first.",
        )

    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    file = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{TABLE_TRANSACTIONS}.csv"
    if not os.path.isfile(file):
        return {"status": "error", "message": "transactions.csv not found — run Generate first."}

    return await mock_svc.upload_to_s3(
        file, endpoint, access_key, secret_key, s.targets.s3_bucket
    )
