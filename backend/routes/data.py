import logging
import os

from fastapi import APIRouter, Depends, Query

from config import (
    BASEDIR, MOUNT_PATH, VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD,
    TABLE_CUSTOMERS, TABLE_TRANSACTIONS, TABLE_PROFILES, TABLE_FRAUD,
    FETCH_RECORD_NUM,
)
from store import ClusterConfig, get_cluster_config, get_cluster_name, ensure_cluster_name
from services import tables, iceberger
from services import mock as mock_svc
from services import functions, ingestion

logger = logging.getLogger("routes.data")

router = APIRouter()


@router.post("/customers/create")
async def create_customers(
    count: int = Query(default=200),
    config: ClusterConfig = Depends(get_cluster_config),
):
    return await mock_svc.create_customers(config, count)


@router.post("/transactions/create")
async def create_transactions(
    count: int = Query(default=100),
    config: ClusterConfig = Depends(get_cluster_config),
):
    return await mock_svc.create_transactions(config, count)


@router.post("/transactions/publish")
async def publish_transactions(
    count: int = Query(default=10),
    config: ClusterConfig = Depends(get_cluster_config),
):
    return await mock_svc.publish_transactions(config, count)


@router.get("/customers/preview")
async def preview_customers(config: ClusterConfig = Depends(get_cluster_config)):
    records = mock_svc.read_customers_preview(config)
    return {"records": records, "count": len(records)}


@router.get("/transactions/preview")
async def preview_transactions(config: ClusterConfig = Depends(get_cluster_config)):
    records = mock_svc.read_transactions_preview(config)
    return {"records": records, "count": len(records)}


@router.post("/ingest/customers")
async def ingest_customers(config: ClusterConfig = Depends(get_cluster_config)):
    return await ingestion.ingest_customers_iceberg(config)


@router.post("/ingest/transactions")
async def ingest_transactions(config: ClusterConfig = Depends(get_cluster_config)):
    return await ingestion.ingest_transactions(config)


@router.post("/refine/customers")
async def refine_customers(config: ClusterConfig = Depends(get_cluster_config)):
    return await functions.refine_customers(config)


@router.post("/refine/transactions")
async def refine_transactions(config: ClusterConfig = Depends(get_cluster_config)):
    return await functions.refine_transactions(config)


@router.post("/consolidate")
async def consolidate(config: ClusterConfig = Depends(get_cluster_config)):
    return await functions.create_golden(config)


@router.post("/fraud")
async def detect_fraud(config: ClusterConfig = Depends(get_cluster_config)):
    return await functions.fraud_detection(config)


@router.get("/peek/{tier}/{table}")
async def peek_data(
    tier: str,
    table: str,
    limit: int = Query(default=FETCH_RECORD_NUM),
    config: ClusterConfig = Depends(get_cluster_config),
):
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    table_path = f"{BASEDIR}/{tier}/{table}"
    full_path = f"{MOUNT_PATH}/{cluster_name}{table_path}"

    if not os.path.lexists(full_path):
        return {"status": "error", "message": f"Table not found: {table_path}", "records": []}

    if tier == VOLUME_GOLD:
        df = await tables.delta_table_get(cluster_name, table_path)
        records = df.head(limit).to_dict("records") if not df.empty else []
        return {"records": records, "count": len(df)}

    elif tier == VOLUME_BRONZE and table == TABLE_CUSTOMERS:
        df = iceberger.find_all(cluster_name, tier, table)
        records = df.head(limit).to_dict("records") if not df.empty else []
        return {"records": records, "count": len(df)}

    else:
        records = await tables.get_documents(config, table_path, limit=limit)
        return {"records": records, "count": len(records)}


@router.get("/iceberg/{tier}/{table}/history")
async def iceberg_history(
    tier: str,
    table: str,
    config: ClusterConfig = Depends(get_cluster_config),
):
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    full_path = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{tier}/{table}"
    if not os.path.exists(full_path):
        return {"status": "error", "message": f"Table not found: {tier}/{table}", "history": []}

    history = list(iceberger.history(cluster_name, tier, table))
    return {"history": history}


@router.get("/iceberg/{tier}/{table}/tail")
async def iceberg_tail(
    tier: str,
    table: str,
    config: ClusterConfig = Depends(get_cluster_config),
):
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    df = iceberger.tail(cluster_name, tier, table)
    records = df.tail(100).to_dict("records") if not df.empty else []
    return {"records": records, "total": len(df)}


@router.post("/s3/upload")
async def s3_upload(
    s3_server: str,
    access_key: str,
    secret_key: str,
    config: ClusterConfig = Depends(get_cluster_config),
):
    from config import DATA_PRODUCT
    cluster_name = await ensure_cluster_name(config)
    if not cluster_name:
        return {"status": "error", "message": "Cluster not connected"}

    file = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}/{TABLE_TRANSACTIONS}.csv"
    if not os.path.isfile(file):
        return {"status": "error", "message": f"File not found: {file}"}

    return await mock_svc.upload_to_s3(file, s3_server, access_key, secret_key, DATA_PRODUCT)


@router.get("/fs/list")
async def fs_list(
    path: str = Query(...),
    config: ClusterConfig = Depends(get_cluster_config),
):
    import asyncio
    # path is an absolute filesystem path already — do not prepend mount/cluster prefix
    process = await asyncio.create_subprocess_shell(
        f"ls -lA {path}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await process.communicate()
    return {"output": stdout.decode()}
