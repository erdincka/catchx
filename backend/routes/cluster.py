import asyncio
import logging
import os
from typing import AsyncGenerator

import httpx
from fastapi import APIRouter, Body, Depends
from fastapi.responses import StreamingResponse

from config import BASEDIR, VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD, STREAM_INCOMING, STREAM_CHANGELOG, TABLE_TRANSACTIONS
from store import ClusterConfig, cache_cluster_info, get_cached_cluster_info, get_cluster_config

logger = logging.getLogger("routes.cluster")

router = APIRouter()


@router.post("/connect")
async def connect(
    host: str = Body(...),
    user: str = Body(...),
    password: str = Body(...),
):
    auth = (user, password)
    try:
        async with httpx.AsyncClient(verify=False, timeout=10) as client:
            response = await client.get(f"https://{host}:8443/rest/dashboard/info", auth=auth)

        if response.status_code != 200:
            return {"status": "error", "message": f"HTTP {response.status_code}"}

        data = response.json()
        cluster = data["data"][0]["cluster"]
        cache_cluster_info(host, cluster)
        return {"status": "ok", "cluster": cluster}

    except Exception as error:
        logger.warning("Cluster connect error: %s", error)
        return {"status": "error", "message": str(error)}


@router.get("/info")
async def cluster_info(config: ClusterConfig = Depends(get_cluster_config)):
    info = get_cached_cluster_info(config.host)
    if info:
        return {"status": "ok", "cluster": info}
    # Try fetching if not cached
    return await connect(host=config.host, user=config.user, password=config.password)


def _classify_httpx_error(e: Exception, host: str) -> str:
    """Return a human-readable message for common httpx/network errors."""
    err = str(e)
    if isinstance(e, httpx.ConnectError):
        return f"Cannot reach {host} — check hostname/IP and network connectivity"
    if isinstance(e, httpx.TimeoutException):
        return f"Connection to {host} timed out — host may be unreachable or overloaded"
    if isinstance(e, httpx.ConnectTimeout):
        return f"Connection to {host} timed out"
    if "SSL" in err or "certificate" in err.lower():
        return f"TLS/SSL error connecting to {host}: {err}"
    return err


async def _setup_step_stream(host: str, user: str, password: str) -> AsyncGenerator[str, None]:
    import json
    from services.mock import create_customers, create_transactions
    from store import ClusterConfig, cache_cluster_info, get_cluster_name

    config = ClusterConfig(host=host, user=user, password=password)
    auth = (user, password)

    def event(name: str, status: str, message: str = ""):
        payload = json.dumps({"name": name, "status": status, "message": message})
        logger.debug("SSE event: %s", payload)
        return f"data: {payload}\n\n"

    # Step 1: Get cluster info
    logger.info("Setup step 1 — connecting to cluster at %s", host)
    yield event("clusterinfo", "running", f"Connecting to {host}…")
    try:
        async with httpx.AsyncClient(verify=False, timeout=15) as client:
            resp = await client.get(f"https://{host}:8443/rest/dashboard/info", auth=auth)

        if resp.status_code == 401:
            logger.warning("Setup: authentication failed for user at %s", host)
            yield event("clusterinfo", "error", "Authentication failed — check username and password")
            return
        if resp.status_code != 200:
            logger.warning("Setup: cluster info returned HTTP %s from %s", resp.status_code, host)
            yield event("clusterinfo", "error", f"Cluster returned HTTP {resp.status_code}")
            return

        data = resp.json()
        cluster = data["data"][0]["cluster"]
        cache_cluster_info(host, cluster)
        logger.info("Setup: connected to cluster '%s' (%s)", cluster.get("name"), host)
        yield event("clusterinfo", "check", f"Connected to {cluster['name']}")
    except Exception as e:
        msg = _classify_httpx_error(e, host)
        logger.error("Setup: cluster connect failed: %s", msg)
        yield event("clusterinfo", "error", msg)
        return

    # Step 2: Configure cluster
    logger.info("Setup step 2 — configuring cluster")
    yield event("reconfigure", "running", "Configuring cluster…")
    try:
        os.environ["CLUSTER_IP"] = cluster["ip"]
        os.environ["CLUSTER_NAME"] = cluster["name"]
        os.environ["MAPR_USER"] = user
        os.environ["MAPR_PASS"] = password
        process = await asyncio.create_subprocess_shell(
            "/bin/bash ./cluster_configure_and_attach.sh",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await process.communicate()
        rc = process.returncode
        if rc != 0:
            output = stdout.decode().strip()
            logger.warning("Setup: configure script exited %s: %s", rc, output)
            yield event("reconfigure", "error", f"Configure script failed (exit {rc}): {output[:200]}")
        else:
            logger.info("Setup: cluster configured successfully")
            yield event("reconfigure", "check", "Cluster configured")
    except Exception as e:
        logger.error("Setup: configure step failed: %s", e)
        yield event("reconfigure", "error", f"Configure failed: {e}")

    # Step 3: Create volumes, tables, streams
    logger.info("Setup step 3 — creating volumes, tables and streams")
    yield event("createvolumes", "running", "Creating volumes and streams…")
    try:
        from routes.cluster import _create_volumes, _create_tables, _create_streams
        ok = await _create_volumes(config) and await _create_tables(config) and await _create_streams(config)
        if ok:
            logger.info("Setup: volumes, tables and streams created")
            yield event("createvolumes", "check", "Volumes and streams ready")
        else:
            logger.warning("Setup: volume/stream creation reported failure")
            yield event("createvolumes", "error", "Failed to create one or more volumes or streams — check backend logs")
    except Exception as e:
        logger.error("Setup: volume/stream creation exception: %s", e)
        yield event("createvolumes", "error", f"Volume/stream setup failed: {e}")

    # Step 4: Mock data
    logger.info("Setup step 4 — generating mock data")
    yield event("mockdata", "running", "Generating mock data…")
    try:
        r1 = await create_customers(config)
        r2 = await create_transactions(config)
        ok = r1.get("status") == "ok" and r2.get("status") == "ok"
        if ok:
            logger.info("Setup: mock data created")
            yield event("mockdata", "check", "Mock data created")
        else:
            err1 = r1.get("message", "")
            err2 = r2.get("message", "")
            detail = " / ".join(filter(None, [err1, err2])) or "unknown error"
            logger.warning("Setup: mock data creation failed: %s", detail)
            yield event("mockdata", "error", f"Mock data failed: {detail}")
    except Exception as e:
        logger.error("Setup: mock data exception: %s", e)
        yield event("mockdata", "error", f"Mock data failed: {e}")


@router.post("/setup")
async def setup_cluster(
    host: str = Body(...),
    user: str = Body(...),
    password: str = Body(...),
):
    return StreamingResponse(
        _setup_step_stream(host, user, password),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


async def _create_volumes(config: ClusterConfig) -> bool:
    from config import MOUNT_PATH
    from store import get_cluster_name
    auth = (config.user, config.password)

    cluster_name = get_cluster_name(config)
    if cluster_name:
        basedir = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}"
        os.makedirs(basedir, exist_ok=True)

    for vol in [VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD]:
        URL = f"https://{config.host}:8443/rest/volume/create?name={vol}&path={BASEDIR}/{vol}&replication=1&minreplication=1&nsreplication=1&nsminreplication=1"
        try:
            async with httpx.AsyncClient(verify=False, timeout=10) as client:
                response = await client.post(URL, auth=auth)
            res = response.json()
            if res["status"] not in ("OK", "ERROR"):
                return False
        except Exception as e:
            logger.warning("create_volumes error for %s: %s", vol, e)
            return False
    return True


async def _create_tables(config: ClusterConfig) -> bool:
    auth = (config.user, config.password)

    for tier in [VOLUME_BRONZE, VOLUME_SILVER]:
        table_path = f"{BASEDIR}/{tier}/{TABLE_TRANSACTIONS}-binary"
        for URL in [
            f"https://{config.host}:8443/rest/table/create?path={table_path}&tabletype=binary&defaultreadperm=p&defaultwriteperm=p&defaultappendperm=p&defaultunmaskedreadperm=p",
            f"https://{config.host}:8443/rest/table/cf/create?path={table_path}&cfname=cf1",
        ]:
            try:
                async with httpx.AsyncClient(verify=False, timeout=10) as client:
                    await client.post(URL, auth=auth)
            except Exception as e:
                logger.warning("create_tables error: %s", e)
    return True


async def _create_streams(config: ClusterConfig) -> bool:
    auth = (config.user, config.password)

    for stream in [STREAM_INCOMING, STREAM_CHANGELOG]:
        URL = f"https://{config.host}:8443/rest/stream/create?path={BASEDIR}/{stream}&ttl=38400&compression=lz4&produceperm=p&consumeperm=p&topicperm=p"
        if stream == STREAM_CHANGELOG:
            URL += "&ischangelog=true&defaultpartitions=1"
        try:
            async with httpx.AsyncClient(verify=False, timeout=10) as client:
                await client.post(URL, auth=auth)
        except Exception as e:
            logger.warning("create_streams error for %s: %s", stream, e)
    return True


@router.post("/volumes")
async def create_volumes(config: ClusterConfig = Depends(get_cluster_config)):
    ok = await _create_volumes(config)
    return {"status": "ok" if ok else "error"}


@router.post("/tables")
async def create_tables(config: ClusterConfig = Depends(get_cluster_config)):
    ok = await _create_tables(config)
    return {"status": "ok" if ok else "error"}


@router.post("/streams")
async def create_streams(config: ClusterConfig = Depends(get_cluster_config)):
    ok = await _create_streams(config)
    return {"status": "ok" if ok else "error"}


@router.delete("/cleanup")
async def cleanup(config: ClusterConfig = Depends(get_cluster_config)):
    from services.functions import delete_volumes_and_streams
    return await delete_volumes_and_streams(config)
