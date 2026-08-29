from __future__ import annotations

import asyncio
import glob
import logging
import os
import functools
import subprocess
import time
from pathlib import Path
from typing import AsyncGenerator

import httpx
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse

from config import (
    BASEDIR, MOUNT_PATH, VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD,
    CATCHX_VOL_PARENT, CATCHX_VOL_BRONZE, CATCHX_VOL_SILVER, CATCHX_VOL_GOLD,
    STREAM_INCOMING, STREAM_CHANGELOG, TABLE_TRANSACTIONS,
)
from asyncutil import to_thread
from store import (
    ClusterConfig, cache_cluster_info, ensure_cluster_name,
    get_cached_cluster_info, get_cluster_config,
)
import settings as settings_module

logger = logging.getLogger("routes.cluster")

router = APIRouter()


# ── Shared helpers ─────────────────────────────────────────────────────────────

def _sse(name: str, status: str, message: str = "") -> str:
    import json
    return f"data: {json.dumps({'name': name, 'status': status, 'message': message})}\n\n"


def _classify_httpx_error(e: Exception, host: str) -> str:
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


def _check_mapr_client() -> bool:
    if glob.glob("/tmp/maprticket_*"):
        return True
    try:
        r = subprocess.run(["maprlogin", "info"], capture_output=True, timeout=3)
        return r.returncode == 0
    except Exception:
        return False


def _check_nfs() -> bool:
    try:
        return any("/mapr" in line for line in open("/proc/mounts"))
    except Exception:
        return False


async def _check_volume_path(host: str, path: str, user: str, password: str) -> str:
    """Check whether a volume is mounted at `path` using volume/info?path=."""
    try:
        async with httpx.AsyncClient(verify=settings_module.ssl_verify(), timeout=5) as client:
            r = await client.get(
                f"https://{host}:8443/rest/volume/info",
                auth=(user, password),
                params={"path": path},
            )
        if r.status_code == 200 and r.json().get("status") == "OK":
            return "ok"
        return "missing"
    except Exception:
        return "error"


async def _check_stream(host: str, stream_path: str, user: str, password: str) -> str:
    try:
        async with httpx.AsyncClient(verify=settings_module.ssl_verify(), timeout=5) as client:
            r = await client.get(
                f"https://{host}:8443/rest/stream/info",
                auth=(user, password),
                params={"path": stream_path},
            )
        return "ok" if r.status_code == 200 and r.json().get("status") == "OK" else "missing"
    except Exception:
        return "error"


# ── Readiness check ────────────────────────────────────────────────────────────

@router.get("/readiness")
async def get_readiness():
    """Check local client state (ticket, NFS) and remote artefact existence."""
    s = settings_module.load()
    host = s.cluster_host
    user = s.credentials.cluster_user
    password = s.credentials.cluster_pass

    # _check_mapr_client shells out to maprlogin (up to 3s) — keep it off the loop.
    client_configured = await to_thread(_check_mapr_client)
    nfs_mounted = _check_nfs()

    if host and user:
        results = await asyncio.gather(
            _check_volume_path(host, f"{BASEDIR}/{VOLUME_BRONZE}", user, password),
            _check_volume_path(host, f"{BASEDIR}/{VOLUME_SILVER}", user, password),
            _check_volume_path(host, f"{BASEDIR}/{VOLUME_GOLD}",   user, password),
            _check_stream(host, f"{BASEDIR}/{STREAM_INCOMING}",    user, password),
            return_exceptions=True,
        )
        def _r(v):
            return "error" if isinstance(v, Exception) else v
        volumes = {
            VOLUME_BRONZE: _r(results[0]),
            VOLUME_SILVER: _r(results[1]),
            VOLUME_GOLD:   _r(results[2]),
        }
        streams = {STREAM_INCOMING: _r(results[3])}
    else:
        volumes = {v: "unknown" for v in [VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD]}
        streams = {STREAM_INCOMING: "unknown"}

    return {
        "client_configured": client_configured,
        "nfs_mounted": nfs_mounted,
        "volumes": volumes,
        "streams": streams,
    }


# ── Client configuration SSE ───────────────────────────────────────────────────

async def _run(*args, **kwargs):
    """subprocess.run on a worker thread.

    These run inside an SSE generator; calling subprocess.run directly would
    block the event loop for the full timeout, stalling every other request
    and preventing this stream from flushing its own progress events.
    """
    return await to_thread(functools.partial(subprocess.run, *args, **kwargs))


async def _configure_client_stream(host: str, user: str, password: str) -> AsyncGenerator[str, None]:
    # Step 0: Fetch cluster info (need name + IP)
    yield _sse("connect", "running", f"Connecting to {host}…")
    cluster_name = ""
    cluster_ip = host
    try:
        async with httpx.AsyncClient(verify=settings_module.ssl_verify(), timeout=10) as client:
            r = await client.get(f"https://{host}:8443/rest/dashboard/info", auth=(user, password))
        if r.status_code == 401:
            yield _sse("connect", "error", "Authentication failed — check username/password")
            return
        if r.status_code != 200:
            yield _sse("connect", "error", f"Cluster returned HTTP {r.status_code}")
            return
        data = r.json()["data"][0]["cluster"]
        cluster_name = data["name"]
        cluster_ip = data.get("ip", host)
        cache_cluster_info(host, data)
        yield _sse("connect", "check", f"Connected to {cluster_name} ({cluster_ip})")
    except Exception as e:
        yield _sse("connect", "error", _classify_httpx_error(e, host))
        return

    # Step 1: Ensure local user
    yield _sse("user", "running", f"Ensuring local user '{user}'…")
    try:
        rc = (await _run(["id", user], capture_output=True)).returncode
        if rc != 0:
            await _run(
                ["useradd", "-u", "5000", "-U", "-m", "-d", f"/home/{user}", "-s", "/bin/bash", "-G", "sudo", user],
                capture_output=True,
            )
        await _run(["chpasswd"], input=f"{user}:{password}\nroot:{password}", text=True)
        yield _sse("user", "check", f"User '{user}' ready")
    except Exception as e:
        yield _sse("user", "error", str(e)[:200])

    # Step 2: SSH key + deploy to cluster
    yield _sse("ssh", "running", "Deploying SSH key to cluster…")
    try:
        ssh_dir = Path("/root/.ssh")
        ssh_dir.mkdir(exist_ok=True, mode=0o700)
        id_rsa = ssh_dir / "id_rsa"
        if not id_rsa.exists():
            await _run(["ssh-keygen", "-t", "rsa", "-b", "2048", "-f", str(id_rsa), "-q", "-N", ""])
        await _run(["ssh-keygen", "-f", str(ssh_dir / "known_hosts"), "-R", cluster_ip], capture_output=True)
        r2 = await _run(
            ["sshpass", "-p", password, "ssh-copy-id", "-o", "StrictHostKeyChecking=no", f"{user}@{cluster_ip}"],
            capture_output=True, text=True, timeout=20,
        )
        if r2.returncode != 0:
            yield _sse("ssh", "error", (r2.stderr or r2.stdout)[:200])
        else:
            yield _sse("ssh", "check", "SSH key deployed")
    except Exception as e:
        yield _sse("ssh", "error", str(e)[:200])

    # Step 3: Fetch SSL truststore
    yield _sse("ssl", "running", "Fetching SSL truststore from cluster…")
    try:
        r3 = await _run(
            ["scp", "-o", "StrictHostKeyChecking=no",
             f"{user}@{cluster_ip}:/opt/mapr/conf/ssl_truststore*", "/opt/mapr/conf/"],
            capture_output=True, text=True, timeout=20,
        )
        if r3.returncode != 0:
            yield _sse("ssl", "error", (r3.stderr or "SCP failed")[:200])
        else:
            yield _sse("ssl", "check", "Truststore ready")
    except Exception as e:
        yield _sse("ssl", "error", str(e)[:200])

    # Step 4: Run configure.sh
    yield _sse("configure", "running", f"Running configure.sh for '{cluster_name}'…")
    try:
        proc = await asyncio.create_subprocess_exec(
            "/opt/mapr/server/configure.sh", "-c", "-secure", "-N", cluster_name, "-C", cluster_ip,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=90)
        if proc.returncode != 0:
            yield _sse("configure", "error", stdout.decode(errors="replace")[:300])
        else:
            yield _sse("configure", "check", "MapR client configured")
    except asyncio.TimeoutError:
        yield _sse("configure", "error", "configure.sh timed out (90s)")
    except Exception as e:
        yield _sse("configure", "error", str(e)[:200])

    # Step 5: Copy key credentials
    yield _sse("keycreds", "running", "Copying key credentials…")
    try:
        for pattern in ["maprkeycreds.*", "maprtrustcreds.*", "maprhsm.conf"]:
            await _run(
                ["scp", "-o", "StrictHostKeyChecking=no",
                 f"{user}@{cluster_ip}:/opt/mapr/conf/{pattern}", "/opt/mapr/conf/"],
                capture_output=True, timeout=15,
            )
        yield _sse("keycreds", "check", "Key credentials copied")
    except Exception as e:
        yield _sse("keycreds", "error", str(e)[:200])

    # Step 6: Create MapR login ticket
    yield _sse("ticket", "running", "Creating MapR login ticket…")
    try:
        proc2 = await asyncio.create_subprocess_exec(
            "maprlogin", "password", "-user", user,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        stdout2, _ = await asyncio.wait_for(
            proc2.communicate(input=f"{password}\n".encode()), timeout=20
        )
        if proc2.returncode != 0:
            yield _sse("ticket", "error", stdout2.decode(errors="replace")[:200])
        else:
            yield _sse("ticket", "check", "Login ticket created")
    except Exception as e:
        yield _sse("ticket", "error", str(e)[:200])

    # Step 7: Mount /mapr via NFSv3
    yield _sse("nfs", "running", f"Mounting {cluster_ip}:/mapr…")
    try:
        await _run(["umount", "-l", "/mapr"], capture_output=True)
        os.makedirs("/mapr", exist_ok=True)
        proc3 = await asyncio.create_subprocess_exec(
            "mount", "-t", "nfs", "-o", "nfsvers=3,proto=tcp,nolock,sec=sys", f"{cluster_ip}:/mapr", "/mapr",
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        )
        stdout3, _ = await asyncio.wait_for(proc3.communicate(), timeout=30)
        if proc3.returncode != 0:
            yield _sse("nfs", "error", stdout3.decode(errors="replace")[:200])
        else:
            yield _sse("nfs", "check", "/mapr mounted successfully")
    except Exception as e:
        yield _sse("nfs", "error", str(e)[:200])


def _sse_response(gen) -> StreamingResponse:
    return StreamingResponse(
        gen,
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/configure")
async def configure_client(config: ClusterConfig = Depends(get_cluster_config)):
    """Stage 1 — configure the MapR client and mount the global namespace."""
    return _sse_response(_configure_client_stream(config.host, config.user, config.password))


# ── Artefact creation SSE ──────────────────────────────────────────────────────

async def _await_namespace(cluster_name: str, timeout: float = 30.0) -> bool:
    """Wait until a freshly created volume is visible through the NFS mount.

    A volume exists on the cluster before the NFS client can see its mount
    point, so provisioning used to report success while the very next write
    failed with ENOENT. Waiting here means "Provision succeeded" actually
    implies the demo can write.
    """
    from asyncutil import to_thread

    target = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await to_thread(os.path.isdir, target):
            return True
        await asyncio.sleep(1.0)
    return False


async def _create_artefacts_stream(host: str, user: str, password: str) -> AsyncGenerator[str, None]:
    config = ClusterConfig(host=host, user=user, password=password)

    yield _sse("volumes", "running", "Creating data lake volumes…")
    ok, msg = await _create_volumes(config)
    if ok:
        cluster_name = await ensure_cluster_name(config)
        if cluster_name and not await _await_namespace(cluster_name):
            ok, msg = False, (
                f"Volumes created, but {MOUNT_PATH}/{cluster_name}{BASEDIR} is not visible "
                "over NFS yet. Re-run client configuration, then provision again."
            )
    yield _sse("volumes", "check" if ok else "error", msg)

    yield _sse("tables", "running", "Creating binary tables…")
    ok, msg = await _create_tables(config)
    yield _sse("tables", "check" if ok else "error", msg)

    yield _sse("streams", "running", "Creating Kafka streams…")
    ok, msg = await _create_streams(config)
    yield _sse("streams", "check" if ok else "error", msg)


@router.post("/provision")
async def provision_artefacts(config: ClusterConfig = Depends(get_cluster_config)):
    """Stage 2 — create the demo volumes, tables and streams."""
    return _sse_response(_create_artefacts_stream(config.host, config.user, config.password))


@router.get("/info")
async def cluster_info(config: ClusterConfig = Depends(get_cluster_config)):
    """Live cluster identity and health, from the cluster REST API."""
    cached = get_cached_cluster_info(config.host)
    try:
        async with httpx.AsyncClient(verify=settings_module.ssl_verify(), timeout=10) as client:
            response = await client.get(
                f"https://{config.host}:8443/rest/dashboard/info",
                auth=(config.user, config.password),
            )
        if response.status_code == 401:
            return {"status": "error", "message": "Authentication failed — check the username and password."}
        if response.status_code != 200:
            return {"status": "error", "message": f"Cluster returned HTTP {response.status_code}"}
        cluster = response.json()["data"][0]["cluster"]
        cache_cluster_info(config.host, cluster)
        return {"status": "ok", "cluster": cluster}
    except Exception as error:
        logger.warning("Cluster info error: %s", error)
        if cached:
            return {"status": "stale", "cluster": cached, "message": _classify_httpx_error(error, config.host)}
        return {"status": "error", "message": _classify_httpx_error(error, config.host)}


# ── Volume / table / stream creation helpers ───────────────────────────────────

def _rest_ok(data: dict, label: str) -> bool:
    """Return True if the REST response is OK or the resource already exists."""
    status = data.get("status")
    if status == "OK":
        return True
    if status == "ERROR":
        errors = data.get("errors", [])
        descs = [e.get("desc", "") for e in errors]
        if any("exist" in d.lower() for d in descs):
            logger.info("%s already exists", label)
            return True
        logger.error("%s failed: %s", label, "; ".join(descs))
    else:
        logger.error("%s unexpected status %r: %s", label, status, data)
    return False


async def _rest_post(host: str, auth: tuple, path: str, params: dict) -> dict:
    async with httpx.AsyncClient(verify=settings_module.ssl_verify(), timeout=15) as client:
        r = await client.post(
            f"https://{host}:8443/rest/{path}",
            auth=auth,
            params=params,
        )
    return r.json()


_VOLUMES = [
    # (cluster-unique volume name, mount path)
    (CATCHX_VOL_PARENT, BASEDIR),
    (CATCHX_VOL_BRONZE, f"{BASEDIR}/{VOLUME_BRONZE}"),
    (CATCHX_VOL_SILVER, f"{BASEDIR}/{VOLUME_SILVER}"),
    (CATCHX_VOL_GOLD,   f"{BASEDIR}/{VOLUME_GOLD}"),
]

_VOL_PARAMS = {
    "replication": 1, "minreplication": 1,
    "nsreplication": 1, "nsminreplication": 1,
    "createparent": 1,
}


async def _create_volumes(config: ClusterConfig) -> tuple[bool, str]:
    auth = (config.user, config.password)
    for vol_name, mount_path in _VOLUMES:
        try:
            data = await _rest_post(config.host, auth, "volume/create",
                                    {"name": vol_name, "path": mount_path, **_VOL_PARAMS})
            status = data.get("status")
            if status == "OK":
                logger.info("Created volume '%s' at %s", vol_name, mount_path)
                continue
            if status == "ERROR":
                descs = [e.get("desc", "") for e in data.get("errors", [])]
                if any("exist" in d.lower() for d in descs):
                    # Volume name exists — verify it's mounted at OUR path
                    check = await _check_volume_path(config.host, mount_path, config.user, config.password)
                    if check == "ok":
                        logger.info("Volume '%s' already at %s", vol_name, mount_path)
                        continue
                    logger.error(
                        "Volume '%s' exists but NOT mounted at %s — manual cleanup needed",
                        vol_name, mount_path,
                    )
                    return False, (
                        f"Volume '{vol_name}' already exists but is not mounted at '{mount_path}'. "
                        "Delete the conflicting volume on the cluster and retry."
                    )
                errors = "; ".join(descs)
                logger.error("Volume '%s' creation failed: %s", vol_name, errors)
                return False, f"Volume '{vol_name}' creation failed: {errors}"
            return False, f"Volume '{vol_name}': unexpected status '{status}'"
        except Exception as e:
            logger.error("create_volumes error for %s: %s", vol_name, e)
            return False, f"Volume '{vol_name}': {e}"
    return True, "Volumes ready"


async def _create_tables(config: ClusterConfig) -> tuple[bool, str]:
    auth = (config.user, config.password)
    for tier in [VOLUME_BRONZE, VOLUME_SILVER]:
        table_path = f"{BASEDIR}/{tier}/{TABLE_TRANSACTIONS}-binary"
        try:
            data = await _rest_post(config.host, auth, "table/create", {
                "path": table_path,
                "tabletype": "binary",
                "defaultreadperm": "p",
                "defaultwriteperm": "p",
                "defaultappendperm": "p",
                "defaultunmaskedreadperm": "p",
            })
            if not _rest_ok(data, f"table/{tier}"):
                errors = "; ".join(e.get("desc", "") for e in data.get("errors", []))
                return False, f"Table '{tier}' creation failed: {errors}"
            # column family (ignore already-exists)
            await _rest_post(config.host, auth, "table/cf/create", {
                "path": table_path,
                "cfname": "cf1",
            })
        except Exception as e:
            logger.error("create_tables error for %s: %s", tier, e)
            return False, f"Table '{tier}': {e}"
    return True, "Binary tables ready"


async def _create_streams(config: ClusterConfig) -> tuple[bool, str]:
    auth = (config.user, config.password)
    for stream in [STREAM_INCOMING, STREAM_CHANGELOG]:
        stream_path = f"{BASEDIR}/{stream}"
        params: dict = {
            "path": stream_path,
            "ttl": 38400,
            "compression": "lz4",
            "produceperm": "p",
            "consumeperm": "p",
            "topicperm": "p",
        }
        if stream == STREAM_CHANGELOG:
            params.update({"ischangelog": "true", "defaultpartitions": 1})
        try:
            data = await _rest_post(config.host, auth, "stream/create", params)
            if not _rest_ok(data, f"stream/{stream}"):
                errors = "; ".join(e.get("desc", "") for e in data.get("errors", []))
                return False, f"Stream '{stream}' creation failed: {errors}"
        except Exception as e:
            logger.error("create_streams error for %s: %s", stream, e)
            return False, f"Stream '{stream}': {e}"
    return True, "Streams ready"


@router.delete("/cleanup")
async def cleanup(config: ClusterConfig = Depends(get_cluster_config)):
    from services.functions import delete_volumes_and_streams
    return await delete_volumes_and_streams(config)
