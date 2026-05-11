"""Settings + service-status REST API."""

from __future__ import annotations

import logging
import re
import subprocess

from fastapi import APIRouter, HTTPException

import settings as settings_module
from services.probes import probe_all
from services.s3keys import get_or_create

logger = logging.getLogger("routes.settings")

router = APIRouter()


@router.get("")
async def get_settings():
    s = settings_module.load()
    return {
        "settings": s.model_dump(),
        "resolved_endpoints": settings_module.resolved_endpoints(s),
    }


@router.put("")
async def put_settings(payload: settings_module.Settings):
    saved = await settings_module.save(payload)
    return {
        "settings": saved.model_dump(),
        "resolved_endpoints": settings_module.resolved_endpoints(saved),
    }


@router.post("/reset")
async def reset_settings():
    defaults = settings_module.Settings()
    saved = await settings_module.save(defaults)
    return {
        "settings": saved.model_dump(),
        "resolved_endpoints": settings_module.resolved_endpoints(saved),
    }


@router.post("/test")
async def test_services():
    return {"services": await probe_all()}


# Lightweight cached version for the home page; for now just re-runs probes.
# Caching can be added in chunk 7 when we have a clearer staleness budget.
@router.get("/status")
async def services_status():
    return {"services": await probe_all()}


@router.post("/s3keys")
async def refresh_s3_keys():
    """Generate or refresh S3 keys using cluster credentials.

    On success the keys are persisted into settings so every other service
    that needs them (S3 upload, Polaris, Spark) can read them directly.
    """
    s = settings_module.load()
    host = s.cluster_host
    user = s.credentials.cluster_user
    password = s.credentials.cluster_pass

    if not host or not user:
        raise HTTPException(status_code=400, detail="Cluster host and user must be configured before generating S3 keys")

    try:
        ak, sk = await get_or_create(host, user, password)
    except Exception as e:
        logger.error("S3 key generation failed: %s", e)
        raise HTTPException(status_code=502, detail=str(e))
    updated = settings_module.Settings.model_validate({
        **s.model_dump(),
        "credentials": {**s.credentials.model_dump(), "s3_access_key": ak, "s3_secret": sk},
    })
    await settings_module.save(updated)
    return {"access_key": ak, "secret_key_masked": sk[:4] + "***"}


@router.get("/polaris-creds")
async def fetch_polaris_creds():
    """SSH into the cluster and read the Polaris root-principal credentials file.

    Parses output of the form:
      realm: POLARIS root principal credentials: <client_id>:<client_secret>
    """
    s = settings_module.load()
    host = s.cluster_host
    user = s.credentials.cluster_user
    if not host or not user:
        raise HTTPException(status_code=400, detail="Cluster host and user must be configured first")

    cmd = (
        "cat /opt/mapr/polaris/polaris-"
        "$(cat /opt/mapr/polaris/polarisversion)"
        "/server/config/credentials.txt"
    )
    try:
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10",
             f"{user}@{host}", cmd],
            capture_output=True, text=True, timeout=15,
        )
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail=f"SSH timed out connecting to {host}")
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"SSH error: {e}")

    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()[:400]
        raise HTTPException(status_code=502, detail=f"SSH command failed: {detail}")

    output = result.stdout.strip()
    match = re.search(r"credentials:\s*(\S+:\S+)", output)
    if not match:
        raise HTTPException(status_code=502,
                            detail=f"Could not parse credentials from output: {output[:200]}")

    credential = match.group(1)
    updated = settings_module.Settings.model_validate({
        **s.model_dump(),
        "credentials": {**s.credentials.model_dump(), "polaris_credential": credential},
    })
    await settings_module.save(updated)
    logger.info("Polaris credential fetched and saved for %s", host)
    return {"polaris_credential": credential}
