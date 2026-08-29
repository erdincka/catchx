"""Settings + service-status REST API.

Settings are the single source of truth for cluster host, credentials and
targets; the browser never holds them.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

import settings as settings_module
from services.probes import probe_all
from services.s3keys import get_or_create

logger = logging.getLogger("routes.settings")

router = APIRouter()


def _envelope(s: settings_module.Settings) -> dict:
    return {
        "settings": s.model_dump(),
        "resolved_endpoints": settings_module.resolved_endpoints(s),
        "configured": bool(s.cluster_host and s.credentials.cluster_user),
    }


@router.get("")
async def get_settings():
    return _envelope(settings_module.load())


@router.put("")
async def put_settings(payload: settings_module.Settings):
    return _envelope(await settings_module.save(payload))


@router.post("/reset")
async def reset_settings():
    return _envelope(await settings_module.save(settings_module.Settings()))


@router.post("/test")
async def test_services():
    return {"services": await probe_all()}


@router.get("/status")
async def services_status():
    return {"services": await probe_all()}


@router.post("/s3keys")
async def refresh_s3_keys():
    """Generate or refresh S3 keys using the configured cluster credentials.

    Keys are persisted into settings so the S3 upload path can read them
    directly rather than having them passed around by the caller.
    """
    s = settings_module.load()
    host = s.cluster_host
    user = s.credentials.cluster_user
    password = s.credentials.cluster_pass

    if not host or not user:
        raise HTTPException(
            status_code=400,
            detail="Set the cluster host and credentials before generating S3 keys.",
        )

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
