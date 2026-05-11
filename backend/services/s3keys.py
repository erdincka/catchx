"""Auto-generate and cache S3 access keys via the Data Fabric REST API.

API reference:
  GET https://<host>:8443/rest/s3keys/generate
  GET https://<host>:8443/rest/s3keys/list

Both endpoints accept HTTP Basic auth with MapR cluster credentials.
"""

from __future__ import annotations

import logging

import httpx

import settings as settings_module

logger = logging.getLogger("services.s3keys")


async def _rest(
    host: str, user: str, password: str, path: str, params: dict | None = None
) -> dict:
    url = f"https://{host}:8443/rest/{path}"
    async with httpx.AsyncClient(
        verify=settings_module.ssl_verify(), timeout=15
    ) as client:
        r = await client.get(url, auth=(user, password), params=params or {})
    if not r.is_success:
        body = r.text[:400]
        raise httpx.HTTPStatusError(
            f"HTTP {r.status_code}: {body}",
            request=r.request,
            response=r,
        )
    return r.json()


async def list_keys(host: str, user: str, password: str) -> list[dict]:
    """Return existing S3 keys for the authenticated user."""
    try:
        data = await _rest(host, user, password, "s3keys/list")
        return data.get("data") or []
    except Exception as e:
        logger.warning("s3keys/list failed: %s", e)
        return []


async def generate_key(host: str, user: str, password: str) -> tuple[str, str]:
    """Generate a new S3 key pair.

    Returns (access_key, secret_key).
    Raises RuntimeError with a human-readable message on failure.
    """
    last_error: str = "Unknown error"

    for params in [
        {"accountname": "default", "domainname": "primary"},
        {},
    ]:
        try:
            data = await _rest(host, user, password, "s3keys/generate", params)
            entries = data.get("data") or []
            if not entries:
                raise RuntimeError(f"s3keys/generate returned empty data list: {data}")
            entry = entries[0]
            ak = entry.get("accesskey", "")
            sk = entry.get("secretkey", "")
            if not ak or not sk:
                raise RuntimeError(f"s3keys/generate entry missing keys: {entry}")
            logger.info("Generated S3 key for user %s on %s", user, host)
            return ak, sk
        except httpx.HTTPStatusError as e:
            last_error = str(e)
            logger.warning("s3keys/generate HTTP error (params=%s): %s", params, e)
            if e.response.status_code == 400 and not params:
                continue  # retry with explicit account/domain params
            break
        except RuntimeError as e:
            last_error = str(e)
            logger.error("s3keys/generate parse error: %s", e)
            break
        except Exception as e:
            last_error = str(e)
            logger.error("s3keys/generate unexpected error: %s", e)
            break

    raise RuntimeError(f"Failed to generate S3 keys: {last_error}")


async def get_or_create(host: str, user: str, password: str) -> tuple[str, str]:
    """Return an existing key pair if available, otherwise generate one.

    Raises RuntimeError if both listing and generation fail.
    """
    existing = await list_keys(host, user, password)
    if existing:
        entry = existing[0]
        ak = entry.get("accesskey", "")
        sk = entry.get("secretkey", "")
        if ak and sk:
            logger.debug("Reusing existing S3 key for %s", user)
            return ak, sk

    return await generate_key(host, user, password)
