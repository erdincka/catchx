"""Cluster connection config and in-memory cluster-info cache.

Settings are the single source of truth: `ClusterConfig` is derived from the
persisted settings file, never from request headers. The browser holds no
credentials — it edits settings through /api/settings and everything else
reads them server-side.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from fastapi import HTTPException

logger = logging.getLogger("store")

# Cached cluster info keyed by host: { host: { "name": ..., "ip": ..., ... } }
_cluster_cache: dict = {}


@dataclass
class ClusterConfig:
    host: str
    user: str
    password: str


def current_config() -> Optional[ClusterConfig]:
    """Return the configured cluster, or None when settings are incomplete."""
    import settings as settings_module

    cfg = settings_module.load()
    if not cfg.cluster_host or not cfg.credentials.cluster_user:
        return None
    return ClusterConfig(
        host=cfg.cluster_host,
        user=cfg.credentials.cluster_user,
        password=cfg.credentials.cluster_pass,
    )


def get_cluster_config() -> ClusterConfig:
    """FastAPI dependency — 400s with an actionable message when unconfigured."""
    config = current_config()
    if config is None:
        raise HTTPException(
            status_code=400,
            detail="Cluster not configured — set the host and credentials on the Setup page.",
        )
    return config


def cache_cluster_info(host: str, info: dict):
    _cluster_cache[host] = info


def get_cached_cluster_info(host: str) -> Optional[dict]:
    return _cluster_cache.get(host)


def get_cluster_name(config: ClusterConfig) -> Optional[str]:
    info = get_cached_cluster_info(config.host)
    return info["name"] if info else None


async def ensure_cluster_name(config: ClusterConfig) -> Optional[str]:
    """Return cluster name from cache, re-fetching from the API on a cache miss."""
    name = get_cluster_name(config)
    if name:
        return name
    import httpx
    import settings as settings_module
    try:
        async with httpx.AsyncClient(verify=settings_module.ssl_verify(), timeout=5) as client:
            resp = await client.get(
                f"https://{config.host}:8443/rest/dashboard/info",
                auth=(config.user, config.password),
            )
        if resp.status_code == 200:
            cluster = resp.json()["data"][0]["cluster"]
            cache_cluster_info(config.host, cluster)
            logger.info("Re-cached cluster info for %s → %s", config.host, cluster["name"])
            return cluster["name"]
    except Exception as e:
        logger.warning("ensure_cluster_name failed for %s: %s", config.host, e)
    return None
