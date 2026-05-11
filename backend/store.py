import logging
from dataclasses import dataclass
from typing import Optional
from fastapi import Header, HTTPException

logger = logging.getLogger("store")

# Cached cluster info keyed by host: { host: { "name": ..., "ip": ..., ... } }
_cluster_cache: dict = {}


@dataclass
class ClusterConfig:
    host: str
    user: str
    password: str


def get_cluster_config(
    x_mapr_host: str = Header(default=""),
    x_mapr_user: str = Header(default=""),
    x_mapr_pass: str = Header(default=""),
) -> ClusterConfig:
    if x_mapr_host:
        return ClusterConfig(host=x_mapr_host, user=x_mapr_user, password=x_mapr_pass)
    # Fall back to server-side settings when no headers provided
    import settings as s_mod
    cfg = s_mod.load()
    if cfg.cluster_host:
        return ClusterConfig(
            host=cfg.cluster_host,
            user=cfg.credentials.cluster_user,
            password=cfg.credentials.cluster_pass,
        )
    raise HTTPException(status_code=400, detail="Cluster not configured — set cluster host in Settings or provide X-Mapr-Host header")


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
