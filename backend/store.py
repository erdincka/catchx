from dataclasses import dataclass
from fastapi import Header, HTTPException

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
    if not x_mapr_host:
        raise HTTPException(status_code=400, detail="Cluster not configured — provide X-Mapr-Host header")
    return ClusterConfig(host=x_mapr_host, user=x_mapr_user, password=x_mapr_pass)


def cache_cluster_info(host: str, info: dict):
    _cluster_cache[host] = info


def get_cached_cluster_info(host: str) -> dict | None:
    return _cluster_cache.get(host)


def get_cluster_name(config: ClusterConfig) -> str | None:
    info = get_cached_cluster_info(config.host)
    return info["name"] if info else None
