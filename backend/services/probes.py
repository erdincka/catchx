"""Reachability probes for every service the demo depends on.

Each probe returns a dict:
  status     – "good" | "degraded" | "failed"
  latency_ms – round-trip in milliseconds
  detail     – human-readable summary (URL + status code / error)
  url        – the exact URL/address that was probed (for display in UI)

CatchX depends on the cluster REST API (:8443) and the S3 object store (:9000);
the Data Fabric MCP server (:5679) is optional. Nothing else is required.

All probes run in parallel; total time is bounded by the slowest probe.
Non-S3 HTTP probes include HTTP Basic auth with the cluster credentials.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

import settings as settings_module

logger = logging.getLogger("services.probes")

PROBE_TIMEOUT = 5.0


def _result(status: str, started: float, detail: str, url: str = "") -> dict[str, Any]:
    return {
        "status": status,
        "latency_ms": int((time.monotonic() - started) * 1000),
        "detail": detail,
        "url": url,
    }


def _classify(e: Exception, url: str) -> str:
    msg = str(e)
    if isinstance(e, httpx.ConnectError):
        return f"Connection refused at {url}"
    if isinstance(e, httpx.ConnectTimeout):
        return f"Connect timed out ({PROBE_TIMEOUT}s)"
    if isinstance(e, httpx.TimeoutException):
        return f"Request timed out ({PROBE_TIMEOUT}s)"
    if "ssl" in msg.lower() or "certificate" in msg.lower():
        return f"TLS error — {msg[:120]}"
    if "name or service not known" in msg.lower() or "nodename" in msg.lower():
        return "DNS resolution failed — check cluster host"
    return msg[:160]


async def _http(
    url: str,
    *,
    auth: tuple[str, str] | None = None,
    ok_codes: tuple[int, ...] = (200, 401, 403),
    degraded_codes: tuple[int, ...] = (500, 502, 503, 504),
) -> dict[str, Any]:
    started = time.monotonic()
    try:
        async with httpx.AsyncClient(verify=settings_module.ssl_verify(), timeout=PROBE_TIMEOUT) as client:
            r = await client.get(url, auth=auth)
        code = r.status_code
        if code in ok_codes:
            return _result("good", started, f"HTTP {code}", url)
        if code in degraded_codes:
            return _result("degraded", started, f"HTTP {code} — service may be starting", url)
        return _result("degraded", started, f"HTTP {code} — unexpected response", url)
    except Exception as e:
        return _result("failed", started, _classify(e, url), url)


# ── Per-service probes ─────────────────────────────────────────────────────────


async def probe_cluster(host: str, user: str, password: str) -> dict[str, Any]:
    started = time.monotonic()
    if not host:
        return _result("failed", started, "No cluster host configured — enter it in Settings", "")
    url = f"https://{host}:8443/rest/dashboard/info"
    try:
        async with httpx.AsyncClient(verify=settings_module.ssl_verify(), timeout=PROBE_TIMEOUT) as client:
            r = await client.get(url, auth=(user, password) if user else None)
        if r.status_code == 200:
            try:
                cluster = r.json()["data"][0]["cluster"]
                name = cluster.get("name", "unknown")
                ip = cluster.get("ip", "")
                return _result("good", started, f"{name} ({ip})", url)
            except Exception:
                return _result("good", started, "Cluster reachable (unexpected response shape)", url)
        if r.status_code == 401:
            return _result("degraded", started, "HTTP 401 — check cluster username / password", url)
        return _result("degraded", started, f"HTTP {r.status_code}", url)
    except Exception as e:
        return _result("failed", started, _classify(e, url), url)


async def probe_s3(endpoint: str) -> dict[str, Any]:
    if not endpoint or "{host}" in endpoint:
        return _result("failed", time.monotonic(), "Endpoint not resolved — set Cluster Host first", endpoint)
    url = endpoint.rstrip("/") + "/"
    return await _http(url)  # no Basic auth — S3 uses AWS SigV4


async def probe_mcp(url: str, user: str, password: str) -> dict[str, Any]:
    if not url or "{host}" in url:
        return _result("failed", time.monotonic(), "Endpoint not resolved — set Cluster Host first", url)
    # Data Fabric MCP exposes /s3, /file, /spark, /iceberg tool routes — probe /s3 as liveness check
    probe_url = url.rstrip("/") + "/s3"
    return await _http(probe_url, auth=(user, password) if user else None, ok_codes=(200, 401, 403, 405))


# ── Public entry point ─────────────────────────────────────────────────────────


async def probe_all() -> dict[str, dict[str, Any]]:
    s = settings_module.load()
    eps = settings_module.resolved_endpoints(s)
    creds = s.credentials
    user = creds.cluster_user
    password = creds.cluster_pass

    tasks: dict[str, Any] = {
        "cluster": probe_cluster(s.cluster_host, user, password),
        "s3":      probe_s3(eps["s3_endpoint"]),
    }
    # MCP is optional — probing it while disabled would report a red service
    # the demo does not actually require.
    if s.flags.mcp_enabled:
        tasks["mcp"] = probe_mcp(eps["mcp_server_url"], user, password)

    names = list(tasks.keys())
    results = await asyncio.gather(*tasks.values(), return_exceptions=True)

    out: dict[str, dict[str, Any]] = {}
    for name, res in zip(names, results):
        if isinstance(res, Exception):
            out[name] = {"status": "failed", "latency_ms": 0, "detail": str(res)[:160], "url": ""}
        else:
            out[name] = res
    return out
