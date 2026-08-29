"""Data Fabric MCP server integration.

The fabric ships an MCP server exposing its capabilities as agent-callable
tools. This route discovers what that server offers so the demo can show a
real, current capability rather than a green dot on a status page.

Discovery is deliberately defensive: MCP deployments differ in transport and
auth, so we try the standard JSON-RPC `tools/list` first and fall back to
probing the documented route prefixes. Whatever is actually reachable is what
gets reported — nothing is asserted that was not observed.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException

import settings as settings_module

logger = logging.getLogger("routes.mcp")

router = APIRouter()

TIMEOUT = 6.0

# Documented Data Fabric MCP route prefixes, probed when JSON-RPC discovery
# is unavailable.
KNOWN_ROUTES = [
    ("s3", "Object store — list buckets, read and write objects"),
    ("file", "Global namespace — browse and read files over the fabric mount"),
    ("iceberg", "Iceberg tables — inspect schemas, snapshots and data"),
    ("spark", "Spark — submit and track jobs against fabric data"),
]


def _endpoint() -> tuple[str, tuple[str, str] | None]:
    s = settings_module.load()
    if not s.flags.mcp_enabled:
        raise HTTPException(status_code=409, detail="MCP is disabled in settings.")
    url = settings_module.resolve_endpoint(s.endpoints.mcp_server_url, s.cluster_host)
    if not url or "{host}" in url:
        raise HTTPException(status_code=400, detail="Set the cluster host before querying MCP.")
    creds = s.credentials
    auth = (creds.cluster_user, creds.cluster_pass) if creds.cluster_user else None
    return url.rstrip("/"), auth


async def _try_jsonrpc(client: httpx.AsyncClient, base: str, auth) -> list[dict[str, Any]] | None:
    """Standard MCP discovery: JSON-RPC `tools/list`."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    for path in ("", "/mcp", "/rpc"):
        try:
            r = await client.post(base + path, json=payload, auth=auth)
        except Exception:
            continue
        if r.status_code != 200:
            continue
        try:
            tools = r.json().get("result", {}).get("tools")
        except Exception:
            continue
        if isinstance(tools, list) and tools:
            return [
                {
                    "name": t.get("name", "?"),
                    "description": (t.get("description") or "").strip()[:240],
                    "source": "tools/list",
                }
                for t in tools
            ]
    return None


async def _probe_routes(client: httpx.AsyncClient, base: str, auth) -> list[dict[str, Any]]:
    """Fallback: report which documented route prefixes actually respond."""
    async def one(name: str, desc: str) -> dict[str, Any]:
        try:
            r = await client.get(f"{base}/{name}", auth=auth)
            # Anything that is not a transport failure means the route exists;
            # 401/403/405 still prove something is listening there.
            reachable = r.status_code < 500
            detail = f"HTTP {r.status_code}"
        except Exception as e:
            reachable, detail = False, type(e).__name__
        return {
            "name": name,
            "description": desc,
            "reachable": reachable,
            "detail": detail,
            "source": "route probe",
        }

    return list(await asyncio.gather(*(one(n, d) for n, d in KNOWN_ROUTES)))


@router.get("/tools")
async def list_tools():
    """Discover the tools the fabric's MCP server exposes."""
    base, auth = _endpoint()

    try:
        async with httpx.AsyncClient(
            verify=settings_module.ssl_verify(), timeout=TIMEOUT, follow_redirects=True
        ) as client:
            tools = await _try_jsonrpc(client, base, auth)
            if tools is not None:
                return {"status": "ok", "endpoint": base, "discovery": "tools/list", "tools": tools}

            routes = await _probe_routes(client, base, auth)
    except Exception as e:
        logger.warning("MCP discovery failed for %s: %s", base, e)
        raise HTTPException(status_code=502, detail=f"MCP server unreachable at {base}: {e}")

    if not any(r["reachable"] for r in routes):
        raise HTTPException(
            status_code=502,
            detail=f"MCP server at {base} did not respond on any known route.",
        )

    return {"status": "ok", "endpoint": base, "discovery": "route probe", "tools": routes}
