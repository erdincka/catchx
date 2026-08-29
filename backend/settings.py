"""Persisted demo configuration — the single source of truth.

Reads/writes one JSON file at $CATCHX_DATA_DIR/settings.json. In k8s that's a
PVC mount (e.g. /app/data); locally it defaults to ./data next to the backend.

The browser holds no credentials and sends no auth headers: the UI edits this
file through /api/settings and every other route reads it server-side. Keeping
one store is deliberate — an earlier split between browser session state and
this file meant "configured" in the UI did not imply configured on the server.

Scope note: CatchX depends on the Data Fabric cluster REST API (:8443), its S3
object store (:9000), and optionally the Data Fabric MCP server (:5679). It
needs no Grafana, OpenTSDB, Fluentd, Livy or external Iceberg catalog — stream
and volume telemetry come from the cluster's own REST API, and the Iceberg
catalog is a SQL catalog stored in the global namespace.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

from pydantic import BaseModel, Field

logger = logging.getLogger("settings")

DATA_DIR = Path(os.environ.get("CATCHX_DATA_DIR", "./data"))
SETTINGS_FILE = DATA_DIR / "settings.json"

_write_lock = asyncio.Lock()


# ── Defaults ───────────────────────────────────────────────────────────────────
# Endpoints carry a "{host}" placeholder substituted at read time, so the form
# follows whatever cluster host is configured without manual editing.

_DEFAULT_ENDPOINTS = {
    "s3_endpoint": "https://{host}:9000",
    "mcp_server_url": "https://{host}:5679",
}


# ── Schema ─────────────────────────────────────────────────────────────────────


class Endpoints(BaseModel):
    s3_endpoint: str = _DEFAULT_ENDPOINTS["s3_endpoint"]
    mcp_server_url: str = _DEFAULT_ENDPOINTS["mcp_server_url"]


class Credentials(BaseModel):
    cluster_user: str = ""
    cluster_pass: str = ""
    # S3 keys are generated via /rest/s3keys/generate — never user-entered.
    s3_access_key: str = ""
    s3_secret: str = ""


class DemoTargets(BaseModel):
    base_volume: str = "/catchx-demo"
    s3_bucket: str = "catchx-demo"
    stream_path: str = "/catchx-demo/incoming"


class FeatureFlags(BaseModel):
    # False = skip TLS verification, the norm for internal cluster certs.
    verify_ssl: bool = False
    # MCP is optional; when off it is neither probed nor surfaced.
    mcp_enabled: bool = True


class Settings(BaseModel):
    cluster_host: str = ""
    # TLS name presented to the DocumentDB (OJAI) endpoint on :5678.
    # MapR clusters usually carry a wildcard cert (CN=*.example.com), which an
    # IP address can never match — leave empty to auto-detect from the cluster's
    # own certificate, or set it explicitly for unusual PKI setups.
    ojai_target_name: str = ""
    endpoints: Endpoints = Field(default_factory=Endpoints)
    credentials: Credentials = Field(default_factory=Credentials)
    targets: DemoTargets = Field(default_factory=DemoTargets)
    flags: FeatureFlags = Field(default_factory=FeatureFlags)

    model_config = {"extra": "ignore"}  # tolerate settings.json from older versions


# ── Endpoint resolution ────────────────────────────────────────────────────────


def resolve_endpoint(template: str, host: str) -> str:
    """Substitute {host}. An empty host leaves the placeholder visible."""
    return template.replace("{host}", host) if host else template


def resolved_endpoints(s: Settings) -> dict:
    return {k: resolve_endpoint(v, s.cluster_host) for k, v in s.endpoints.model_dump().items()}


def ssl_verify() -> bool:
    """Current TLS verification flag (False = skip checks)."""
    return load().flags.verify_ssl


def is_configured() -> bool:
    s = load()
    return bool(s.cluster_host and s.credentials.cluster_user)


# ── Load / save ────────────────────────────────────────────────────────────────


_cached: Settings | None = None


def _ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load() -> Settings:
    """Load settings from disk, or defaults when nothing is persisted yet."""
    global _cached
    if _cached is not None:
        return _cached

    _ensure_dir()
    if not SETTINGS_FILE.exists():
        _cached = Settings()
        return _cached

    try:
        _cached = Settings.model_validate(json.loads(SETTINGS_FILE.read_text()))
    except Exception as e:
        logger.warning("Failed to load %s: %s — falling back to defaults", SETTINGS_FILE, e)
        _cached = Settings()
    return _cached


async def save(new_settings: Settings) -> Settings:
    """Persist atomically and refresh the in-memory cache."""
    global _cached
    async with _write_lock:
        _ensure_dir()
        tmp = SETTINGS_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(new_settings.model_dump(), indent=2))
        tmp.replace(SETTINGS_FILE)
        _cached = new_settings
        logger.info("Settings persisted to %s", SETTINGS_FILE)
        return _cached


def reset() -> Settings:
    """Clear the cache; the next load() returns defaults. Caller decides on persisting."""
    global _cached
    _cached = Settings()
    return _cached
