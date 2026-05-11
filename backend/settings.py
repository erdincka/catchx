"""Persisted demo configuration.

Reads/writes a single JSON file at $NEXMESH_DATA_DIR/settings.json.
In k8s, NEXMESH_DATA_DIR is a PVC mount (e.g. /app/data); for local dev
it defaults to ./data relative to the backend cwd.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, Field

logger = logging.getLogger("settings")

DATA_DIR = Path(os.environ.get("NEXMESH_DATA_DIR", "./data"))
SETTINGS_FILE = DATA_DIR / "settings.json"

_write_lock = asyncio.Lock()


# ── Defaults ───────────────────────────────────────────────────────────────────
# Endpoints carry "{host}" placeholders that get substituted at read time so the
# pre-populated form follows whatever cluster host the user has entered.

_DEFAULT_ENDPOINTS = {
    "s3_endpoint": "https://{host}:9000",
    "polaris_url": "https://{host}:8181",
    "livy_url": "https://{host}:8998",
    "grafana_url": "https://{host}:3000",
    "opentsdb_url": "http://{host}:4242",
    "fluentd_host": "{host}:24224",
    "mcp_server_url": "https://{host}:5679",
}


# ── Schema ─────────────────────────────────────────────────────────────────────


class Endpoints(BaseModel):
    s3_endpoint: str = _DEFAULT_ENDPOINTS["s3_endpoint"]
    polaris_url: str = _DEFAULT_ENDPOINTS["polaris_url"]
    livy_url: str = _DEFAULT_ENDPOINTS["livy_url"]
    grafana_url: str = _DEFAULT_ENDPOINTS["grafana_url"]
    opentsdb_url: str = _DEFAULT_ENDPOINTS["opentsdb_url"]
    fluentd_host: str = _DEFAULT_ENDPOINTS["fluentd_host"]
    mcp_server_url: str = _DEFAULT_ENDPOINTS["mcp_server_url"]


class Credentials(BaseModel):
    cluster_user: str = ""
    cluster_pass: str = ""
    # S3 keys are auto-generated via /rest/s3keys/generate — not user-entered
    s3_access_key: str = ""
    s3_secret: str = ""
    polaris_credential: str = ""


class DemoTargets(BaseModel):
    base_volume: str = "/nexmesh-demo"
    s3_bucket: str = "nexmesh-demo"
    stream_path: str = "/nexmesh-demo/incoming"
    polaris_warehouse: str = "nexmesh"


class FeatureFlags(BaseModel):
    catalog: Literal["polaris"] = "polaris"
    verify_ssl: bool = False  # False = skip TLS verification (typical for internal clusters)


class Settings(BaseModel):
    cluster_host: str = ""
    endpoints: Endpoints = Field(default_factory=Endpoints)
    credentials: Credentials = Field(default_factory=Credentials)
    targets: DemoTargets = Field(default_factory=DemoTargets)
    flags: FeatureFlags = Field(default_factory=FeatureFlags)


# ── Endpoint resolution ────────────────────────────────────────────────────────


def resolve_endpoint(template: str, host: str) -> str:
    """Substitute {host} placeholders. Empty host leaves the placeholder visible."""
    return template.replace("{host}", host) if host else template


def resolved_endpoints(s: Settings) -> dict:
    return {
        k: resolve_endpoint(v, s.cluster_host)
        for k, v in s.endpoints.model_dump().items()
    }


def ssl_verify() -> bool:
    """Return the current SSL verification flag (False = skip TLS checks)."""
    return load().flags.verify_ssl


# ── Load / save ────────────────────────────────────────────────────────────────


_cached: Settings | None = None


def _ensure_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load() -> Settings:
    """Load settings from disk, or return defaults if none persisted yet."""
    global _cached
    if _cached is not None:
        return _cached

    _ensure_dir()
    if not SETTINGS_FILE.exists():
        _cached = Settings()
        return _cached

    try:
        raw = json.loads(SETTINGS_FILE.read_text())
        _cached = Settings.model_validate(raw)
        return _cached
    except Exception as e:
        logger.warning(
            "Failed to load %s: %s — falling back to defaults", SETTINGS_FILE, e
        )
        _cached = Settings()
        return _cached


async def save(new_settings: Settings) -> Settings:
    """Persist settings atomically and update the in-memory cache."""
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
    """Clear cache; next load() returns defaults. Caller decides whether to persist."""
    global _cached
    _cached = Settings()
    return _cached
