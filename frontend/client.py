import logging

import httpx
from nicegui import app

from config import BACKEND_URL

logger = logging.getLogger("client")


def _headers() -> dict:
    return {
        "X-Mapr-Host": app.storage.user.get("MAPR_HOST", ""),
        "X-Mapr-User": app.storage.user.get("MAPR_USER", ""),
        "X-Mapr-Pass": app.storage.user.get("MAPR_PASS", ""),
    }


async def api_get(path: str, params: dict = None, timeout: float = 30.0):
    try:
        async with httpx.AsyncClient() as c:
            return await c.get(f"{BACKEND_URL}{path}", headers=_headers(), params=params, timeout=timeout)
    except Exception as error:
        logger.warning("GET %s failed: %s", path, error)
        return None


async def api_post(path: str, json: dict = None, timeout: float = 60.0):
    try:
        async with httpx.AsyncClient() as c:
            return await c.post(f"{BACKEND_URL}{path}", headers=_headers(), json=json, timeout=timeout)
    except Exception as error:
        logger.warning("POST %s failed: %s", path, error)
        return None


async def api_delete(path: str, timeout: float = 30.0):
    try:
        async with httpx.AsyncClient() as c:
            return await c.delete(f"{BACKEND_URL}{path}", headers=_headers(), timeout=timeout)
    except Exception as error:
        logger.warning("DELETE %s failed: %s", path, error)
        return None


def handle_response(response, success_key: str = None):
    """Notify user based on API response. Returns parsed JSON or None."""
    from nicegui import ui

    if response is None:
        ui.notify("Backend unreachable", type="negative")
        return None

    try:
        data = response.json()
    except Exception:
        ui.notify(f"Invalid response from backend", type="negative")
        return None

    if response.status_code >= 400:
        ui.notify(data.get("detail", "Request failed"), type="negative")
        return None

    status = data.get("status")
    message = data.get("message", "")

    if status == "error":
        ui.notify(message or "Operation failed", type="negative")
    elif status == "ok":
        if success_key and success_key in data:
            ui.notify(f"Done: {data[success_key]}", type="positive")
        elif message:
            ui.notify(message, type="positive")

    return data
