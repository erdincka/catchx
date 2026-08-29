"""Async helpers with a Python 3.8 fallback.

The MapR PACC base image ships Python 3.8, which predates `asyncio.to_thread`
(3.9). Everything in services/ offloads blocking client libraries through this
shim, so keep it importable on 3.8 — swapping in `asyncio.to_thread` directly
would break the container at import time.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import sys

if sys.version_info >= (3, 9):
    to_thread = asyncio.to_thread
else:
    async def to_thread(func, *args, **kwargs):  # type: ignore[misc]
        """Backport of asyncio.to_thread, context vars included."""
        loop = asyncio.get_event_loop()
        ctx = contextvars.copy_context()
        call = functools.partial(ctx.run, func, *args, **kwargs)
        return await loop.run_in_executor(None, call)


__all__ = ["to_thread"]
