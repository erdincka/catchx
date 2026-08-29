"""Source-code viewer.

Shows the real implementation of each pipeline step, read with
`inspect.getsource` so it can never drift from the code that actually runs.

A step's entry function is rarely the interesting part, though. The point of
the demo is that `publish_transactions` uses a stock Kafka producer and
`ingest_transactions` uses the OJAI client — and both of those live one call
deeper, in helpers the viewer used to hide. So each request also returns the
call chain: the entry function's own source, followed by the source of the
project functions it calls, resolved from the AST rather than a hand-kept list
that would rot the moment someone renames a helper.
"""

from __future__ import annotations

import ast
import inspect
import logging
import textwrap
from typing import Any, Optional

from fastapi import APIRouter, HTTPException

logger = logging.getLogger("routes.code")

router = APIRouter()

# Only the demo's service layer is followed. Connection plumbing (store,
# asyncutil, settings) resolves fine but says nothing about Data Fabric, and
# including it buried the Kafka and OJAI calls that are the actual point.
OWN_MODULE_PREFIXES = ("services.",)

# Trivial one-liners (path builders, thin async wrappers) are skipped: they add
# entries to scroll past without showing anything.
MIN_CALLEE_LINES = 4

MAX_DEPTH = 2

# Libraries worth calling out — the UI badges these so it is obvious at a glance
# that the demo talks to the fabric through standard clients.
LIBRARY_MARKERS = [
    ("confluent_kafka", "Kafka client"),
    ("mapr.ojai", "MapR OJAI"),
    ("pyiceberg", "Apache Iceberg"),
    ("deltalake", "Delta Lake"),
    ("minio", "S3 / MinIO"),
]

_registry: Optional[dict] = None


def _get_registry() -> dict:
    global _registry
    if _registry is not None:
        return _registry

    from services.functions import (
        refine_customers, refine_transactions, create_golden, fraud_detection,
        upsert_profiles, build_account_index,
    )
    from services.ingestion import ingest_customers_iceberg, ingest_transactions
    from services.mock import (
        create_customers, create_transactions, fake_customer, fake_transaction,
        publish_transactions, dummy_fraud_score,
    )
    from services import streams, tables, iceberger

    _registry = {
        "publish_transactions": publish_transactions,
        "create_customers": create_customers,
        "create_transactions": create_transactions,
        "fake_customer": fake_customer,
        "fake_transaction": fake_transaction,
        "ingest_customers_iceberg": ingest_customers_iceberg,
        "ingest_transactions": ingest_transactions,
        "refine_customers": refine_customers,
        "refine_transactions": refine_transactions,
        "create_golden": create_golden,
        "fraud_detection": fraud_detection,
        "upsert_profiles": upsert_profiles,
        "build_account_index": build_account_index,
        "dummy_fraud_score": dummy_fraud_score,
        "streams_produce_many": streams.produce_many,
        "streams_consume_batch": streams.consume_batch,
        "tables_upsert_documents": tables.upsert_documents,
        "tables_get_documents": tables.get_documents,
        "iceberger_write": iceberger.write,
        "iceberger_find_all": iceberger.find_all,
    }
    return _registry


def _is_own_code(obj: Any) -> bool:
    module = getattr(obj, "__module__", "") or ""
    return any(module == p or module.startswith(p) for p in OWN_MODULE_PREFIXES)


def _resolve(node: ast.AST, namespace: dict) -> Optional[Any]:
    """Turn a call target in the AST into the object it refers to at runtime.

    Resolution goes through the defining function's own globals, so it follows
    exactly what Python would call — no import-path guessing.
    """
    if isinstance(node, ast.Name):
        return namespace.get(node.id)

    if isinstance(node, ast.Attribute):
        base = node.value
        if isinstance(base, ast.Name):
            owner = namespace.get(base.id)
            if owner is not None:
                return getattr(owner, node.attr, None)
    return None


def _callees(fn: Any) -> list:
    """Project functions called by `fn`, in source order, de-duplicated.

    ast.walk covers nested helpers too, which matters here: several steps do
    their real work inside a closure passed to a worker thread.
    """
    try:
        source = textwrap.dedent(inspect.getsource(fn))
        tree = ast.parse(source)
    except (OSError, TypeError, SyntaxError) as e:
        logger.debug("Cannot parse %s: %s", getattr(fn, "__name__", fn), e)
        return []

    namespace = getattr(fn, "__globals__", {})
    found: list = []
    seen: set = set()

    def consider(node: ast.AST) -> None:
        target = _resolve(node, namespace)
        if target is None or not callable(target) or isinstance(target, type):
            return
        if not _is_own_code(target):
            return
        key = (getattr(target, "__module__", ""), getattr(target, "__qualname__", ""))
        if key in seen or target is fn:
            return
        seen.add(key)
        found.append(target)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue

        consider(node.func)

        # Functions handed to another call rather than invoked directly. The
        # whole service layer offloads blocking work as `to_thread(fn, ...)`,
        # so without this the Delta, Iceberg and OJAI implementations — the
        # code worth reading — never appear in the chain.
        for arg in list(node.args) + [kw.value for kw in node.keywords]:
            if isinstance(arg, (ast.Name, ast.Attribute)):
                consider(arg)

    return found


def _highlights(source: str) -> list:
    return [label for marker, label in LIBRARY_MARKERS if marker in source]


def _entry(fn: Any, depth: int) -> Optional[dict]:
    try:
        source = textwrap.dedent(inspect.getsource(fn))
    except (OSError, TypeError):
        return None
    # Skip trivial helpers, but never the function the user actually asked for.
    if depth > 0 and len(source.strip().splitlines()) < MIN_CALLEE_LINES:
        return None
    return {
        "name": getattr(fn, "__qualname__", getattr(fn, "__name__", "?")),
        "module": getattr(fn, "__module__", "?"),
        "source": source,
        "depth": depth,
        "highlights": _highlights(source),
    }


def _build_chain(fn: Any, max_depth: int = MAX_DEPTH) -> list:
    """Breadth-first walk so the most directly relevant code comes first."""
    chain: list = []
    seen: set = set()

    frontier = [(fn, 0)]
    while frontier:
        current, depth = frontier.pop(0)
        key = (getattr(current, "__module__", ""), getattr(current, "__qualname__", ""))
        if key in seen:
            continue
        seen.add(key)

        entry = _entry(current, depth)
        if entry is not None:
            chain.append(entry)

        if depth < max_depth:
            for callee in _callees(current):
                ckey = (getattr(callee, "__module__", ""), getattr(callee, "__qualname__", ""))
                if ckey not in seen:
                    frontier.append((callee, depth + 1))

    return chain


@router.get("/")
def list_functions():
    return {"available": sorted(_get_registry().keys())}


@router.get("/{function_name}")
def get_source(function_name: str):
    fn = _get_registry().get(function_name)
    if fn is None:
        raise HTTPException(status_code=404, detail=f"Function '{function_name}' not found")

    chain = _build_chain(fn)
    if not chain:
        raise HTTPException(status_code=500, detail="Cannot retrieve source for this function")

    return {
        "function_name": function_name,
        "module": chain[0]["module"],
        "highlights": sorted({h for c in chain for h in c["highlights"]}),
        # The entry function's own source, for callers that only want that.
        "source": chain[0]["source"],
        # Entry first, then the project functions it calls.
        "chain": chain,
    }
