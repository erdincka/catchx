import inspect
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException

logger = logging.getLogger("routes.code")

router = APIRouter()

# Registry is built lazily to avoid import-time failures if MapR libs aren't present
_registry: Optional[dict] = None


def _get_registry() -> dict:
    global _registry
    if _registry is not None:
        return _registry

    from services.functions import (
        refine_customers, refine_transactions, create_golden, fraud_detection, upsert_profile,
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
        "upsert_profile": upsert_profile,
        "dummy_fraud_score": dummy_fraud_score,
        "streams_produce": streams.produce,
        "streams_consume": streams.consume,
        "tables_upsert_document": tables.upsert_document,
        "tables_get_documents": tables.get_documents,
        "iceberger_write": iceberger.write,
        "iceberger_find_all": iceberger.find_all,
    }
    return _registry



@router.get("/")
def list_functions():
    return {"available": list(_get_registry().keys())}


@router.get("/{function_name}")
def get_source(function_name: str):
    fn = _get_registry().get(function_name)
    if fn is None:
        raise HTTPException(status_code=404, detail=f"Function '{function_name}' not found")

    try:
        source = inspect.getsource(fn)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Cannot retrieve source: {e}")

    return {
        "function_name": function_name,
        "module": fn.__module__,
        "source": source,
    }
