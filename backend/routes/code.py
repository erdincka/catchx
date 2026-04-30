import inspect
import logging

from fastapi import APIRouter, HTTPException

logger = logging.getLogger("routes.code")

router = APIRouter()

# Registry is built lazily to avoid import-time failures if MapR libs aren't present
_registry: dict | None = None


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


def _read_file(path: str) -> str | None:
    try:
        with open(path, "r") as f:
            return f.read()
    except Exception:
        return None


@router.get("/")
def list_functions():
    names = list(_get_registry().keys()) + ["airflow_dag", "nifi_template"]
    return {"available": names}


@router.get("/{function_name}")
def get_source(function_name: str, cluster: str = "", mapr_user: str = "", mapr_pass: str = ""):
    if function_name == "airflow_dag":
        source = _read_file("DAGs/csv_to_iceberg_DAG.py")
        if source is None:
            raise HTTPException(status_code=404, detail="DAG file not found")
        return {"function_name": "airflow_dag", "module": "DAGs/csv_to_iceberg_DAG.py", "source": source}

    if function_name == "nifi_template":
        from jinja2 import Environment, FileSystemLoader
        from config import BASEDIR, VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD, TABLE_TRANSACTIONS, DATA_PRODUCT, STREAM_INCOMING, TOPIC_TRANSACTIONS, MOUNT_PATH

        env = Environment(loader=FileSystemLoader("templates/"))
        try:
            template = env.get_template("TransactionFlow.xml.j2")
            content = template.render(
                hive_db_connect_url="jdbc:hive2://localhost:10000/default;auth=maprsasl;ssl=true",
                database_connection_url=f"jdbc:mariadb://{cluster}:3306/{DATA_PRODUCT}",
                database_driver_location=f"{MOUNT_PATH}/{DATA_PRODUCT}/user/root/mariadb-java-client-3.4.1.jar",
                database_user=mapr_user,
                database_password=mapr_pass,
                hive3_table_name=TABLE_TRANSACTIONS,
                hive3_external_table_location=f"{BASEDIR}/{VOLUME_SILVER}/hive{TABLE_TRANSACTIONS}",
                app_dir=BASEDIR,
                incoming_bulk_file=f"{TABLE_TRANSACTIONS}.csv",
                app_logs_failed=f"{BASEDIR}/logs/failed",
                app_logs=f"{BASEDIR}/logs",
                dir_app_logs_failed=f"{MOUNT_PATH}/{DATA_PRODUCT}{BASEDIR}/logs/failed",
                hive3_external_table_location_gold=f"{BASEDIR}/{VOLUME_GOLD}",
                put_db_record_table_name=TABLE_TRANSACTIONS,
                hbase_table_name_silver=f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}-binary",
                hbase_table_name_bronze=f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}-binary",
                incoming_topic=f"{BASEDIR}/{STREAM_INCOMING}:{TOPIC_TRANSACTIONS}",
                sasl_username=mapr_user,
                sasl_password=mapr_pass,
                bronze_transactions_dir=f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}",
            )
            return {"function_name": "nifi_template", "module": "templates/TransactionFlow.xml.j2", "source": content}
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

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
