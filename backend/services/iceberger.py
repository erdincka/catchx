import logging
import os

from config import BASEDIR, MOUNT_PATH

logger = logging.getLogger("iceberger")


def get_catalog(cluster_name: str):
    from pyiceberg.catalog.sql import SqlCatalog

    catalog_path = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}"
    return SqlCatalog(
        "default",
        **{
            "uri": f"sqlite:///{catalog_path}/iceberg.db",
            "warehouse": f"file://{catalog_path}",
        },
    )


def write(cluster_name: str, tier: str, tablename: str, records: list) -> bool:
    import pyarrow as pa

    catalog = get_catalog(cluster_name)
    catalog_path = f"{MOUNT_PATH}/{cluster_name}{BASEDIR}"
    expected_location = f"file://{catalog_path}/{tier}/{tablename}"

    try:
        if (tier,) not in catalog.list_namespaces():
            catalog.create_namespace(tier)

        table_identifier = (tier, tablename)

        if records:
            schema = pa.Table.from_pylist(records).schema
            is_new = table_identifier not in catalog.list_tables(tier)

            if is_new:
                catalog.create_table(table_identifier, schema=schema, location=expected_location)
                new_records = records
            else:
                existing_table = catalog.load_table(table_identifier)
                if existing_table.location() != expected_location:
                    # Catalog entry points to old location — purge and recreate at correct path
                    logger.warning(
                        "Migrating %s.%s: %s → %s",
                        tier, tablename, existing_table.location(), expected_location,
                    )
                    try:
                        catalog.purge_table(table_identifier)
                    except Exception:
                        catalog.drop_table(table_identifier)
                    catalog.create_table(table_identifier, schema=schema, location=expected_location)
                    new_records = records
                else:
                    # Correct location — deduplicate by _id before appending
                    existing = existing_table.scan(selected_fields=["_id"]).to_pandas()
                    existing_ids = set(existing["_id"].tolist()) if not existing.empty else set()
                    new_records = [r for r in records if r.get("_id") not in existing_ids]

            if new_records:
                catalog.load_table(table_identifier).append(pa.Table.from_pylist(new_records))
                skipped = len(records) - len(new_records)
                logger.info(
                    "Wrote %d records to %s.%s%s",
                    len(new_records), tier, tablename,
                    f" ({skipped} duplicates skipped)" if skipped else "",
                )
            else:
                logger.info("All %d records already exist in %s.%s — nothing written", len(records), tier, tablename)

            return True

    except Exception as error:
        logger.warning("Iceberg write error for %s.%s: %s", tier, tablename, error)

    return False


def find_all(cluster_name: str, tier: str, tablename: str):
    import pandas as pd

    try:
        catalog = get_catalog(cluster_name)
        table = catalog.load_table((tier, tablename))
        return table.scan().to_pandas()
    except Exception as error:
        logger.warning("Iceberg find_all error for %s.%s: %s", tier, tablename, error)
        return pd.DataFrame()


def find_by_field(cluster_name: str, tier: str, tablename: str, field: str, value: str):
    try:
        catalog = get_catalog(cluster_name)
        table = catalog.load_table((tier, tablename))
        result = table.scan(row_filter=f"{field} = '{value}'").to_arrow()
        return result if len(result) > 0 else None
    except Exception as error:
        logger.warning("Iceberg find_by_field error: %s", error)
        return None


def history(cluster_name: str, tier: str, tablename: str):
    try:
        catalog = get_catalog(cluster_name)
        table = catalog.load_table((tier, tablename))
        for h in table.history():
            yield str(h)
    except Exception as error:
        logger.warning("Iceberg history error for %s.%s: %s", tier, tablename, error)


def tail(cluster_name: str, tier: str, tablename: str):
    import pandas as pd

    try:
        catalog = get_catalog(cluster_name)
        table = catalog.load_table((tier, tablename))
        df = table.scan().to_pandas()
        return df
    except Exception as error:
        logger.warning("Iceberg tail error for %s.%s: %s", tier, tablename, error)
        return pd.DataFrame()
