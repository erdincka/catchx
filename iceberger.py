import logging
import os
import pyarrow as pa

from helpers import *

logger = logging.getLogger()

catalog_path = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/catalog"

def get_catalog():

    if not os.path.isdir(catalog_path):
        os.mkdir(catalog_path)
        logger.warning("Created catalog path: %s", catalog_path)

    from pyiceberg.catalog.sql import SqlCatalog
    try:
        catalog = SqlCatalog(
            "docs",
            **{
                "uri": f"sqlite:///{catalog_path}/iceberg_catalog.db",
                "warehouse": f"file://{catalog_path}",
            },
        )

    except Exception as error:
        logger.debug("Get Cataloge error: %s", error)
        return None

    return catalog


def write(tier: str, tablename: str, records: list):

    warehouse_path = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{tier}/{tablename}"

    catalog = get_catalog()

    if catalog is None:
        ui.notify(f"Catalog not found at {catalog_path}")
        return None

    if (tier,) not in catalog.list_namespaces():
        catalog.create_namespace(tier)

    ## build PyArrow table from python list
    df = pa.Table.from_pylist(records)
    ## - only create it for new table
    table = None
    try:
        table = catalog.create_table(
            f'{tier}.{tablename}',
            schema=df.schema,
            location=warehouse_path,
        )

    except:
        logger.debug("Table exists, append " + tablename)    
        table = catalog.load_table(f'{tier}.{tablename}')

    ### Write table to Iceberg
    table.append(df)
    return True


def tail(tier: str, tablename: str):

    warehouse_path = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{tier}/{tablename}"

    catalog = get_catalog()

    if catalog is None:
        ui.notify(f"Catalog not found at {catalog_path}")
        return None

    table = catalog.load_table(f'{tier}.{tablename}')

    df = table.scan().to_pandas()

    return df.tail(5)


def history(tier: str, tablename: str):

    # warehouse_path = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{tier}/{tablename}"

    catalog = get_catalog()

    if catalog is None:
        ui.notify(f"Catalog not found at {catalog_path}")
        return None

    logger.warning("Loading table: %s.%s", tier, tablename)

    table = catalog.load_table(f'{tier}.{tablename}')

    print(f"Got table: {table}")

    for h in table.history():
         yield { 
                "date": datetime.datetime.fromtimestamp(int(h.timestamp_ms)/1000).strftime('%Y-%m-%d %H:%M:%S'), 
                "id": h.snapshot_id 
            }


def stats(tier: str):
    metrics = {}

    for tablename in DEMO['tables']:
        warehouse_path = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{tier}/{tablename}"

        try:
            catalog = get_catalog()
            if catalog is None: continue # skip non-iceberg tables

        except Exception as error:
            logger.debug("Iceberg stat error on %s: %s", tier, error)

        table = catalog.load_table(f'{tier}.{tablename}')
        df = table.scan().to_pandas()

        metrics.update({ tablename: len(df) })

    return metrics

