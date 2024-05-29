import logging
import os
import pyarrow as pa

from helpers import *

logger = logging.getLogger()


def get_catalog(warehouse_path:str):
    if not os.path.isdir(warehouse_path):
        os.mkdir(warehouse_path)

    from pyiceberg.catalog.sql import SqlCatalog
    catalog = SqlCatalog(
        "docs",
        **{
            "uri": f"sqlite:///{warehouse_path}/iceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
        },
    )

    return catalog


def write(schemaname: str, tablename: str, records: list):

    warehouse_path = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{schemaname}/{tablename}"

    catalog = get_catalog(warehouse_path)

    if (schemaname,) not in catalog.list_namespaces():
        catalog.create_namespace(schemaname)

    ## build PyArrow table from python list
    df = pa.Table.from_pylist(records)
    ## - only create it for new table
    table = None
    try:
        table = catalog.create_table(
            f'{schemaname}.{tablename}',
            schema=df.schema,
            location=warehouse_path,
        )

    except:
        logger.debug("Table exists, append " + tablename)    
        table = catalog.load_table(f'{schemaname}.{tablename}')

    ### Write table to Iceberg
    table.append(df)
    return True


def tail(tier: str, tablename: str):

    warehouse_path = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{tier}/{tablename}"

    catalog = get_catalog(warehouse_path)

    table = catalog.load_table(f'{tier}.{tablename}')

    df = table.scan().to_pandas()

    return df.tail(5)


def history(tier: str, tablename: str):

    warehouse_path = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{tier}/{tablename}"

    catalog = get_catalog(warehouse_path)

    table = catalog.load_table(f'{tier}.{tablename}')

    for h in table.history():
         yield { 
                "date": datetime.datetime.fromtimestamp(int(h.timestamp_ms)/1000).strftime('%Y-%m-%d %H:%M:%S'), 
                "id": h.snapshot_id 
            }

