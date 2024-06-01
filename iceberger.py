import logging
import pyarrow as pa
from pyiceberg.expressions import EqualTo

from helpers import *

logger = logging.getLogger("iceberger")


def get_catalog():
    """Create or return a catalog"""

    try:
        from pyiceberg.catalog.sql import SqlCatalog
        catalog = SqlCatalog(
            "default",
            **{
                "uri": "sqlite:////tmp/pyiceberg.db",
            },
        )

    except Exception as error:
        logger.warning("Iceberg Catalog error: %s", error)
        ui.notify(f"Iceberg catalog error: {error}", type='negative')

    finally:
        return catalog


def write(tier: str, tablename: str, records: list) -> bool:
    """
    Write rows into iceberg table

    :param tier str: namespace in catalog
    :param tablename str: table name in namespace
    :param records list: records to append to `tablename`

    :return bool: Success or failure
    """

    warehouse_path = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{tier}/{tablename}"

    catalog = get_catalog()

    if catalog is not None:
        if (tier,) not in catalog.list_namespaces():
            catalog.create_namespace(tier)

        # build PyArrow table from python list
        df = pa.Table.from_pylist(records)

        table = None

        # Create table if not exists
        try:
            table = catalog.create_table(
                f'{tier}.{tablename}',
                schema=df.schema,
                location=warehouse_path,
            )

        except:
            logger.info("Table exists, appending to: " + tablename)    
            table = catalog.load_table(f'{tier}.{tablename}')

        # Append to Iceberg table
        table.append(df)
        
        return True

    # if append didn't succeed
    return False


def tail(tier: str, tablename: str):
    """Return last 5 records from tablename"""

    catalog = get_catalog()

    if catalog is not None:

        table = catalog.load_table(f'{tier}.{tablename}')

        df = table.scan().to_pandas()

        return df.tail()


def history(tier: str, tablename: str):
    """Return list of snapshot history from tablename"""

    # warehouse_path = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{tier}/{tablename}"

    catalog = get_catalog()

    if catalog is not None:

        logger.debug("Loading table: %s.%s", tier, tablename)

        table = catalog.load_table(f'{tier}.{tablename}')

        logger.debug("Got table: %s", table)

        return [
                {
                    "date": datetime.datetime.fromtimestamp(int(h.timestamp_ms)/1000).strftime('%Y-%m-%d %H:%M:%S'), 
                    "id": h.snapshot_id 
                } 
                for h in table.history()
        ]
                
# TODO: need a better way to monitor table statistics
def stats(tier: str):
    """Return table statistics"""

    metrics = {}

    for tablename in DEMO['tables']:

        catalog = get_catalog()

        if catalog is not None:

            table = catalog.load_table(f'{tier}.{tablename}')
            df = table.scan().to_pandas()

            metrics.update({ tablename: len(df) })

    return metrics


def find_by_field(tier: str, tablename: str, field: str, value: str):
    """Find record(s) matching the field"""

    catalog = get_catalog()

    if catalog is not None:
        try:
            table = catalog.load_table(
                f'{tier}.{tablename}',
            )

            filtered = table.scan(
                row_filter=EqualTo(field, value),
                selected_fields=("id",),
                # limit=1, # assuming no duplicates
            ).to_arrow()

            return filtered

        except:
            logger.info("Table exists, append " + tablename)    
            table = catalog.load_table(f'{tier}.{tablename}')
        
        return None
