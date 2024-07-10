import logging
import pyarrow as pa
from pyiceberg.expressions import EqualTo

from common import *
from nicegui import ui

logger = logging.getLogger("iceberger")
logging.getLogger("pyiceberg.io").setLevel(logging.DEBUG)


def hive_cat():
    try:
        from pyiceberg import catalog
        catalog = catalog.load_catalog(
            "default",
            **{
                "uri": "hive://10.1.1.31:10000/default",
                "ssl": True,
                # "s3.endpoint": "",
                # "s3.access-key-id": "2XVBOI33OQ9LYDZ1X5IPIRXMYVD8HWWN6938VNVDI03YHHBK4XP273GXD5BURQNQ44E14Y0SMCG7F7FJ9YK8HJ3EEOQR8VCA2",
                # "s3.secret-access-key": "FGOGA7I2BVV99HFPNRFOAUF79WOSZBO4V6T0D0P1ZGHSFLUTQ275HIO",
            },
        )
        print(catalog)
        print(catalog.list_namespaces())

    except Exception as error:
        logger.warning("Iceberg Catalog error: %s", error)
        ui.notify(f"Iceberg catalog error: {error}", type='negative')


def get_catalog():
    """Return the catalog, create if not exists"""

    catalog = None

    try:
        from pyiceberg.catalog.sql import SqlCatalog
        catalog = SqlCatalog(
            "default",
            **{
                "uri": f"sqlite:///{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/iceberg.db",
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
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

    warehouse_path = f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{tier}/{tablename}"

    catalog = get_catalog()

    if catalog is not None:
        # create namespace if missing
        if (tier,) not in catalog.list_namespaces():
            catalog.create_namespace(tier)

        table = None

        # Create table if missing
        try:
            table = catalog.create_table(
                f"{tier}.{tablename}",
                schema=pa.Table.from_pylist(records).schema,
                location=warehouse_path,
            )

        except:
            logger.info("Table exists, appending to: " + tablename)
            table = catalog.load_table(f"{tier}.{tablename}")

        existing = table.scan().to_pandas()

        incoming = pd.DataFrame.from_dict(records)

        merged = pd.concat([existing, incoming]).drop_duplicates(subset="_id", keep="last")

        table.append(pa.Table.from_pandas(merged, preserve_index=False))

        return True

    # catalog not found
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

    # warehouse_path = f"{MOUNT_PATH}/{get_cluster_name()}{DEMO['basedir']}/{tier}/{tablename}"

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


def find_all(tier: str, tablename: str):
    """
    Return pandas dataframe of all records

    :param tier str: tier volume name used as iceberg namespace

    :param tablename str: iceberg table name in the namespace

    :returns DataFrame: all records, or None
    """

    catalog = get_catalog()

    if catalog is not None:
        try:
            table = catalog.load_table(f'{tier}.{tablename}')
            df = table.scan().to_pandas()
            return df

        except Exception as error:
            logger.warning("Failed to scan table %s: %s", "cust", error)
            return None


def find_by_field(tier: str, tablename: str, field: str, value: str):
    """
    Find record(s) matching the field as arrow dataset

    :param tier str: tier volume name used as iceberg namespace

    :param tablename str: iceberg table name in the namespace

    :param field str: field in the table to match against

    :param value str: `field` value to match

    :return found `rows` or None
    """

    catalog = get_catalog()

    if catalog is not None:
        try:
            table = catalog.load_table(
                f'{tier}.{tablename}',
            )

            filtered = table.scan(
                row_filter=EqualTo(field, value),
                selected_fields=("_id",),
                # limit=1, # assuming no duplicates
            ).to_arrow()

            return filtered

        except:
            logger.warning("Cannot scan table: " + tablename)

        return None
