from functools import lru_cache
import random
import shutil
from nicegui import run
import country_converter as coco
import pandas as pd
import pycountry

from common import *
from mock import *
import tables
import iceberger

logger = logging.getLogger("functions")


async def upsert_profile(transaction: dict):
    """
    Assign customer ID and fraud score for the receiver account in transaction

    param: transaction dict: transaction record as dict
    """

    profile = {
        "_id": get_customer_id(transaction['receiver_account']),
        "score": await dummy_fraud_score()
    }
    
    logger.debug("Profile update %s", profile)
    
    # skip unmatched records (txn records left from previous data sets, ie, new customer csv ingested)
    if profile['_id'] == None: return

    # updated profile information is written to "silver" tier
    table_path = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}"

    if tables.upsert_document(table_path=table_path, json_dict=profile):
        logger.debug("Updated profile: %s with score: %s", profile['_id'], profile['score'])


def get_customer_id(from_account: str):
    """Find the customerID from customers table using account number"""

    # NOTE: This should be reading from silver tier, as reading ID from bronze (dirty) data is not ideal
    found = iceberger.find_by_field(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS, field="account_number", value=from_account)

    if found is not None and len(found) > 0:
        logger.debug(found)
        # get first column (id) from first row as string
        return found[0][0].as_py()

    else: return None


# SSE-TODO: for each individual transaction, add customer ID (using get_customer_id()) and mask account numbers (sender and receiver)
# output to be written to maprdb binary table
async def refine_transactions():
    """
    Retrieve customers from "bronze" table, enrich/classify/mask and write into the binary table in "silver" tier

    :param transaction dict: raw transaction record as dict

    :return bool: Success or Failure
    """

    # input table - iceberg
    tier = VOLUME_BRONZE
    tablename = TABLE_TRANSACTIONS

    # output table - maprdb binary
    # TODO: using DocumentDB here, change to BinaryDB
    silver_transactions_table = f"{BASEDIR}/{VOLUME_SILVER}/{tablename}"

    if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{tier}/{tablename}"): # table not created yet
        ui.notify(f"Input table not found: {tablename} on {tier} volume", type="warning")
        return

    # df = iceberger.find_all(tier=tier, tablename=tablename)
    df = pd.DataFrame.from_dict(tables.get_documents(table_path=f"{BASEDIR}/{tier}/{tablename}", limit=None))
    ui.notify(f"Found {df.shape[0]} rows in {tablename}")

    # assign a random category to the transaction
    df['category'] = df.apply(lambda _: random.choice(TRANSACTION_CATEGORIES), axis=1)

    # use _id to comply with OJAI primary key
    # df.rename(columns={'id': '_id'}, inplace=True)

    try:
        logger.info("Loading %s documents into %s", df.shape[0], silver_transactions_table)
        if await run.io_bound(tables.upsert_documents, table_path=silver_transactions_table, docs=df.to_dict("records")):
            ui.notify(f"Records are written to {silver_transactions_table}", type='positive')
        else:
            ui.notify(f"Failed to save records in {silver_transactions_table}", type='negative')

    except Exception as error:
        logger.warning(error)
        ui.notify(f"Failed to write into table: {error}", type='negative')

    finally:
        app.storage.general["busy"] = False


# SSE-TODO: output to be written to maprdb binary table
async def refine_customers():
    """
    Retrieve customers from "bronze" table, enrich/classify/mask and write into the binary table in "silver" tier

    :return bool: Success or Failure
    """

    # input table - iceberg
    tier = VOLUME_BRONZE
    tablename = TABLE_CUSTOMERS

    # output table - maprdb binary
    # TODO: using DocumentDB here, change to BinaryDB
    silver_customers_table = f"{BASEDIR}/{VOLUME_SILVER}/{tablename}"

    if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{tier}/{tablename}"): # table not created yet
        ui.notify(f"Input table not found: {tablename} on {tier}", type="warning")
        return

    cc = coco.CountryConverter()

    logger.info("Reading from iceberg")

    df = iceberger.find_all(tier=tier, tablename=tablename)
    ui.notify(f"Found {df.shape[0]} rows in {tablename}")
    logger.info("Read %d rows", df.shape[0])

    df.drop_duplicates(subset="_id", keep="last", ignore_index=True, inplace=True)
    logger.info("Removed duplicates, left with %d records", df.shape[0])

    # add country column with short name (from ISO2 country code)
    df['country'] = cc.pandas_convert(df['country_code'], src="ISO2", to="name_short")
    logger.debug("Added country name from country_code")

    # find and add iso3166-2 subdivision code (used for country map)
    @lru_cache()
    def to_iso3166_2(c):
        try:
            subdiv = pycountry.subdivisions.search_fuzzy(str(c))
            return subdiv[0].code if subdiv else ""
        except Exception as error: # silently ignore non-found counties
            logger.warning(error)

    logger.info("Searching county iso codes")
    df['iso3166_2'] = df["county"].map(to_iso3166_2)
    print(to_iso3166_2.cache_info())
    logger.info("County iso codes updated")

    # mask last n characters from the account_number
    # last_n_chars = 8
    # df["account_number"] = df["account_number"].astype(str).str[:-last_n_chars] + "*" * last_n_chars
    # mask birthdate
    df["birthdate"] = "*" * 8

    # # use _id to comply with OJAI primary key
    df.rename(columns={'id': '_id'}, inplace=True)

    try:
        logger.info("Loading %s documents into %s", df.shape[0], silver_customers_table)
        if await run.io_bound(tables.upsert_documents, table_path=silver_customers_table, docs=df.to_dict("records")):
            ui.notify(f"Records are written to {silver_customers_table}", type='positive')
        else:
            ui.notify(f"Failed to save records in {silver_customers_table}", type='negative')

    except Exception as error:
        logger.warning(error)
        ui.notify(f"Failed to write into table: {error}", type='negative')

    finally:
        app.storage.general["busy"] = False


def iceberg_table_history(tier: str, tablename: str):
    """
    Get Iceberg table history and display in dialog

    :param tier str: iceberg namespace matching the volume tier
    :param tablename str: iceberg table name
    """

    if not os.path.exists(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{tier}/{tablename}"): # table not created yet
        ui.notify(f"Table not found: {tier}/{tablename}", type="warning")
        return

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")
        result = ui.log().classes("w-full mt-6").style("white-space: pre-wrap")

        for history in iceberger.history(tier=tier, tablename=tablename):
            result.push(history)

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


def iceberg_table_tail(tier: str, tablename: str):
    """
    Get last 5 records from Iceberg table and display in dialog

    :param tier str: iceberg namespace matching the volume tier
    :param tablename str: iceberg table name
    """

    if not os.path.exists(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{tier}/{tablename}"): # table not created yet
        ui.notify(f"Table not found: {tier}/{tablename}", type="warning")
        return

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        df = iceberger.tail(tier=tier, tablename=tablename)

        logger.debug(df)

        ui.label(f"Total records: {df.shape[0]}") # shape[0] giving total rows
        ui.table.from_pandas(df.tail(), row_key="_id", title=f"{tier}.{tablename}").classes('w-full mt-6').props("dense")

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


def peek_documents(tablepath: str):
    """
    Get `FETCH_RECORD_NUM` documents from DocumentDB table

    :param tablepath str: full path for the JSON table
    """

    logger.info("Reading %s", f"{MOUNT_PATH}/{get_cluster_name()}{tablepath}")
    if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{tablepath}"): # table not created yet
        ui.notify(f"Table not found: {tablepath}", type="warning")
        return

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        docs = tables.get_documents(table_path=tablepath, limit=FETCH_RECORD_NUM)
        ui.table.from_pandas(pd.DataFrame.from_dict(docs)).classes('w-full mt-6').props("dense")

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


async def peek_sqlrecords(tablenames: list):
    """
    Get `FETCH_RECORD_NUM` documents from SQL DB tables

    :param tablename str: table name to query
    """

    mydb = f"mysql+pymysql://{app.storage.general['MYSQL_USER']}:{app.storage.general['MYSQL_PASS']}@{app.storage.general['cluster']}/{DATA_PRODUCT}"

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        for tablename in tablenames:
            try:
                docs = pd.read_sql(
                    f"SELECT * FROM {tablename} ORDER BY _id DESC LIMIT {FETCH_RECORD_NUM}",
                    con=mydb,
                )
                ui.table.from_pandas(docs, title=tablename, row_key="_id").classes('w-full mt-6').props("dense")
            except Exception as error:
                logger.warning(error)

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


async def create_golden():

    app.storage.general["busy"] = True

    (msg, sev) = await run.io_bound(data_aggregation)

    ui.notify(msg, type=sev)

    app.storage.general["busy"] = False


def data_aggregation():
    """
    Create golden data lake with consolidated customer and transaction information.

    Code mostly generated by ChatGPT - 4o (June 2024)

    """

    # inputs from "Silver" tier
    profile_input_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}"
    customers_input_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}"
    transactions_input_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}"

    for input_file in [profile_input_table, customers_input_table, transactions_input_table]:
        if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{input_file}"): # table not created yet
            return (f"Input table not found: {input_file}", "warning")

    profiles_df = pd.DataFrame.from_dict(tables.get_documents(table_path=profile_input_table, limit=None))
    customers_df = pd.DataFrame.from_dict(tables.get_documents(table_path=customers_input_table, limit=None))
    transactions_df = pd.DataFrame.from_dict(tables.get_documents(table_path=transactions_input_table, limit=None))

    if profiles_df.empty or customers_df.empty or transactions_df.empty:
        return ("Not all silver tables are populated", 'negative')

    # merge customers with profiles on _id
    updated_customers = pd.merge(customers_df, profiles_df, on="_id", how="left").fillna({"score": 0})

    # Clean up tables from PII/individual data, and remove duplicates
    updated_customers.drop(['name', 'birthdate', 'mail', 'username', 'address', 'account_number'], axis=1, inplace=True)
    transactions_df.drop(["sender_account", "receiver_account"], axis=1, inplace=True)

    # Append customers and transactions in the gold tier rdbms
    mydb = f"mysql+pymysql://{app.storage.general['MYSQL_USER']}:{app.storage.general['MYSQL_PASS']}@{app.storage.general['cluster']}/{DATA_PRODUCT}"

    # upsert by reading existing records and updating them
    try:
        existing_customers = pd.read_sql_table(table_name="customers", con=mydb)
    except Exception as error:
        existing_customers = pd.DataFrame()
        logger.warning(error)

    all_customers = pd.concat([existing_customers, updated_customers]).drop_duplicates(subset="_id", keep="last")
    num_customers = all_customers.to_sql(name="customers", con=mydb, if_exists='replace', index=False)

    try:
        existing_transactions = pd.read_sql_table(table_name="transactions", con=mydb)
    except Exception as error:
        existing_transactions = pd.DataFrame()
        logger.warning(error)

    all_transactions = pd.concat([existing_transactions, transactions_df]).drop_duplicates(subset="_id", keep="last")
    num_transactions = all_transactions.to_sql(
        name="transactions", con=mydb, if_exists="replace", index=False
    )

    return (f"{num_customers} customers and {num_transactions} transactions updated in {VOLUME_GOLD} tier", 'positive')


async def delete_volumes_and_streams():
    auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

    app.storage.general['busy'] = True

    for vol in [VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD]:

        URL = f"https://{app.storage.general['cluster']}:8443/rest/volume/remove?name={vol}"
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(URL, auth=auth)

            if response is None or response.status_code != 200:
                logger.warning("REST failed for delete volume: %s", vol)
                logger.warning("Response: %s", response.text)

            else:
                res = response.json()
                if res['status'] == "OK":
                    ui.notify(f"Volume '{vol}' deleted", type='warning')
                elif res['status'] == "ERROR":
                    ui.notify(f"{vol}: {res['errors'][0]['desc']}", type='negative')

    # Delete streams
    for stream in [STREAM_INCOMING, STREAM_CHANGELOG]:
        URL = f"https://{app.storage.general['cluster']}:8443/rest/stream/delete?path={BASEDIR}/{stream}"
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(URL, auth=auth)

            if response is None or response.status_code != 200:
                logger.warning(f"REST failed for delete stream: %s", stream)
                logger.warning("Response: %s", response.text)

            else:
                res = response.json()
                if res['status'] == "OK":
                    ui.notify(f"Stream '{stream}' deleted", type='warning')
                elif res['status'] == "ERROR":
                    ui.notify(f"Stream: {stream}: {res['errors'][0]['desc']}", type='negative')

    # delete app folder
    try:
        basedir = f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}"
        # remove iceberg tables and metadata catalog
        if os.path.exists(f"{basedir}/iceberg.db"):
            catalog = iceberger.get_catalog()
            for tier in [VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD]:
                if (tier,) in catalog.list_namespaces():
                    logger.info("Found ns: %s", tier)
                    for table in catalog.list_tables(tier):
                        logger.info("Found table %s in ns: %s", table, tier)
                        try:
                            catalog.purge_table(table)
                        except Exception as error:
                            logger.warning(error)
                try:
                    # remove the namespace
                    catalog.drop_namespace(tier)
                except Exception as error:
                    logger.warning(error)

            # finally delete the catalog file
            os.unlink(f"{basedir}/iceberg.db")

        ui.notify("Iceberg tables purged", type="warning")

        if os.path.isdir(basedir):
            shutil.rmtree(basedir, ignore_errors=True)
            ui.notify(f"{basedir} removed from {get_cluster_name()}", type="warning")

    except Exception as error:
        logger.warning(error)

    # remove tables from mysql
    mydb = f"mysql+pymysql://{app.storage.general['MYSQL_USER']}:{app.storage.general['MYSQL_PASS']}@{app.storage.general['cluster']}/{DATA_PRODUCT}"
    try:
        engine = create_engine(mydb)
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_CUSTOMERS};"))
            conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_TRANSACTIONS};"))
            conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_FRAUD};"))
            conn.commit()
            ui.notify("Database cleared", type='warning')

    except Exception as error:
        logger.warning(error)
        ui.notify(f"Failed to remove db tables: {error}", type='negative')

    app.storage.general['busy'] = False
