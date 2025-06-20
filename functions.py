from functools import lru_cache
import random
import re
import shutil
from nicegui import events, background_tasks, run
import country_converter as coco
import pandas as pd

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

    logger.info("Profile to update %s", profile)

    # skip unmatched records (txn records left from previous data sets, ie, new customer csv ingested)
    if profile['_id'] == None: return

    # updated profile information is written to "silver" tier
    table_path = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}"

    if tables.upsert_document(table_path=table_path, json_dict=profile):
        logger.info("Updated profile: %s with score: %s", profile['_id'], profile['score'])


def get_customer_id(from_account: str):
    """Find the customerID from customers table using account number"""

    # NOTE: This should be reading from silver tier, as reading ID from bronze (dirty) data is not ideal
    found = iceberger.find_by_field(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS, field="account_number", value=from_account)

    if found is not None and len(found) > 0:
        logger.info(found.to_pydict())
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

    df = pd.DataFrame.from_dict(await tables.get_documents(table_path=f"{BASEDIR}/{tier}/{tablename}", limit=None))
    ui.notify(f"Found {df.shape[0]} rows in {tablename}")

    # assign a random category to the transaction
    df['category'] = df.apply(lambda _: random.choice(TRANSACTION_CATEGORIES), axis=1)

    try:
        logger.info("Loading %s documents into %s", df.shape[0], silver_transactions_table)
        if await tables.upsert_documents(table_path=silver_transactions_table, docs=df.to_dict("records")):
            ui.notify(f"Records are written to {silver_transactions_table}", type='positive')
        else:
            ui.notify(f"Failed to save records in {silver_transactions_table}", type='negative')

    except Exception as error:
        logger.warning(error)
        ui.notify(f"Failed to write into table: {error}", type='negative')

    finally:
        app.storage.user["busy"] = False


# SSE-TODO: output to be written to maprdb binary table
async def refine_customers():
    """
    Retrieve customers from "bronze" table, enrich/classify/mask and write into the binary table in "silver" tier

    :return bool: Success or Failure
    """

    # TODO: using DocumentDB here, change to BinaryDB
    silver_customers_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}"

    if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{VOLUME_BRONZE}/{TABLE_CUSTOMERS}"): # table not created yet
        ui.notify(f"Input table not found: {TABLE_CUSTOMERS} on {VOLUME_BRONZE}", type="warning")
        return

    cc = coco.CountryConverter()

    logger.info("Reading from iceberg")

    # app.storage.user["busy"] = True
    ui.notify("Processing customers for enrichment", type='positive')

    df = iceberger.find_all(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS)
    ui.notify(f"Found {df.shape[0]} rows in {TABLE_CUSTOMERS}")
    logger.info("Read %d rows", df.shape[0])

    df.drop_duplicates(subset="_id", keep="last", ignore_index=True, inplace=True)
    logger.info("Duplicates removed, left with %d records", df.shape[0])

    # add country column with short name (from ISO2 country code)
    df['country'] = cc.pandas_convert(df['country_code'], src="ISO2", to="name_short")
    logger.info("Added country name from country_code")

    # find and add iso3166-2 subdivision code (used for country map)
    @lru_cache()
    def to_iso3166_2(c):
        try:
            # tick = timeit.default_timer()
            import pycountry
            subdiv = pycountry.subdivisions.search_fuzzy(str(c))
            # logger.debug("subdivision search took: %f", timeit.default_timer() - tick)
            return subdiv[0].code if subdiv else ""

            # logger.debug(subdiv)
            # if len(subdiv) > 0:
            #     return subdiv
            # else:
            #     logger.debug("No subdiv for %s", c)
            #     return ""
        except Exception as error: # silently ignore non-found subdivisions
            logger.warning(error)

    logger.info("Searching county iso codes")
    df['iso3166_2'] = df["county"].map(to_iso3166_2)
    logger.debug(to_iso3166_2.cache_info())
    logger.info("County iso codes updated")

    # mask last n characters from the account_number
    # last_n_chars = 8
    # df["account_number"] = df["account_number"].astype(str).str[:-last_n_chars] + "*" * last_n_chars
    # mask birthdate and location
    df["birthdate"] = "*" * 8
    df["current_location"] = "*" * 4

    # # use _id to comply with OJAI primary key
    df.rename(columns={'id': '_id'}, inplace=True)

    try:
        logger.info("Loading %s documents into %s", df.shape[0], silver_customers_table)
        # if await background_tasks.create(run.io_bound(tables.upsert_documents, table_path=silver_customers_table, docs=df.to_dict("records"))):
        if await tables.upsert_documents(table_path=silver_customers_table, docs=df.to_dict("records")):
            ui.notify(f"Records are written to {silver_customers_table}", type='positive')
        else:
            ui.notify(f"Failed to save records in {silver_customers_table}", type='negative')

    except Exception as error:
        logger.warning(error)
        ui.notify(f"Failed to write into table: {error}", type='negative')

    finally:
        # app.storage.user["busy"] = False
        pass



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

        ui.label(f"Total records: {df.shape[0]}") # shape[0] giving total rows
        ui.table.from_pandas(df.tail(100), row_key="_id", title=f"{tier}.{tablename}").classes('w-full mt-6').props("dense")

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


async def peek_document(table_path: str):
    """
    Get `FETCH_RECORD_NUM` documents from DocumentDB table

    :param table_path str: full path for the JSON table
    """

    logger.info("Reading %s", f"{MOUNT_PATH}/{get_cluster_name()}{table_path}")
    if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{table_path}"): # table not created yet
        ui.notify(f"Table not found: {table_path}", type="warning")
        return

    docs = await tables.get_documents(table_path=table_path, limit=FETCH_RECORD_NUM)
    # logger.debug(docs)

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        with ui.row():
            ui.label(f"Fetched records: {len(docs)}")
        ui.table.from_pandas(pd.DataFrame([doc for doc in docs])).classes('w-full mt-6').props("dense")

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


async def peek_deltatable(table_path: str, query: str = None):
    """
    Get all records from deltalake table

    :param table_path str: path for the deltalake table relative to the cluster root
    """

    logger.info("Reading %s", f"{MOUNT_PATH}/{get_cluster_name()}{table_path}")
    if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{table_path}"): # table not created yet
        ui.notify(f"Table not found: {table_path}", type="warning")
        return

    df = await tables.delta_table_get(table_path=table_path, query=query)

    return df

async def peek_silver_profiles():
    await peek_document(table_path=f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}")


async def peek_silver_customers():
    await peek_document(table_path=f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}")


async def peek_bronze_transactions():
    await peek_document(table_path=f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}")


async def peek_silver_transactions():
    await peek_document(table_path=f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}")


async def gold_customers_table():
    customers = await peek_deltatable(table_path=f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_CUSTOMERS}")
    ui.label(f"Number of customers: {customers.shape[0]}")
    ui.table.from_pandas(customers).classes('w-full mt-6').props("dense")


async def gold_transactions_table():
        transactions = await peek_deltatable(table_path=f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}")
        ui.label(f"Number of transactions: {transactions.shape[0]}")
        ui.table.from_pandas(transactions).classes('w-full').props("dense")


async def gold_fraud_table():
        fraud = await peek_deltatable(table_path=f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}", query="fraud == True")
        ui.label(f"Number of fraud transactions: {fraud.shape[0]}")
        ui.table.from_pandas(fraud).classes('w-full').props("dense")


async def peek_gold_customers():
    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")
        await gold_customers_table()

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


async def peek_gold_transactions():
    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")
        await gold_transactions_table()

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


async def peek_gold_fraud():
    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")
        await gold_fraud_table()

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


async def peek_gold_all():
    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        await gold_customers_table()
        await gold_transactions_table()
        await gold_fraud_table()

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


async def create_golden():
    """
    Create golden data lake with consolidated customer and transaction information.

    Code mostly generated by ChatGPT - 4o (June 2024)

    """

    app.storage.user["busy"] = True

    # inputs from "Silver" tier
    profile_input_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}"
    customers_input_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}"
    transactions_input_table = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}"

    for input_file in [profile_input_table, customers_input_table, transactions_input_table]:
        if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{input_file}"): # table not created yet
            return (f"Input table not found: {input_file}", "warning")

    profiles_df = pd.DataFrame.from_dict(await tables.get_documents(table_path=profile_input_table, limit=None))
    customers_df = pd.DataFrame.from_dict(await tables.get_documents(table_path=customers_input_table, limit=None))
    transactions_df = pd.DataFrame.from_dict(await tables.get_documents(table_path=transactions_input_table, limit=None))

    if profiles_df.empty or customers_df.empty or transactions_df.empty:
        return ("Not all silver tables are populated", 'negative')

    # merge customers with profiles on _id
    updated_customers = pd.merge(customers_df, profiles_df, on="_id", how="left").fillna({"score": 0})

    # Clean up tables from PII/individual data, and remove duplicates
    updated_customers.drop(['name', 'birthdate', 'current_location', 'mail', 'username', 'address', 'account_number', 'ssn'], axis=1, inplace=True)
    # Convert score column to numeric
    updated_customers['score'] = pd.to_numeric(updated_customers['score'], errors='coerce').convert_dtypes()

    transactions_df.drop(["sender_account", "receiver_account"], axis=1, inplace=True)
    # Add fraud column
    transactions_df['fraud'] = False

    if await tables.delta_table_upsert(f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_CUSTOMERS}", updated_customers):
        ui.notify(f"Gold tier customers are written", type='positive')
    else:
        ui.notify("Failed to write gold customers", type='negative')

    if await tables.delta_table_upsert(f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}", transactions_df):
        ui.notify(f"Gold tier transactions are written", type='positive')
    else:
        ui.notify("Failed to write gold customers", type='negative')

    app.storage.user["busy"] = False


async def delete_volumes_and_streams():
    auth = (app.storage.user["MAPR_USER"], app.storage.user["MAPR_PASS"])

    app.storage.user['busy'] = True

    for vol in [VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD]:

        URL = f"https://{app.storage.user['MAPR_HOST']}:8443/rest/volume/remove?name={vol}"
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
                    ui.notify(f"{vol}: {res['errors'][0]['desc']}", type='warning')

    # Delete streams
    for stream in [STREAM_INCOMING, STREAM_CHANGELOG]:
        URL = f"https://{app.storage.user['MAPR_HOST']}:8443/rest/stream/delete?path={BASEDIR}/{stream}"
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
                    ui.notify(f"Stream: {stream}: {res['errors'][0]['desc']}", type='warning')

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

        # reset counters
        for metric in MONITORING_METRICS:
            app.storage.user[metric] = 0

        ui.notify("All volumes and data contained in them are removed", type="positive")

    except Exception as error:
        logger.warning(error)


    app.storage.user['busy'] = False


def handle_image_info(e: events.MouseEventArguments):
    # logger.debug(e)
    # eventargs is a dict with: type, element_id, image_x, image_y keys.
    element = e["element_id"]

    # Regex to convert "CamelCase" to "Title Case"
    title = " ".join(re.findall(r"[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))", element))

    if element == "Fraud":
        label = "Open the Fraud Data Pipeline"
    elif element == "NFS":
        label = "Bring existing datalakes into the 'Mesh'."
    elif element == "S3":
        label = "Bring existing Object Stores into the 'Mesh'."
    elif element == "IAM":
        label = "Globally manage and integrate with IAM for auth/z"
    elif element == "Edge":
        label = "'Data Domains' without 'Data Products' can join to the Mesh too"
    elif element == "Policies":
        label = "Define and distribute governance policies from federation hub"
    elif element == "Catalogue":
        label = "Enable Data Product registration and publishing from federation hub"

    # Data Domain functions
    elif element == "PublishTransactions":
        label = "Send transactions into the input stream"
    elif element == "PublishTransactionsCode":
        label = "Look at the code that publishes demo data for ingestion"
    elif element == "CreateData":
        label = "Generate mock data to use for ingestion - customers & transactions"
    elif element == "CreateDataCode":
        label = "Look at the code for creating the data for this demo"
    elif element == "IngestTransactions":
        label = "Process incoming transactions with stream consumer."
    elif element == "IngestCustomersIceberg":
        label = "Execute batch data ingestion."
    elif element == "BronzeTransactions":
        label = "Look at the transactions data from Bronze tier"
    elif element == "RefineTransactions":
        label = "Create refined transactions table in the silver tier"
    elif element == "RefineCustomers":
        label = "Create refined customers table in the silver tier"
    else:
        label = "Not defined yet"
    ui.notify(
        message=title,
        caption=label,
        type="info",
        color="secondary",
        progress=True,
        multi_line=True,
        timeout=2000,
    )


async def s3_upload():
    transactions_file = (
        f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv"
    )
    if (
        os.path.lexists(transactions_file)
        and len(app.storage.user.get("S3_SECRET_KEY", "")) > 0
    ):
        ui.notify(
            f"Copying {TABLE_TRANSACTIONS} to external S3 bucket", type="success"
        )
        await upload_to_s3(transactions_file)
    else:
        ui.notify(f"Missing path {transactions_file} or key {app.storage.user.get('S3_SECRET_KEY')}")

def nfs_upload():
    # Copy customers if NFS is mounted and customers csv exist
    customers_file = (
        f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv"
    )
    if os.path.lexists(customers_file):
        ui.notify(
            f"Copying {TABLE_CUSTOMERS} to external NFS server", type="success"
        )
        logger.info(
            shutil.copyfile(
                customers_file, f"/mnt/{TABLE_CUSTOMERS}.csv"
            )
        )
