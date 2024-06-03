import asyncio
import random
from nicegui import run
import country_converter as coco
import pandas as pd

from helpers import *
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

    # updated profile information is written to "silver" tier
    table_path = f"{DEMO['basedir']}/{DEMO['volumes']['silver']}/{DEMO['tables']['profiles']}"

    if tables.upsert_document(table_path=table_path, json_dict=profile):
        logger.debug("Updated profile: %s with score: %d", profile['_id'], profile['score'])


async def dummy_fraud_score():
    """Return a random scoring between 1 to 10 with adding a delay to simulate querying to an AI model"""

    # add delay
    await asyncio.sleep(0.05)

    # respond with a random probability
    return random.randint(0, 10)


# TODO: get customer ID from customers table
def get_customer_id(from_account: str):
    """Find the customerID from customers table using account number"""

    # TODO: This should be changed to silver tier, as reading ID from bronze (dirty) data is not ideal
    found = iceberger.find_by_field(tier=DEMO['volumes']['bronze'], tablename=DEMO['tables']['customers'], field="account_number", value=from_account)

    if found is not None:
        # get first column (id) from first row
        return found[0][0]

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
    tier = DEMO['volumes']['bronze']
    tablename = DEMO['tables']['transactions']

    # output table - maprdb binary
    # TODO: using DocumentDB here, change to BinaryDB
    silver_transactions_table = f"{DEMO['basedir']}/{DEMO['volumes']['silver']}/{DEMO['tables']['transactions']}"

    df = iceberger.find_all(tier=tier, tablename=tablename)
    ui.notify(f"Found {df.count(axis=1).size} rows in {tablename}")

    # assign a random category to the transaction
    df['category'] = df.apply(lambda _: random.choice(TRANSACTION_CATEGORIES), axis=1)

    # use _id to comply with OJAI primary key
    df.rename(columns={'id': '_id'}, inplace=True)

    try:
        logger.info("Loading %s documents into %s", df.count(axis=1).size, silver_transactions_table)
        if await run.io_bound(tables.upsert_documents, table_path=silver_transactions_table, docs=df.to_dict("records")):
            ui.notify("Succeed", type='positive')
        else:
            ui.notify("Failed", type='negative')


    except Exception as error:
        logger.warning(error)
        ui.notify(f"Failed to write into table: {error}", type='negative')

    finally:
        ui.notify(f"Customers are written to {silver_transactions_table}", type='positive')


# SSE-TODO: for each customer with country_name empty, get country_code and add country_name field with converted name (GB -> Great Britain)
# output to be written to maprdb binary table
async def refine_customers():
    """
    Retrieve customers from "bronze" table, enrich/classify/mask and write into the binary table in "silver" tier

    :return bool: Success or Failure
    """

    # input table - iceberg
    tier = DEMO['volumes']['bronze']
    tablename = DEMO['tables']['customers']

    # output table - maprdb binary
    # TODO: using DocumentDB here, change to BinaryDB
    silver_customers_table = f"{DEMO['basedir']}/{DEMO['volumes']['silver']}/{DEMO['tables']['customers']}"

    cc = coco.CountryConverter()

    df = iceberger.find_all(tier=tier, tablename=tablename)
    ui.notify(f"Found {df.count(axis=1).size} rows in {tablename}")

    # add country_name column with short name (from ISO2 country code)
    df['country_name'] = cc.pandas_convert(df['country_code'], src="ISO2", to="name_short")

    # mask last n characters from the account_number
    last_n_chars = 8
    df["account_number"] = df["account_number"].astype(str).str[:-last_n_chars] + "*" * last_n_chars

    # use _id to comply with OJAI primary key
    df.rename(columns={'id': '_id'}, inplace=True)

    try:
        logger.info("Loading %s documents into %s", df.count(axis=1).size, silver_customers_table)
        if await run.io_bound(tables.upsert_documents, table_path=silver_customers_table, docs=df.to_dict("records")):
            ui.notify("Succeed", type='positive')
        else:
            ui.notify("Failed", type='negative')
        
    except Exception as error:
        logger.warning(error)
        ui.notify(f"Failed to write into table: {error}", type='negative')

    finally:
        ui.notify(f"Customers are written to {silver_customers_table}", type='positive')


def iceberg_table_history(tier: str, tablename: str):
    """
    Get Iceberg table history and display in dialog

    :param tier str: iceberg namespace matching the volume tier    
    :param tablename str: iceberg table name
    """

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

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        df = iceberger.tail(tier=tier, tablename=tablename)
        ui.table.from_pandas(df).classes('w-full mt-6')

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()

def peek_documents(tablepath: str):
    """
    Get 5 documents from DocumentDB table

    :param tablepath str: full path for the JSON table
    """

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        docs = tables.get_documents(table=tablepath, limit=5)
        ui.table.from_pandas(pd.DataFrame.from_dict(docs)).classes('w-full mt-6')
        
    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


# def detect_fraud(params: list, count: int):
#     """
#     Generator for randomly selected transactions to simulate fraud activity

#     """
#     # params is not used
#     table_path = f"{DEMO['basedir']}/{DEMO['volumes']['bronze']}/{DEMO['tables']['profiles']}"

#     # just a simulation of query to the profiles table, 
#     # if any doc is found with the number as their CHECK DIGITS in IBAN, we consider it as fraud!
#     whereClause = {
#         "$or":[
#             {"$like":{"sender":f"GB{count}%"}},
#             {"$like":{"receiver":f"GB{count}%"}}
#         ]
#     }

#     for doc in tables.search_documents(app.storage.general.get('cluster', 'localhost'), table_path, whereClause):

#         logger.debug("DB GET RESPONSE: %s", doc)

#         app.storage.general["counting"] = app.storage.general.get("counting", 0) + 1
#         yield f"Fraud txn from {doc['sender']} to {doc['receiver']}"
        
