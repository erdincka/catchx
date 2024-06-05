import asyncio
import json
import random
from nicegui import run
import country_converter as coco
import pandas as pd

from common import *
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
    table_path = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['silver']}/{DATA_DOMAIN['tables']['profiles']}"

    if tables.upsert_document(table_path=table_path, json_dict=profile):
        logger.debug("Updated profile: %s with score: %s", profile['_id'], profile['score'])


async def dummy_fraud_score():
    """Return a random percentile with adding a delay to simulate querying to an AI model"""

    # add delay
    await asyncio.sleep(0.02)

    # respond with a random probability, using string to avoid OJAI conversion to this \"score\": {\"$numberLong\": 46}}
    return str(random.randint(0, 100))


def get_customer_id(from_account: str):
    """Find the customerID from customers table using account number"""

    # NOTE: This should be reading from silver tier, as reading ID from bronze (dirty) data is not ideal
    found = iceberger.find_by_field(tier=DATA_DOMAIN['volumes']['bronze'], tablename=DATA_DOMAIN['tables']['customers'], field="account_number", value=from_account)

    if found is not None:
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
    tier = DATA_DOMAIN['volumes']['bronze']
    tablename = DATA_DOMAIN['tables']['transactions']

    # output table - maprdb binary
    # TODO: using DocumentDB here, change to BinaryDB
    silver_transactions_table = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['silver']}/{tablename}"

    if not os.path.lexists(f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{tier}/{tablename}"): # table not created yet
        ui.notify(f"Input table not found: {tablename} on {tier} volume", type="warning")
        return

    app.storage.user["busy"] = True

    # df = iceberger.find_all(tier=tier, tablename=tablename)
    df = pd.DataFrame.from_dict(tables.get_documents(table_path=f"{DATA_DOMAIN['basedir']}/{tier}/{tablename}", limit=None))
    ui.notify(f"Found {df.count(axis=1).size} rows in {tablename}")

    # assign a random category to the transaction
    df['category'] = df.apply(lambda _: random.choice(TRANSACTION_CATEGORIES), axis=1)

    # use _id to comply with OJAI primary key
    # df.rename(columns={'id': '_id'}, inplace=True)

    try:
        logger.info("Loading %s documents into %s", df.count(axis=1).size, silver_transactions_table)
        if await run.io_bound(tables.upsert_documents, table_path=silver_transactions_table, docs=df.to_dict("records")):
            ui.notify(f"Records are written to {silver_transactions_table}", type='positive')
        else:
            ui.notify(f"Failed to save records in {silver_transactions_table}", type='negative')

    except Exception as error:
        logger.warning(error)
        ui.notify(f"Failed to write into table: {error}", type='negative')

    finally:
        app.storage.user["busy"] = False


# SSE-TODO: for each customer with country_name empty, get country_code and add country_name field with converted name (GB -> Great Britain)
# output to be written to maprdb binary table
async def refine_customers():
    """
    Retrieve customers from "bronze" table, enrich/classify/mask and write into the binary table in "silver" tier

    :return bool: Success or Failure
    """

    # input table - iceberg
    tier = DATA_DOMAIN['volumes']['bronze']
    tablename = DATA_DOMAIN['tables']['customers']

    # output table - maprdb binary
    # TODO: using DocumentDB here, change to BinaryDB
    silver_customers_table = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['silver']}/{tablename}"

    if not os.path.lexists(f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{tier}/{tablename}"): # table not created yet
        ui.notify(f"Input table not found: {tablename} on {tier}", type="warning")
        return

    cc = coco.CountryConverter()

    app.storage.user["busy"] = True

    df = iceberger.find_all(tier=tier, tablename=tablename)
    ui.notify(f"Found {df.count(axis=1).size} rows in {tablename}")

    # add country_name column with short name (from ISO2 country code)
    df['country_name'] = cc.pandas_convert(df['country_code'], src="ISO2", to="name_short")

    # mask last n characters from the account_number
    last_n_chars = 8
    df["account_number"] = df["account_number"].astype(str).str[:-last_n_chars] + "*" * last_n_chars

    # # use _id to comply with OJAI primary key
    df.rename(columns={'id': '_id'}, inplace=True)

    try:
        logger.info("Loading %s documents into %s", df.count(axis=1).size, silver_customers_table)
        if await run.io_bound(tables.upsert_documents, table_path=silver_customers_table, docs=df.to_dict("records")):
            ui.notify(f"Records are written to {silver_customers_table}", type='positive')
        else:
            ui.notify(f"Failed to save records in {silver_customers_table}", type='negative')
        
    except Exception as error:
        logger.warning(error)
        ui.notify(f"Failed to write into table: {error}", type='negative')

    finally:
        app.storage.user["busy"] = False


def iceberg_table_history(tier: str, tablename: str):
    """
    Get Iceberg table history and display in dialog

    :param tier str: iceberg namespace matching the volume tier    
    :param tablename str: iceberg table name
    """

    if not os.path.exists(f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{tier}/{tablename}"): # table not created yet
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

    if not os.path.exists(f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{tier}/{tablename}"): # table not created yet
        ui.notify(f"Table not found: {tier}/{tablename}", type="warning")
        return

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        df = iceberger.tail(tier=tier, tablename=tablename)
        ui.table.from_pandas(df).classes('w-full mt-6').props("dense")

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


def peek_documents(tablepath: str):
    """
    Get `FETCH_RECORD_NUM` documents from DocumentDB table

    :param tablepath str: full path for the JSON table
    """

    if not os.path.lexists(f"/edfs/{get_cluster_name()}{tablepath}"): # table not created yet
        ui.notify(f"Table not found: {tablepath}", type="warning")
        return

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        docs = tables.get_documents(table_path=tablepath, limit=FETCH_RECORD_NUM)
        ui.table.from_pandas(pd.DataFrame.from_dict(docs)).classes('w-full mt-6').props("dense")
        
    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


async def create_golden():

    app.storage.user["busy"] = True
    
    (msg, sev) = await run.io_bound(data_aggregation)

    ui.notify(msg, type=sev)
    
    app.storage.user["busy"] = False


def data_aggregation():
    """
    Create golden data lake with consolidated customer and transaction information.

    Code generated by ChatGPT - 4o (June 2024)

    """

    # inputs from "Silver" tier
    profile_input_table = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['silver']}/{DATA_DOMAIN['tables']['profiles']}"
    customers_input_table = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['silver']}/{DATA_DOMAIN['tables']['customers']}"
    transactions_input_table = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['silver']}/{DATA_DOMAIN['tables']['transactions']}"

    for input_file in [profile_input_table, customers_input_table, transactions_input_table]:
        if not os.path.lexists(f"/edfs/{get_cluster_name()}{input_file}"): # table not created yet
            return (f"Input table not found: {input_file}", "warning")

    # output table for combined data
    combined_table = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['gold']}/{DATA_DOMAIN['tables']['combined']}"

    profiles_df = pd.DataFrame.from_dict(tables.get_documents(table_path=profile_input_table, limit=None))
    customers_df = pd.DataFrame.from_dict(tables.get_documents(table_path=customers_input_table, limit=None))
    transactions_df = pd.DataFrame.from_dict(tables.get_documents(table_path=transactions_input_table, limit=None))

    # merge customers with profiles on _id
    merged_df = pd.merge(customers_df, profiles_df, on="_id", how="left")
    
    # Group transactions by sender_account and receiver_account
    sent_transactions = transactions_df.groupby('sender_account').agg(list).reset_index()
    received_transactions = transactions_df.groupby('receiver_account').agg(list).reset_index()

    # Merge the transactions with the merged customer-profile data
    merged_df = pd.merge(merged_df, sent_transactions, left_on='account_number', right_on='sender_account', how='left', suffixes=('', '_sent'))
    merged_df = pd.merge(merged_df, received_transactions, left_on='account_number', right_on='receiver_account', how='left', suffixes=('', '_received'))

    # Fill NaN with empty lists for transactions
    merged_df['amount_sent'] = merged_df['amount'].apply(lambda x: [] if pd.isna(x) else x)
    merged_df['transaction_date_sent'] = merged_df['transaction_date'].apply(lambda x: [] if pd.isna(x) else x)
    merged_df['receiver_account'] = merged_df['receiver_account'].apply(lambda x: [] if pd.isna(x) else x)
    merged_df['amount_received'] = merged_df['amount_received'].apply(lambda x: [] if pd.isna(x) else x)
    merged_df['transaction_date_received'] = merged_df['transaction_date_received'].apply(lambda x: [] if pd.isna(x) else x)
    merged_df['sender_account'] = merged_df['sender_account'].apply(lambda x: [] if pd.isna(x) else x)
    # update score from nan to None
    merged_df['score'] = merged_df['score'].apply(lambda x: "" if pd.isna(x) else x)

    # Create a combined JSON structure
    def create_combined_json(row):
        return {
            '_id': row['_id'],
            'name': json.dumps(row['name']),
            'address': json.dumps(row['address']),
            'account_number': row['account_number'],
            'score': row['score'],
            'transactions_sent': [{'amount': a, 'transaction_date': t, 'receiver_account': r} for a, t, r in zip(row['amount_sent'], row['transaction_date_sent'], row['receiver_account'])],
            'transactions_received': [{'amount': a, 'transaction_date': t, 'sender_account': s} for a, t, s in zip(row['amount_received'], row['transaction_date_received'], row['sender_account'])]
        }

    # Apply the function to the DataFrame
    combined_data = merged_df.apply(create_combined_json, axis=1).tolist()
    print(combined_data)

    # Insert combined data into new JSON table
    if tables.upsert_documents(table_path=combined_table, docs=combined_data):
        logger.info("Created golden table with %d records", len(combined_data))
        return (f"Created golden table with {len(combined_data)} records", 'positive')
    else:
        logger.warning("Failed to write golden table at: %s", combined_table)
        return (f"Failed to write golden table at: {combined_table}", 'negative')


def reporting():
    not_implemented()
    # Reports / Dashboards
    # Profiles
        # Statistics (mean, std, min, percentiles, max)
        # top 10 by score
    # profile_aggregate = df_profiles.describe()
    # profile_top_10_by_score = df_profiles.sort_values("score", ascending = False).head(10)

    # Customers
        # Statistics (mean, std, min, percentiles, max)
        # top 10 spenders
        # top 10 receivers
        # monthly spend per customer
        # monthly recieved per customer
        # top 10 txn (send + recieve)
    # customers_aggregate = df_customers.describe()

    # Transactions
        # Statistics (mean, std, min, percentiles, max)
        # txn per month
        # amount per month
    # transactions_aggregate = df_transactions.describe()

