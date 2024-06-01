import asyncio
import random
from nicegui import app

from helpers import *
import tables
import iceberger


async def upsert_profile(message: dict):
    """Assign customer ID and fraud score for the receiver account in transaction"""
    
    profile = {
        "_id": get_customer_id(message['receiver_account']),
        "score": await dummy_fraud_score()
    }

    table_path = f"{DEMO['basedir']}/{DEMO['volumes']['bronze']}/{DEMO['tables']['profiles']}"

    if tables.upsert_document(host=app.storage.general["cluster"], table_path=table_path, json_dict=profile):
        logger.debug("Updated profile: %s with score: %d", profile['_id'], profile['score'])


async def dummy_fraud_score():
    # introduce delay for calculation
    await asyncio.sleep(0.05)
    # respond with a probability
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
async def refine_transaction(message: dict):

    logger.info("Transaction cleanup")
    # input table - iceberg
    table_path = f"{DEMO['basedir']}/{DEMO['volumes']['bronze']}/{DEMO['tables']['profiles']}" 

    # output table - maprdb binary
    output_table = f"{DEMO['basedir']}/{DEMO['volumes']['silver']}/{DEMO['tables']['profiles']}"



def detect_fraud(params: list, count: int):
    # params is not used
    table_path = f"{DEMO['basedir']}/{DEMO['volumes']['bronze']}/{DEMO['tables']['profiles']}"

    # just a simulation of query to the profiles table, 
    # if any doc is found with the number as their CHECK DIGITS in IBAN, we consider it as fraud!
    whereClause = {
        "$or":[
            {"$like":{"sender":f"GB{count}%"}},
            {"$like":{"receiver":f"GB{count}%"}}
        ]
    }

    for doc in tables.search_documents(app.storage.general.get('MAPR_IP', 'localhost'), table_path, whereClause):

        logger.debug("DB GET RESPONSE: %s", doc)

        app.storage.general["counting"] = app.storage.general.get("counting", 0) + 1
        yield f"Fraud txn from {doc['sender']} to {doc['receiver']}"
        
