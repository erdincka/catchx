import asyncio
import random
from nicegui import app

from helpers import *
import tables


def open_airflow():
    pass


async def upsert_profile(message: dict):
    """
    assign customer ID and fraud score for the receiver account
    """
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


def get_customer_id(from_account: str):
    """
    find the customerID from customers table using account #
    """
    return ""  # TODO: get customer ID from customers table


async def refine_transaction(message: dict):

    logger.info("Transaction cleanup")
    table_path = f"{DEMO['basedir']}/{DEMO['volumes']['bronze']}/{DEMO['tables']['profiles']}"
    # iceberger.read(table_path, message['id'])


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
        
