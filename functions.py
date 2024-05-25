import asyncio
import random
from faker import Faker
from nicegui import app, run

from helpers import *
from streams import consume, produce
from tables import search_documents, upsert_document


fake = Faker("en_US")


def fake_transaction():
    return {
        "id": get_uuid_key(),
        "sender_account": fake.iban(),
        "receiver_account": fake.iban(),
        "amount": round(fake.pyint(0, 10_000), 2),
        "currency": "GBP",
        "transaction_date": fake.past_datetime(start_date="-12M").timestamp(),
    }


def publish_transaction(txn: dict):
    stream_path = f"{DEMO['volumes']['bronze']}/{DEMO['stream']}"

    if produce(stream_path, DEMO['topic'], json.dumps(txn)):
        logger.debug("Sent %s", txn["id"])
    else:
        logger.warning("Failed to send message to %s", DEMO['topic'])


async def transaction_feed_service():

    if not app.storage.general.get("txn_feed_svc", False):
        # enable
        app.storage.general["txn_feed_svc"] = True
        # start
        while app.storage.general["txn_feed_svc"]:
            await run.io_bound(publish_transaction, fake_transaction())
            # add delay
            # await asyncio.sleep(0.05)

    else:
        # disable
        app.storage.general["txn_feed_svc"] = False


async def ingest_transactions():

    stream_path = f"{DEMO['volumes']['bronze']}/{DEMO['stream']}"
    
    for record in await run.io_bound(consume, stream=stream_path, topic=DEMO['topic']):
        message = json.loads(record)
        
        # Write into iceberg for Bronze tier (raw data)
        # iceberger.write(message)
        # then update the profile
        await create_update_profile(message)
    

async def create_update_profile(message: dict):

    profile = {
        "s": message["sender_account"],
        "r": message["receiver_account"],
        "score": await dummy_fraud_score()
    }

    table_path = f"{DEMO['volumes']['bronze']}/{DEMO['table']}"

    if upsert_document(host=app.storage.general["cluster"], table_path=table_path, json_dict=profile):
        logger.debug("Profile written")


async def dummy_fraud_score():
    # introduce delay for calculation
    await asyncio.sleep(0.05)
    # respond with a probability
    return random.randint(0, 10)


async def refine_transaction(message: dict):
    profile = {
        "_id": str(message["transaction_date"]),
        "sender": message["sender_account"],
        "receiver": message["receiver_account"],
    }

    table_path = f"{DEMO['volumes']['bronze']}/{DEMO['table']}"
    return upsert_document(host=app.storage.general['cluster'], table=table_path, json_dict=profile)


async def customer_data_ingestion():
    await run.io_bound(print, "will run airflow")


def detect_fraud(params: list, count: int):
    # params is not used
    table_path = f"{DEMO['volumes']['bronze']}/{DEMO['table']}"

    # just a simulation of query to the profiles table, 
    # if any doc is found with the number as their CHECK DIGITS in IBAN, we consider it as fraud!
    whereClause = {
        "$or":[
            {"$like":{"sender":f"GB{count}%"}},
            {"$like":{"receiver":f"GB{count}%"}}
        ]
    }

    for doc in search_documents(app.storage.general.get('MAPR_IP', 'localhost'), table_path, whereClause):

        logger.debug("DB GET RESPONSE: %s", doc)

        app.storage.general["counting"] = app.storage.general.get("counting", 0) + 1
        yield f"Fraud txn from {doc['sender']} to {doc['receiver']}"
        
