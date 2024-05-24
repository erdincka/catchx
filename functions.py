import asyncio
import logging
import random
import timeit
from faker import Faker
from nicegui import app, run

from apprunner import consumer_stats, topic_stats
from elements import add_measurement, get_echart
from helpers import *
from streams import consume, produce
from tables import search_documents, upsert_document

logger = logging.getLogger()

fake = Faker("en_US")


def fake_transaction():
    return {
        "id": datetime.datetime.timestamp(datetime.datetime.now()),
        "sender_account": fake.iban(),
        "receiver_account": fake.iban(),
        "amount": round(fake.pyint(0, 10_000), 2),
        "currency": "GBP",  # fake.currency_code(),
        "transaction_date": fake.past_datetime(start_date="-12M").timestamp(),
        "merchant": random.randrange(10**11, 10**12)
    }


def publish_transaction(txn: dict):
    stream_path = f"{DEMO['endpoints']['volume']}/{DEMO['endpoints']['stream']}"

    if produce(stream_path, DEMO['endpoints']['topic'], json.dumps(txn)):
        logger.debug("Sent %s", txn["id"])
    else:
        logger.warning("Failed to send message to %s", DEMO['endpoints']['topic'])


async def transaction_feed_service():

    if not app.storage.general.get("txn_feed_svc", False):
        # enable
        app.storage.general["txn_feed_svc"] = True
        # start
        while app.storage.general["txn_feed_svc"]:
            await run.io_bound(publish_transaction, fake_transaction())
            # add delay
            await asyncio.sleep(0.5)

    else:
        # disable
        app.storage.general["txn_feed_svc"] = False


async def ingest_transactions():

    stream_path = f"{DEMO['endpoints']['volume']}/{DEMO['endpoints']['stream']}"

    for record in await run.io_bound(consume, stream=stream_path, topic=DEMO['endpoints']['topic']):
        message = json.loads(record)
        
        logger.debug("Write into iceberg %s", message)
        # iceberger.write(message)


async def refine_transaction(message: dict):
    profile = {
        "_id": str(message["transaction_date"]),
        "sender": message["sender_account"],
        "receiver": message["receiver_account"],
    }

    table_path = f"{DEMO['endpoints']['volume']}/{DEMO['endpoints']['table']}"
    return upsert_document(host=app.storage.general['cluster'], table=table_path, json_dict=profile)


async def customer_data_ingestion():
    await run.io_bound(print, "will run airflow")


def detect_fraud(params: list, count: int):
    # params is not used
    table_path = f"{DEMO['endpoints']['volume']}/{DEMO['endpoints']['table']}"

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
        

def toggle_log():
    app.storage.user["showlog"] = not app.storage.user["showlog"]


def toggle_debug(val: bool):
    if val:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


# Handle exceptions without UI failure
def gracefully_fail(exc: Exception):
    print("gracefully failing...")
    logger.exception(exc)
    app.storage.user["busy"] = False


# async def stream_consumer(stream: str, topic: str):
#     app.storage.user["busy"] = True

#     topic_path = f"{stream}:{topic}"

#     result = []

#     try:
#         # Create consumer instance
#         response = await run.io_bound(
#             restrunner.kafkapost,
#             host=app.storage.general["hq"],
#             path=f"/consumers/{topic}_cg",
#             data={
#                 "name": f"{topic}_ci",
#                 "format": "json",
#                 "auto.offset.reset": "earliest",
#                 # "fetch.max.wait.ms": 1000,
#                 # "consumer.request.timeout.ms": "500",
#             },
#         )

#         if response is None:
#             return result

#         ci = response.json()
#         ci_path = urlparse(ci["base_uri"]).path

#         # subscribe to consumer
#         await run.io_bound(
#             restrunner.kafkapost,
#             host=app.storage.general["hq"],
#             path=f"{ci_path}/subscription",
#             data={"topics": [topic_path]},
#         )
#         # No content in response

#         # get records
#         records = await run.io_bound(
#             restrunner.kafkaget,
#             host=app.storage.general["hq"],
#             path=f"{ci_path}/records",
#         )
#         if records and records.ok:
#             for message in records.json():
#                 # logger.debug("CONSUMER GOT MESSAGE: %s", message)
#                 result.append(message["value"])

#     except Exception as error:
#         logger.warning("STREAM CONSUMER ERROR %s", error)

#     finally:
#         # Unsubscribe from consumer instance
#         await run.io_bound(
#             restrunner.kafkadelete,
#             host=app.storage.general["hq"],
#             path=f"/consumers/{topic}_cg/instances/{topic}_ci",
#         )

#         app.storage.user["busy"] = False

#         return result
