import asyncio
import logging
import random
from faker import Faker
from nicegui import app, run
from nicegui.events import ValueChangeEventArguments

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
            await asyncio.sleep(0.05)

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


def toggle_debug(arg: ValueChangeEventArguments):
    print(f"debug set to {arg.value}")
    if arg.value:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


# Handle exceptions without UI failure
def gracefully_fail(exc: Exception):
    print("gracefully failing...")
    logger.exception(exc)
    app.storage.user["busy"] = False


def topic_stats(topic: str):
    stream_path = f"{DEMO['endpoints']['volume']}/{DEMO['endpoints']['stream']}"

    if app.storage.general.get("cluster", None) is None:
        logger.debug("Cluster not configured, skipping.")
        return
    
    try:
        response = restrunner.get(host=app.storage.general["cluster"],
            path=f"/rest/stream/topic/info?path={stream_path}&topic={topic}"
        )
        if response is None or response.status_code != 200:
            # possibly not connected or topic not populated yet, just ignore
            logger.debug(f"Failed to get topic stats for {topic}")

        else:
            metrics = response.json()
            if not metrics["status"] == "ERROR":
                logger.debug("TOPIC STAT %s", metrics)

                series = []
                for m in metrics["data"]:
                    series.append(
                        {"publishedMsgs": m["maxoffset"] + 1}
                    )  # interestingly, maxoffset starts from -1
                    series.append(
                        {"consumedMsgs": m["minoffsetacrossconsumers"]}
                    )  # this metric starts at 0
                    series.append(
                        {
                            "latestAgo(s)": (
                                datetime.now().astimezone()
                                - dt_from_iso(m["maxtimestamp"])
                            ).total_seconds()
                        }
                    )
                    series.append(
                        {
                            "consumerLag(s)": (
                                dt_from_iso(m["maxtimestamp"])
                                - dt_from_iso(m["mintimestampacrossconsumers"])
                            ).total_seconds()
                        }
                    )
                # logger.info("Metrics %s", series)
                return {
                    "name": topic,
                    "time": datetime.fromtimestamp(
                        metrics["timestamp"] / (10**3)
                    ).strftime("%H:%M:%S"),
                    "values": series,
                }
            else:
                logger.warn("Topic stat query error %s", metrics["errors"])

    except Exception as error:
        logger.warning("Topic stat request error %s", error)


def consumer_stats(topic: str):
    stream_path = f"{DEMO['endpoints']['volume']}/{DEMO['endpoints']['stream']}"

    if app.storage.general.get("cluster", None) is None:
        logger.debug("Cluster not configured, skipping.")
        return
    
    try:
        response = restrunner.get(host=app.storage.general["cluster"],
            path=f"/rest/stream/cursor/list?path={stream_path}&topic={topic}"
        )

        if response is None:
            # possibly not connected or topic not populated yet, just ignore
            logger.debug(f"Failed to get consumer stats for {topic}")

        else:
            metrics = response.json()

            if not metrics["status"] == "ERROR":
                logger.debug("CONSUMER STAT %s", metrics)
                series = []
                for m in metrics["data"]:
                    series.append(
                        {
                            f"{m['consumergroup']}_{m['partitionid']}_lag(s)": float(
                                m["consumerlagmillis"]
                            )
                            / 1000
                        }
                    )
                    series.append(
                        {
                            f"{m['consumergroup']}_{m['partitionid']}_offsetBehind": int(
                                m["produceroffset"]
                            ) + 1 # starting from -1 !!!
                            - int(m["committedoffset"])
                        }
                    )
                # logger.info("Metrics %s", series)
                return {
                    "name": "Consumer",
                    "time": datetime.fromtimestamp(
                        metrics["timestamp"] / (10**3)
                    ).strftime("%H:%M:%S"),
                    "values": series,
                }
            else:
                logger.warn("Consumer stat query error %s", metrics["errors"])

    except Exception as error:
        # possibly not connected or topic not populated yet, just ignore it
        logger.debug("Consumer stat request error %s", error)

