import logging
from time import sleep
import timeit
from urllib.parse import quote
from faker import Faker
from nicegui import app

from helpers import *
from restrunner import *
from functions import *


logger = logging.getLogger("banking")


def send_transactions(params: list, count: int):
    # there should be single parameter (topic) for this function
    topic = params[0]

    fake = Faker("en_US")
    transactions = []
    for i in range(count):
        transactions.append(
            {
                "id": i + 1,
                "sender_account": fake.iban(),
                "receiver_account": fake.iban(),
                "amount": round(fake.pyint(0, 10_000), 2),
                "currency": "GBP",  # fake.currency_code(),
                "transaction_date": fake.past_datetime(start_date="-12M").timestamp(),
            }
        )

    # logger.debug("Creating monitoring table")
    stream_path = f"{VOLUME_PATH}/{STREAM}"

    logger.info("Sending %s messages to %s:%s", len(transactions), stream_path, topic)

    tic = timeit.default_timer()

    # stream_publish(
    #     stream=stream_path,
    #     topic=topic,
    #     messages=transactions,
    # )

    logger.info("It took %i seconds", timeit.default_timer() - tic)


def process_transactions(params: list):
    topic = params[0]
    stream_path = f"{VOLUME_PATH}/{STREAM}"
    table_path = f"{VOLUME_PATH}/{TABLE}"

    count = 0

    # ensure profileDB exists
    response = dagput(f"/api/v2/table/{quote(table_path)}")
    logger.debug("DB CREATE RESPONSE: %s", response)

    tic = timeit.default_timer()

    records = stream_consumer(
        stream=stream_path,
        topic=topic,
    )

    if records is None:
        return

    for message in records:

        profile = {
            "_id": str(message["transaction_date"]),
            "sender": message["sender_account"],
            "receiver": message["receiver_account"],
        }

        logger.info("Update DB with %s", profile)

        send_to_profileDB(profile)

        count += 1

    logger.info(
        f"Processed %s transactions in %i seconds", count, timeit.default_timer() - tic
    )


def send_to_profileDB(json_obj: str):
    table_path = f"{VOLUME_PATH}/{TABLE}"

    # logger.debug("Writing %s to %s", json_obj['_id'], table_path)

    response = restrunner.dagpost(
        f"/api/v2/table/{quote(table_path)}", json_obj=json_obj
    )
    logger.debug("DB POST RESPONSE: %s", response.text)


def detect_fraud(params: list, count: int):
    table_path = f"{VOLUME_PATH}/{TABLE}"

    response = restrunner.dagget(
        f"/api/v2/table/{quote(table_path, safe='')}?limit={count}"
    )

    if response is None:
        logger.warning("DB GET FAILED: %s", response)
        return

    records = response.json()
    # logger.info("DB GET RESPONSE: %s", records)

    for record in records["DocumentStream"]:
        # a dumb way to simulate a truth function by comparing accountIds
        logger.info(
            (
                "#%s is fraud"
                if record["sender"] > record["receiver"]
                else "safe transaction #%s"
            ),
            record["_id"],
        )
        sleep(0.2)
        app.storage.general["demo"]["counting"] += 1
        yield (
            f"FRAUD TXN from {record['sender']}"
            if record["sender"] > record["receiver"]
            else f"SAFE TXN between {record['sender']} and {record['receiver']}"
        )
