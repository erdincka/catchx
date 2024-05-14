import inspect
import logging
import os
import timeit
from faker import Faker
from nicegui import app, run, ui

from helpers import *
import restrunner
from streams import consume, produce
from tables import search_documents, upsert_document

logger = logging.getLogger()


async def run_step(step, pager: ui.stepper):
    app.storage.user["busy"] = True

    if step["runner"] == "rest":
        url = str(step["runner_target"])

        if step.get("use_demo_input", None):
            op = step["use_demo_input"]

            if op == "append":
                url += app.storage.user[f"input__{DEMO['name'].replace(' ', '_')}"]

            elif op == "replace":
                url = url.replace(
                    "PLACEHOLDER",
                    app.storage.user[f"input__{DEMO['name'].replace(' ', '_')}"],
                )
                logger.debug("URL %s", url)

            elif op == "prepend":
                url = app.storage.user[f"input__{DEMO['name'].replace(' ', '_')}"] + url

            else:
                logger.warning("Unknown input operation %s", op)

        response = await run.io_bound(restrunner.post, url)

        if response is None:
            ui.notify("No response", type="negative")

        elif response.ok:
            resjson = response.json()
            logger.debug("DEBUG RESTRUNNER RETURN %s", resjson)
            # I took the lazy approach here since different rest calls have different return formats (except the 'status')
            ui.notify(
                resjson, type="positive" if resjson["status"] == "OK" else "warning"
            )

            if resjson["status"] == "OK":
                pager.next()

        else:  # http error returned
            logger.warning("REST HTTP ERROR %s", response.text)
            ui.notify(message=response.text, html=True, type="warning")

    # elif step["runner"] == "restfile":
    #     for response in await run.io_bound(
    #         restrunner.postfile, DEMOS, step["runner_target"]
    #     ):
    #         if isinstance(response, Exception):
    #             ui.notify(response, type="negative")
    #         elif response.ok:
    #             try:
    #                 # logger.debug("DEBUG: RESPONSE FROM RESTRUNNER: %s", response)
    #                 if response.status_code == 201:
    #                     ui.notify("Folder created")
    #                 else:
    #                     resjson = response.json()
    #                     ui.notify(resjson)
    #             except Exception as error:
    #                 logger.debug("RESTFILE ERROR: %s", error)

    #             pager.next()
    #         else:  # http error returned
    #             ui.notify(message=response.text, html=True, type="warning")

    elif step["runner"] == "app":
        func = getattr(__import__(__name__), step["runner_target"])

        if "count" in step.keys():
            app.storage.general["ui"]["counting"] = 0
            # keep user selected count in sync
            count = app.storage.user[
                f"count__{DEMO['name'].replace(' ', '_')}__{step['id']}"
            ]

            # add progressbar if 'count'ing
            ui.linear_progress().bind_value_from(
                app.storage.general["ui"],
                "counting",
                backward=lambda x: f"{100 * int(x/count)} %",
            )

            if inspect.isgeneratorfunction(func):
                for response in await run.io_bound(
                    func, step.get("runner_parameters", None), count
                ):
                    logger.info(response)
            else:
                await run.io_bound(func, step.get("runner_parameters", None), count)

        else:
            if inspect.isgeneratorfunction(func):
                for response in await run.io_bound(
                    func, step.get("runner_parameters", None)
                ):
                    logger.info(response)

            else:
                await run.io_bound(func, step.get("runner_parameters", None))

        # pager.next()

    else:
        ui.notify(
            f"Would run {step['runner_target']} using {step['runner']} but it is not here yet!"
        )
        pager.next()

    app.storage.user["busy"] = False


def send_transactions(params: list, count: int):
    # params not used
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
    stream_path = f"{DEMO['volume']}/{DEMO['stream']}"

    logger.info("Sending %s messages to %s:%s", len(transactions), stream_path, DEMO['topic'])

    tic = timeit.default_timer()

    for msg in transactions:
        produce(stream_path, DEMO["topic"], json.dumps(msg))

    logger.info("It took %i seconds", timeit.default_timer() - tic)


def process_transactions(params: list):
    # params not used
    stream_path = f"{DEMO['volume']}/{DEMO['stream']}"
    table_path = f"{DEMO['volume']}/{DEMO['table']}"

    count = 0

    tic = timeit.default_timer()

    for record in consume(stream=stream_path, topic=DEMO['topic']):
        message = json.loads(record)

        profile = {
            "_id": str(message["transaction_date"]),
            "sender": message["sender_account"],
            "receiver": message["receiver_account"],
        }

        logger.debug("Update DB with %s", profile)

        if upsert_document(host=os.environ["MAPR_IP"], table=table_path, json_dict=profile):
            count += 1

    logger.info(
        f"Processed %s transactions in %i seconds", count, timeit.default_timer() - tic
    )


def detect_fraud(params: list, count: int):
    # params is not used
    table_path = f"{DEMO['volume']}/{DEMO['table']}"

    # just a simulation of query to the profiles table, 
    # if any doc is found with the number as their CHECK DIGITS in IBAN, we consider it as fraud!
    whereClause = {
        "$or":[
            {"$like":{"sender":f"GB{count}%"}},
            {"$like":{"receiver":f"GB{count}%"}}
        ]
    }

    for doc in search_documents(os.environ['MAPR_IP'], table_path, whereClause):

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
