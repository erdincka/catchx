import json
import logging
from urllib.parse import quote, urlparse
from nicegui import app, run, ui

import restrunner

logger = logging.getLogger("functions")

APP_NAME = "catchX"

# DEMO = {
#     "name": "banking",
#     "description": "Here we would like to process transaction messages coming from different sources to detect fraud. For that, we would like to capture these messages as they arrive, process and apply our ML model to detect if that specific transaction is fradualent or not.  \n\nWe will be using Data Fabric Event Store pub/sub mechanism to submit transaction data into a topic, and another app/process that is consuming these transaction messages and generating a transaction profile and put this profile into Data Fabric Binary Table for quick referral. Finally the microservice that is checking all previous and related transactions with the profile placed in this table will respond to our app via topic creating a bi-directional messaging pipeline. \n\nOur pipeline here supports Ezmeral Data Fabric converged platform to store and serve data in both streaming and operational database engines.",
#     "image": "banking.jpg",
#     "link": "https://github.com/ezua-tutorials/df-banking-demo",
# }

DEMO = ""
# with open(importlib_resources.files("app").joinpath("banking.json"), "r") as f:
with open("banking.json", "r") as f:
    DEMO = json.load(f)


class LogElementHandler(logging.Handler):
    """A logging handler that emits messages to a log element."""

    def __init__(self, element: ui.log, level: int = logging.NOTSET) -> None:
        self.element = element
        super().__init__(level)

    def emit(self, record: logging.LogRecord) -> None:
        # change log format for UI
        self.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s: %(message)s",
                datefmt="%H:%M:%S",
            )
        )
        try:
            msg = self.format(record)
            self.element.push(msg)
        except Exception:
            self.handleError(record)


async def prepare():
    volume_path = DEMO["volume"]
    table_path = f"{volume_path}/{DEMO['table']}"
    stream_path = f"{volume_path}/{DEMO['stream']}"

    app.storage.user["busy"] = True

    host = app.storage.general["host"]

    logger.info("[ %s ] Create volume %s", host, volume_path)
    response = await run.io_bound(
        restrunner.post,
        host=host,
        path=f"/rest/volume/create?name={DEMO['name']}&path={DEMO['volume']}&replication=1&minreplication=1&nsreplication=1&nsminreplication=1",
    )
    if response:
        logger.debug(response.text)

    logger.info("[ %s ] Create table %s", host, table_path)
    table_path = quote(f'{DEMO["volume"]}/{DEMO["table"]}', safe='')
    response = await run.io_bound(
        restrunner.dagput,
        host=host,
        path=f"/api/v2/table/{table_path}",
    )
    if response and response.ok:
        logger.debug(response.text if len(response.text) > 0 else "%s created", DEMO['volume'])

    logger.info("[ %s ] Create stream %s", host, stream_path)
    stream_path = quote(stream_path, safe='')
    response = await run.io_bound(
        restrunner.post,
        host=host,
        path=f"/rest/stream/create?path={stream_path}&ttl=86400&compression=lz4",
    )
    if response:
        logger.debug(response.text)

    logger.info("Ready for demo")

    app.storage.user["busy"] = False


def toggle_log():
    app.storage.user["showlog"] = not app.storage.user["showlog"]


def gracefully_fail(exc: Exception):
    print("gracefully dying...")
    logger.debug("Exception %s", exc)
    app.storage.user["busy"] = False


async def stream_consumer(stream: str, topic: str):
    app.storage.user["busy"] = True

    topic_path = f"{stream}:{topic}"

    result = []

    try:
        # Create consumer instance
        response = await run.io_bound(
            restrunner.kafkapost,
            host=app.storage.general["hq"],
            path=f"/consumers/{topic}_cg",
            data={
                "name": f"{topic}_ci",
                "format": "json",
                "auto.offset.reset": "earliest",
                # "fetch.max.wait.ms": 1000,
                # "consumer.request.timeout.ms": "500",
            },
        )

        if response is None:
            return result

        ci = response.json()
        ci_path = urlparse(ci["base_uri"]).path

        # subscribe to consumer
        await run.io_bound(
            restrunner.kafkapost,
            host=app.storage.general["hq"],
            path=f"{ci_path}/subscription",
            data={"topics": [topic_path]},
        )
        # No content in response

        # get records
        records = await run.io_bound(
            restrunner.kafkaget,
            host=app.storage.general["hq"],
            path=f"{ci_path}/records",
        )
        if records and records.ok:
            for message in records.json():
                # logger.debug("CONSUMER GOT MESSAGE: %s", message)
                result.append(message["value"])

    except Exception as error:
        logger.warning("STREAM CONSUMER ERROR %s", error)

    finally:
        # Unsubscribe from consumer instance
        await run.io_bound(
            restrunner.kafkadelete,
            host=app.storage.general["hq"],
            path=f"/consumers/{topic}_cg/instances/{topic}_ci",
        )

        app.storage.user["busy"] = False

        return result
