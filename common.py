import asyncio
import datetime
import logging
import os
import tarfile
import inspect

import httpx
from nicegui import ui, events, app, binding
from nicegui.events import ValueChangeEventArguments
import pandas as pd
from sqlalchemy import create_engine, text


APP_NAME = "Data Fabric"
DIAGRAM = "/images/hubspoke.png"
TITLE = "Building a Hybrid Data Mesh"
STORAGE_SECRET = "ezmer@1r0cks"

DATA_PRODUCT = "fraud" # make this a single word, used for dir and database names, no spaces or fancy characters
BASEDIR = "/app"
MOUNT_PATH = "/mapr"
EXTERNAL_NFS_PATH = "/users" # TODO: make this user defined

VOLUME_BRONZE = "bronze"
VOLUME_SILVER = "silver"
VOLUME_GOLD = "gold"

STREAM_INCOMING = "incoming"
STREAM_CHANGELOG = "changelog"

TOPIC_TRANSACTIONS = "transactions"

TABLE_PROFILES = "profiles"
TABLE_TRANSACTIONS = "transactions"
TABLE_CUSTOMERS = "customers"
TABLE_FRAUD = "fraud_activity"

DATA_DOMAIN = {
#   TODO: describe
  "description": "What, why and how?",
  "diagram": "/images/frauddomain.png",
  "link": "https://github.com/erdincka/catchx"
}

MAX_POLL_TIME = 2.0
MON_REFRESH_INTERVAL = 1.0
MON_REFRESH_INTERVAL3 = 3.0
MON_REFRESH_INTERVAL5 = 5.0
MON_REFRESH_INTERVAL10 = 10.0
FETCH_RECORD_NUM = 15

TRANSACTION_CATEGORIES = [
    "Entertainment",
    "Shopping",
    "Education",
    "Investment",
    "Bills",
    "Transport",
    "Income",
    "Home",
    "Transfers",
    "Other"
]


HPE_COLORS = {
    "green": "#01A982",
    "purple": "#7630EA",
    "teal": "#00E8CF",
    "blue": "#00739D",
    "red": "#C54E4B",
    "orange": "#FF8300",
    "yellow": "#FEC901",
    "darkgreen": "#008567",
    "darkpurple": "#6633BC",
    "darkteal": "#117B82",
    "darkblue": "#00567A",
    "darkred": "#A2423D",
    "darkorange": "#9B6310",
    "darkyellow": "#8D741C",
}

DOCUMENTATION = {
    "Overview": """
        For this demo, we will be using an end to end data pipeline for a financial transaction workflow. 

        We are handling data ingestion from streaming and batch data sources, processing it through its lifecycle using a 'madallion architecture'.

        In a madallion architecture, we are using the bronze tier as the landing page where all the raw data is stored as soon as it arrives.
        
        Then we will apply our ETL processes to clean, enrich and filter data, so the next tier, silver tier, has the organisation's curated data
        with all the details and information ready to process.

        The final tier, gold tier, gives us a consolidated data lake that is used for reporting, trend analysis etc, but doesn't include any individual transaction or customer detail.

        Finally, we expose the data in our gold tier to reporting engines through standard connection/API endpoints to be consumed and shared.

    """,

    "Source Data Generation": """
        Let's start with generating some random data for our customers and their transactions. 
        
        We will create csv files that will act as our data sources that will be ingested as customers (batch) and transactions (stream).

    """
}

logger = logging.getLogger("common")


def dt_from_iso(timestring):
    """
    Convert ISO formatted timestamp to standard one
    """
    # Workaround since received timestring with " AM"/" PM" suffix is not parsed properly
    isPM = " PM" in timestring
    dt = datetime.datetime.strptime(timestring.replace(" AM", "").replace(" PM", ""), "%Y-%m-%dT%H:%M:%S.%f%z")
    return dt + datetime.timedelta(hours=12) if isPM else dt


def upload_client_files(e: events.UploadEventArguments):
    # possibly a security issue to use uploaded file names directly - don't care in demo/lab environment
    try:
        filename = e.name
        with open(f"/tmp/{filename}", "wb") as f:
            f.write(e.content.read())

        with tarfile.open(f"/tmp/{filename}", "r") as tf:
            if "conf" in filename:
                # For DF 7.7
                if "conf/mapr-clusters.conf" in tf.getnames():
                    tf.extract("conf/mapr-clusters.conf", path="/opt/mapr/")
                    tf.extract("conf/ssl_truststore", path="/opt/mapr/")
                    tf.extract("conf/ssl_truststore.pem", path="/opt/mapr/")
                # For DF 7.5
                else:
                    tf.extract("mapr-clusters.conf", path="/opt/mapr/conf/")
                    tf.extract("ssl_truststore", path="/opt/mapr/conf")

                # Refresh cluster list in UI
                update_clusters()
                ui.notify("Refresh page to see cluster(s)", type='info')

            elif "jwt" in filename:
                tf.extractall(path="/root")
            else:
                ui.notify(f"Unknown filename: {filename}", type="warning")
                return

            ui.notify(f"{filename} extracted: {','.join(tf.getnames())}", type="positive")

    except Exception as error:
        ui.notify(error, type="negative")


def update_clusters():
    try:
        with open("/opt/mapr/conf/mapr-clusters.conf", "r") as conf:
            # reset the clusters
            app.storage.general["clusters"] = {}
            for line in conf.readlines():
                t = line.split(' ')
                # dict { 'value1': 'name1' } formatted cluster list, compatible to ui.select options
                cls = { t[2].split(":")[0] : t[0] }
                app.storage.general["clusters"].update(cls)
            logger.info("Found clusters: %s", app.storage.general['clusters'])
            # select first cluster to avoid null value
            app.storage.general["cluster"] = next(iter(app.storage.general["clusters"]))
            logger.info("Set cluster: %s", app.storage.general['cluster'])
    except Exception as error:
        logger.warning("Failed to update clusters: %s", error)


async def run_command_with_dialog(command: str) -> None:
    """
    Run a command in the background and display the output in the pre-created dialog.
    """

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")
        ui.label(f"Running: {command}").classes("text-bold")
        result = ui.log().classes("w-full mt-2").style("white-space: pre-wrap")

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()

    result.content = ''

    async for out in run_command(command): result.push(out)


async def run_command(command: str):
    """
    Run a command in the background and return the output.
    """

    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        cwd=os.path.dirname(os.path.abspath(__file__))
    )

    # NOTE we need to read the output in chunks, otherwise the process will block
    while True:
        new = await process.stdout.read(4096)
        if not new:
            break
        yield new.decode()

    yield f"Finished: {command}"


def get_cluster_name():
    clustername = app.storage.general.get('clusters', {}).get(app.storage.general.get('cluster', ''), '')
    if clustername != "":
        return clustername
    else: return "maprdemo.mapr.io"


async def create_volumes():
    """
    Create an app folder and create volumes in it
    """

    auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

    app.storage.general['busy'] = True

    # create base folder if not exists
    basedir = f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}"
    if not os.path.isdir(basedir):
        os.mkdir(basedir)

    for vol in [VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD]:

        URL = f"https://{app.storage.general['cluster']}:8443/rest/volume/create?name={vol}&path={BASEDIR}/{vol}&replication=1&minreplication=1&nsreplication=1&nsminreplication=1"

        logger.debug("REST call to: %s", URL)

        try:
            async with httpx.AsyncClient(verify=False, timeout=10) as client:
                response = await client.post(URL, auth=auth)

                if response is None or response.status_code != 200:
                    logger.warning(f"REST failed for create volume: %s", vol)
                    logger.warning("Response: %s", response.text)

                else:
                    res = response.json()
                    if res['status'] == "OK":
                        ui.notify(f"{res['messages'][0]}", type='positive')
                    elif res['status'] == "ERROR":
                        ui.notify(f"{res['errors'][0]['desc']}", type='negative')

        except Exception as error:
            logger.warning("Failed to connect %s! Please manually set /opt/mapr/conf/mapr-clusters.conf file with the hostname/ip address of the apiserver!", URL)
            ui.notify(f"Failed to connect to REST. Please manually set /opt/mapr/conf/mapr-clusters.conf file with the hostname/ip address of the apiserver!", type='warning')
            return

    app.storage.general['busy'] = False


async def create_tables():
    auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

    app.storage.general['busy'] = True
    for tier in [VOLUME_BRONZE, VOLUME_SILVER]:
        try:
            # Create table
            async with httpx.AsyncClient(verify=False) as client:
                URL = f"https://{app.storage.general['cluster']}:8443/rest/table/create?path={BASEDIR}/{tier}/b{TABLE_TRANSACTIONS}&tabletype=binary&defaultreadperm=p&defaultwriteperm=p&defaultappendperm=p&defaultunmaskedreadperm=p"
                response = await client.post(
                    url=URL,
                    auth=auth
                )

                # logger.debug(response.json())

                if response is None or response.status_code != 200:
                    # possibly not an issue if table already exists
                    logger.warning("REST failed for create table: %s in %s", TABLE_TRANSACTIONS, tier)
                    logger.warning("Response: %s", response.text)

                else:
                    res = response.json()
                    if res['status'] == "OK":
                        ui.notify(f"Table \"b{TABLE_TRANSACTIONS}\" created in {tier}", type='positive')
                    elif res['status'] == "ERROR":
                        ui.notify(f"Table: \"{TABLE_TRANSACTIONS}\" in {tier}: {res['errors'][0]['desc']}", type='negative')

            # Create Column Family
            async with httpx.AsyncClient(verify=False) as client:
                URL = f"https://{app.storage.general['cluster']}:8443/rest/table/cf/create?path={BASEDIR}/{tier}/b{TABLE_TRANSACTIONS}&cfname=cf1"
                response = await client.post(
                    url=URL,
                    auth=auth
                )

                # logger.debug(response.json())

                if response is None or response.status_code != 200:
                    # possibly not an issue if table already exists
                    logger.warning("REST failed for create table: %s in %s", TABLE_TRANSACTIONS, tier)
                    logger.warning("Response: %s", response.text)

                else:
                    res = response.json()
                    if res['status'] == "OK":
                        ui.notify(f"Column Family created for table in {tier}", type='positive')
                    elif res['status'] == "ERROR":
                        ui.notify(f"Column Family failed for table in {tier}: {res['errors'][0]['desc']}", type='negative')

        except Exception as error:
            logger.warning("Failed to connect %s: %s", URL, error)
            ui.notify(f"Failed to connect to REST: {type(error)}", type='negative')
            return

        finally:
            app.storage.general['busy'] = False

    # TODO: build MySQL DB tables
    # Create RDBMS tables
    # mydb = f"mysql+pymysql://{app.storage.general['MYSQL_USER']}:{app.storage.general['MYSQL_PASS']}@{app.storage.general['cluster']}/{DATA_PRODUCT}"


async def create_streams():
    auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

    for stream in [STREAM_INCOMING, STREAM_CHANGELOG]:
        URL = f"https://{app.storage.general['cluster']}:8443/rest/stream/create?path={BASEDIR}/{stream}&ttl=38400&compression=lz4&produceperm=p&consumeperm=p&topicperm=p"

        # ensure changelog stream is enabled for cdc
        if stream == STREAM_CHANGELOG:
            URL += "&ischangelog=true&defaultpartitions=1"

        app.storage.general['busy'] = True
        try:
            async with httpx.AsyncClient(verify=False) as client:
                response = await client.post(URL, auth=auth)

                if response is None or response.status_code != 200:
                    # possibly not an issue if stream already exists
                    logger.warning(f"REST failed for create stream: %s", stream)
                    logger.warning("Response: %s", response.text)

                else:
                    res = response.json()
                    if res['status'] == "OK":
                        ui.notify(f"Stream \"{stream}\" created", type='positive')
                    elif res['status'] == "ERROR":
                        ui.notify(f"Stream: \"{stream}\": {res['errors'][0]['desc']}", type='negative')

        except Exception as error:
            logger.warning("Failed to connect %s: %s", URL, type(error))
            ui.notify(f"Failed to connect to REST: {error}", type='negative')
            return

        finally:
            app.storage.general['busy'] = False


def show_mysql_tables():
    mydb = f"mysql+pymysql://{app.storage.general['MYSQL_USER']}:{app.storage.general['MYSQL_PASS']}@{app.storage.general['cluster']}/{DATA_PRODUCT}"
    engine = create_engine(mydb)
    with engine.connect() as conn:
        tables = conn.execute(text("SHOW TABLES"))
        peek_tables = {}
        for table in tables:
            # peek_tables[table[0]] = pd.read_sql(f"SELECT * FROM {table[0]} LIMIT 5", con=mydb)
            peek_tables[table[0]] = pd.read_sql(f"SELECT * FROM {table[0]} LIMIT 10", con=mydb)
            logger.debug("%s: %s", table[0], peek_tables[table[0]])

        with ui.dialog().props("full-width") as mysql_tables, ui.card().classes("grow relative"):
            ui.button(icon="close", on_click=mysql_tables.close).props("flat round dense").classes("absolute right-2 top-2")

            with ui.row().classes("w-full mt-6"):
                ui.label("Tables from MySQL DB")
            for table in peek_tables.keys():
                ui.table.from_pandas(peek_tables[table], title=table).classes('w-full mt-6').props("dense")

        mysql_tables.open()

# This is not used due to complexity of its setup
# requires gateway node configuration and DNS modification
async def enable_cdc(source_table_path: str, destination_stream_topic: str):
    auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

    if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{source_table_path}"):
        ui.notify(f"Table not found: {source_table_path}", type="warning")
        return

    logger.info("Check for changelog on: %s", source_table_path)

    URL = f"https://{app.storage.general['cluster']}:8443/rest/table/changelog/list?path={source_table_path}&changelog={destination_stream_topic}"

    try:
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(URL, auth=auth)

            if response is None or response.status_code != 200:
                logger.warning("REST failed to check table changelog for: %s", source_table_path)
                logger.warning("Response: %s", response.text)

            else:
                res = response.json()
                if res["status"] == "ERROR":
                    logger.warning("CDC check failed with: %s", res["errors"])

                logger.info("CDC check: %s", res)

                if res["total"] == 0:
                    # create CDC
                    URL = f"https://{app.storage.general['cluster']}:8443/rest/table/changelog/add?path={source_table_path}&changelog={destination_stream_topic}&useexistingtopic=true"

                    response = await client.get(URL, auth=auth)

                    if response is None or response.status_code != 200:
                        logger.warning("REST failed to add changlog for: %s on %s", source_table_path, destination_stream_topic)
                        logger.warning("Response: %s", response.text)

                    else:
                        res = response.json()
                        if res["status"] == "ERROR":
                            logger.warning("CDC add failed with: %s", res["errors"])

                    logger.info(response.text)

    except Exception as error:
        logger.warning(error)


def configure_logging():
    """
    Set up logging and supress third party errors
    """

    logging.basicConfig(level=logging.DEBUG,
                    format="%(asctime)s:%(levelname)s:%(name)s (%(funcName)s:%(lineno)d): %(message)s",
                    datefmt='%H:%M:%S')

    # during development
    logger.setLevel(logging.DEBUG)

    # INSECURE REQUESTS ARE OK in Lab
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logging.getLogger("httpx").setLevel(logging.WARNING)

    logging.getLogger("requests").setLevel(logging.WARNING)

    logging.getLogger("watchfiles").setLevel(logging.FATAL)

    logging.getLogger("faker").setLevel(logging.FATAL)

    logging.getLogger("pyiceberg.io").setLevel(logging.WARNING)

    logging.getLogger("mapr.ojai.storage.OJAIConnection").setLevel(logging.WARNING)
    logging.getLogger("mapr.ojai.storage.OJAIDocumentStore").setLevel(logging.WARNING)

    # https://sam.hooke.me/note/2023/10/nicegui-binding-propagation-warning/
    binding.MAX_PROPAGATION_TIME = 0.05


def toggle_log(arg: ValueChangeEventArguments):
    print(f"debug set to {arg.value}")
    if arg.value:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


# Handle exceptions without UI failure
def gracefully_fail(exc: Exception):
    print("gracefully failing...")
    logger.exception(exc)
    app.storage.general["busy"] = False


def not_implemented():
    ui.notify('Not implemented', type='warning')


async def open_dialog(content_function):
    with ui.dialog().props("full-width full-height") as dialog, ui.card().classes(
        "relative"
    ):
        ui.button(icon="close", on_click=dialog.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        if inspect.iscoroutinefunction(content_function):
            await content_function()
        else: content_function()

    dialog.open()


class LogElementHandler(logging.Handler):
    """A logging handler that emits messages to a log element."""

    def __init__(self, element: ui.log, level: int = logging.DEBUG) -> None:
        self.element = element
        super().__init__(level)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
            self.element.push(msg)
        except Exception:
            self.handleError(record)
