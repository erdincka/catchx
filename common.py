import asyncio
import datetime
import logging
import os
import tarfile

import httpx
from nicegui import ui, events, app, binding
from nicegui.events import ValueChangeEventArguments

APP_NAME = "Data Fabric"
TITLE = "Building a Hybrid Data Mesh"
STORAGE_SECRET = "ezmer@1r0cks"

DATA_DOMAIN = {
  "name": "fraud",
#   TODO: describe
  "description": "What, why and how?",
  "diagram": "datadomain.png",
  "basedir": "/fraud",
  "volumes": {
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold"
  },
  "streams": {
    "incoming": "incoming",
    "monitoring": "monitoring"
  },
  "topics": {
    "transactions": "transactions",
    "cdc": "cdc"
  },
  "tables": {
    "profiles": "profiles",
    "transactions": "transactions",
    "customers": "customers",
    "combined": "combined"
  },
  "link": "https://github.com/erdincka/catchx"
}

MAX_POLL_TIME = 2.0
MON_REFRESH_INTERVAL = 1.0
MON_REFRESH_INTERVAL3 = 3.0
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

logger = logging.getLogger()


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
            if "config" in filename:
                tf.extractall(path="/opt/mapr")
                # Refresh cluster list in UI
                update_clusters()
                ui.navigate.reload()

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
            # select first cluster to avoid null value
            app.storage.general["cluster"] = next(iter(app.storage.general["clusters"]))
    except Exception as error:
        logger.warning("Failed to update clusters: %s", error)


async def run_command_with_dialog(command: str) -> None:
    """Run a command in the background and display the output in the pre-created dialog."""
    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")
        ui.label(f"Running: {command}").classes("text-bold")
        result = ui.log().classes("w-full mt-2").style("white-space: pre-wrap")

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()
    result.content = ''

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
        result.push(new.decode())

    result.push(f"Command completed.")


async def run_command(command: str):
    """Run a command in the background and return the output."""
    process = await asyncio.create_subprocess_shell(
        command,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        cwd=os.path.dirname(os.path.abspath(__file__))
    )

    while True:
        new = await process.stdout.read(4096)
        if not new:
            break
        yield new.decode()

    yield f"Finished: {command}"


# async def command_to_log(command: str, uilog: ui.code):
#     uilog.content = ""
#     async for output in run_command(command):
#         uilog.content += output


def get_cluster_name():
    return app.storage.general.get('clusters', {}).get(app.storage.general.get('cluster', ''), '')


async def create_volumes_and_streams():
    auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

    # create base folder if not exists
    basedir = f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}"
    if not os.path.isdir(basedir):
        os.mkdir(basedir)

    for vol in DATA_DOMAIN['volumes'].keys():

        URL = f"https://{app.storage.general['cluster']}:8443/rest/volume/create?name={DATA_DOMAIN['volumes'][vol]}&path={DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes'][vol]}&replication=1&minreplication=1&nsreplication=1&nsminreplication=1"
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(URL, auth=auth)

            if response is None or response.status_code != 200:
                logger.warning(f"REST failed for create volume: %s", vol)
                logger.warning("Response: %s", response.text)

            else:
                res = response.json()
                if res['status'] == "OK":
                    ui.notify(f"{res['messages'][0]}", type='positive')
                elif res['status'] == "ERROR":
                    ui.notify(f"{res['errors'][0]['desc']}", type='warning')

    # Create streams
    for stream in DATA_DOMAIN["streams"].keys():
        URL = f"https://{app.storage.general['cluster']}:8443/rest/stream/create?path={DATA_DOMAIN['basedir']}/{DATA_DOMAIN['streams'][stream]}&ttl=38400&compression=lz4&produceperm=p&consumeperm=p&topicperm=p"
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(URL, auth=auth)

            if response is None or response.status_code != 200:
                # possibly not an issue if stream already exists
                logger.warning(f"REST failed for create stream: %s", stream)
                logger.warning("Response: %s", response.text)

            else:
                res = response.json()
                if res['status'] == "OK":
                    ui.notify(f"Stream \"{DATA_DOMAIN['streams'][stream]}\" created", type='positive')
                elif res['status'] == "ERROR":
                    ui.notify(f"Stream: \"{DATA_DOMAIN['streams'][stream]}\": {res['errors'][0]['desc']}", type='warning')


async def delete_volumes_and_streams():
    auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

    for vol in DATA_DOMAIN['volumes'].keys():

        URL = f"https://{app.storage.general['cluster']}:8443/rest/volume/remove?name={DATA_DOMAIN['volumes'][vol]}"
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(URL, auth=auth)

            if response is None or response.status_code != 200:
                logger.warning("REST failed for delete volume: %s", vol)
                logger.warning("Response: %s", response.text)

            else:
                res = response.json()
                if res['status'] == "OK":
                    ui.notify(f"Volume '{vol}' deleted", type='positive')
                elif res['status'] == "ERROR":
                    ui.notify(f"{vol}: {res['errors'][0]['desc']}", type='warning')


    # Delete streams
    for stream in DATA_DOMAIN["streams"].keys():
        URL = f"https://{app.storage.general['cluster']}:8443/rest/stream/delete?path={DATA_DOMAIN['basedir']}/{DATA_DOMAIN['streams'][stream]}"
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(URL, auth=auth)

            if response is None or response.status_code != 200:
                logger.warning(f"REST failed for delete stream: %s", stream)
                logger.warning("Response: %s", response.text)

            else:
                res = response.json()
                if res['status'] == "OK":
                    ui.notify(f"Stream '{DATA_DOMAIN['streams'][stream]}' deleted", type='positive')
                elif res['status'] == "ERROR":
                    ui.notify(f"Stream: {DATA_DOMAIN['streams'][stream]}: {res['errors'][0]['desc']}", type='warning')

    # delete mock data files and iceberg catalog
    for file in ["customers.csv", "transactions.csv", "iceberg.db"]:
        os.remove(f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{file}")

    # delete base folder
    basedir = f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}"
    os.rmdir(basedir)


async def check_cdc():
    auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

    source_table_path = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['bronze']}/{DATA_DOMAIN['tables']['transactions']}"

    if not os.path.lexists(f"/edfs/{app.storage.general.get('cluster', '')}{source_table_path}"):
        ui.notify(f"Table not found: {source_table_path}", type="warning")
        return
    
    logger.info(source_table_path)

    # URL = "http://<ipaddress>:8443/rest/table/changelog/add?path=<source-table-path>&changelog=<destination stream path>:<topic name>"
    
    URL = f"http://{app.storage.general['cluster']}:8443/rest/table/changelog/list?path={source_table_path}"

    async with httpx.AsyncClient(verify=False) as client:
        response = await client.get(URL, auth=auth)

        if response is None or response.status_code != 200:
            logger.warning(f"REST failed to check table changlog for: %s", source_table_path)
            logger.warning("Response: %s", response.text)

        else:
            res = response.json()
            logger.info(res)


def set_logging():
    """
    Set up logging and supress third party errors
    """

    logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s:%(levelname)s:%(name)s (%(funcName)s): %(message)s",
                    datefmt='%H:%M:%S')

    # INSECURE REQUESTS ARE OK in Lab
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logging.getLogger("httpx").setLevel(logging.WARNING)

    logging.getLogger("requests").setLevel(logging.WARNING)
    
    logging.getLogger("watchfiles").setLevel(logging.FATAL)

    logging.getLogger("faker").setLevel(logging.FATAL)

    # https://sam.hooke.me/note/2023/10/nicegui-binding-propagation-warning/
    binding.MAX_PROPAGATION_TIME = 0.05


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


def not_implemented():
    ui.notify('Not implemented', type='warning')

