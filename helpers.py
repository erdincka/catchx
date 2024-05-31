import asyncio
import datetime
import json
import logging
import os
import tarfile

import httpx
import importlib_resources
from nicegui import ui, events, app, binding
from nicegui.events import ValueChangeEventArguments

APP_NAME = "catchX"
TITLE = "Fraud detection pipeline"
STORAGE_SECRET = "ezmer@1r0cks"

DEMO = json.loads(importlib_resources.files().joinpath("catchx.json").read_text())

MAX_POLL_TIME = 2.0
MON_REFRESH_INTERVAL = 1.0
MON_REFRESH_INTERVAL3 = 3.0

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

            elif "jwt_tokens" in filename:
                tf.extractall(path="/root")
            else:
                ui.notify(f"Unknown filename: {filename}", type="warning")
                return

            ui.notify(f"{filename} extracted: {','.join(tf.getnames())}", type="positive")

    except Exception as error:
        ui.notify(error, type="negative")


def update_clusters():
    with open("/opt/mapr/conf/mapr-clusters.conf", "r") as conf:
        # reset the clusters
        app.storage.general["clusters"] = {}
        for line in conf.readlines():
            t = line.split(' ')
            # dict { 'value1': 'name1' } formatted cluster list, compatible to ui.select options
            cls = { t[2].split(":")[0] : t[0] }
            app.storage.general["clusters"].update(cls)
    

async def run_command_with_dialog(command: str) -> None:
    """Run a command in the background and display the output in the pre-created dialog."""
    with ui.dialog().props("full-width v-model='cmd'") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-4 top-2")
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

    # stdout, stderr = await process.communicate()

    # if stdout:
    #     result.push(stdout.decode())
    # if stderr:
    #     result.push(stderr.decode())

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


async def command_to_log(command: str, uilog: ui.code):
    uilog.content = ""
    async for output in run_command(command):
        uilog.content += output


def get_cluster_name():
    return app.storage.general['clusters'].get(app.storage.general.get('cluster', ''), '')


async def create_volumes_and_stream():
    auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

    # create base folder if not exists
    basedir = f"/mapr/{get_cluster_name()}{DEMO['basedir']}"
    if not os.path.isdir(basedir):
        os.mkdir(basedir)

    for vol in DEMO['volumes']:

        URL = f"https://{app.storage.general['cluster']}:8443/rest/volume/create?name={DEMO['volumes'][vol]}&path={DEMO['basedir']}/{DEMO['volumes'][vol]}&replication=1&minreplication=1&nsreplication=1&nsminreplication=1"
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(URL, auth=auth)

            if response is None or response.status_code != 200:
                logger.warning(f"REST failed for create volume: %s", vol)
                logger.warning("Response: %s", response.text)

            else:
                res = response.json()
                if res['status'] == "OK":
                    ui.notify(f"{res['messages']}", type='positive')
                elif res['status'] == "ERROR":
                    ui.notify(f"{res['errors'][0]['desc']}", type='warning')

    # Create stream
    URL = f"https://{app.storage.general['cluster']}:8443/rest/stream/create?path={DEMO['basedir']}/{DEMO['stream']}&ttl=38400&compression=lz4&produceperm=p&consumeperm=p&topicperm=p"
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.post(URL, auth=auth)

        if response is None or response.status_code != 200:
            # possibly not an issue if stream already exists
            logger.warning(f"REST failed for create stream: %s", vol)
            logger.warning("Response: %s", response.text)

        else:
            res = response.json()
            if res['status'] == "OK":
                ui.notify(f"Stream {DEMO['stream']} created", type='positive')
            elif res['status'] == "ERROR":
                ui.notify(f"Stream: {DEMO['stream']}: {res['errors'][0]['desc']}", type='warning')


async def delete_volumes_and_stream():
    auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

    for vol in DEMO['volumes']:

        URL = f"https://{app.storage.general['cluster']}:8443/rest/volume/remove?name={DEMO['volumes'][vol]}"
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.post(URL, auth=auth)

            if response is None or response.status_code != 200:
                logger.warning("REST failed for delete volume: %s", vol)
                logger.warning("Response: %s", response.text)

            else:
                res = response.json()
                if res['status'] == "OK":
                    ui.notify(f"Volume {vol} deleted", type='positive')
                elif res['status'] == "ERROR":
                    ui.notify(f"{vol}: {res['errors'][0]['desc']}", type='warning')


    # Delete stream
    URL = f"https://{app.storage.general['cluster']}:8443/rest/stream/delete?path={DEMO['basedir']}/{DEMO['stream']}"
    async with httpx.AsyncClient(verify=False) as client:
        response = await client.post(URL, auth=auth)

        if response is None or response.status_code != 200:
            logger.warning(f"REST failed for delete stream: %s", vol)
            logger.warning("Response: %s", response.text)

        else:
            res = response.json()
            if res['status'] == "OK":
                ui.notify(f"Stream '{DEMO['stream']}' deleted", type='positive')
            elif res['status'] == "ERROR":
                ui.notify(f"Stream: {DEMO['stream']}: {res['errors'][0]['desc']}", type='warning')

    # delete mock data files
    for file in ["customers.csv", "transactions.csv"]:
        os.remove(f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{file}")

    # delete base folder
    basedir = f"/mapr/{get_cluster_name()}{DEMO['basedir']}"
    os.rmdir(basedir)


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

