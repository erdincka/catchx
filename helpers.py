import asyncio
import datetime
import json
import logging
import os
import re
import tarfile
from time import gmtime, strftime
import uuid

import importlib_resources
from nicegui import ui, events, app

APP_NAME = "catchX"

DEMO = json.loads(importlib_resources.files().joinpath("banking.json").read_text())

MAX_POLL_TIME = 5

class LogElementHandler(logging.Handler):
    """A logging handler that emits messages to a log element."""

    def __init__(self, element: ui.log, level: int = logging.NOTSET) -> None:
        self.element = element
        super().__init__(level)

    def emit(self, record: logging.LogRecord) -> None:
        # change log format for UI
        self.setFormatter(
            logging.Formatter(
                # "%(asctime)s %(levelname)s: %(message)s",
                # datefmt="%H:%M:%S",
                "%(message)s",
            )
        )
        try:
            # remove color formatting for ezfabricctl output
            ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
            msg = self.format(record)
            self.element.push(re.sub(ANSI_RE, "", msg))
        except Exception:
            self.handleError(record)


def dt_from_iso(timestring):
    """
    Convert ISO formatted timestamp to standard one
    """
    # Workaround since received timestring with " AM"/" PM" suffix is not parsed properly
    isPM = " PM" in timestring
    dt = datetime.datetime.strptime(timestring.replace(" AM", "").replace(" PM", ""), "%Y-%m-%dT%H:%M:%S.%f%z")
    return dt + datetime.timedelta(hours=12) if isPM else dt


def get_uuid_key():
    return '{0}_{1}'.format(strftime("%Y%m%d%H%M%S",gmtime()),uuid.uuid4())


# def is_configured():
#     """
#     Check if all client configuration files in place
#     """
#     files = ["/opt/mapr/conf/mapr-clusters.conf", "/opt/mapr/conf/ssl_truststore", "/opt/mapr/conf/ssl_truststore.pem", "/root/jwt_access", "/root/jwt_refresh"]
#     return all([os.path.isfile(f) for f in files])


def upload_client_files(e: events.UploadEventArguments):
    # possibly a security issue to use uploaded file names directly - don't care in demo/lab environment
    try:
        filename = e.name
        with open(f"/tmp/{filename}", "wb") as f:
            f.write(e.content.read())
        
        with tarfile.open(f"/tmp/{filename}", "r") as tf:
            if "config" in filename:
                tf.extractall(path="/opt/mapr")
                get_clusters()

            elif "jwt_tokens" in filename:
                tf.extractall(path="/root")
            else:
                ui.notify(f"Unknown filename: {filename}", type="warning")
                return

            ui.notify(f"{filename} extracted: {','.join(tf.getnames())}", type="positive")

    except Exception as error:
        ui.notify(error, type="negative")


def get_clusters():
    with open("/opt/mapr/conf/mapr-clusters.conf", "r") as conf:
        # reset the clusters
        app.storage.general["clusters"] = {}
        for line in conf.readlines():
            t = line.split(' ')
            # dict { 'value1': 'name1' } formatted cluster list, compatible to ui.select options
            cls = { t[2].split(":")[0] : t[0] }
            app.storage.general["clusters"].update(cls)


async def run_command(command: str) -> None:
    """Run a command in the background and display the output in the pre-created dialog."""
    with ui.dialog().props("full-width v-model='cmd") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-4 top-4")
        result = ui.log().classes("w-full").style("white-space: pre-wrap")

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

