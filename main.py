import inspect
import logging
import os

import requests
from nicegui import app, ui, binding

from functions import *


TITLE = "Data Pipeline for Fraud"
STORAGE_SECRET = "ezmer@1r0cks"

# catch-all exceptions
app.on_exception(gracefully_fail)

# Set up logging and third party errors
logging.basicConfig(level=logging.INFO,
                format="%(asctime)s:%(levelname)s:%(module)s (%(funcName)s): %(message)s",
                datefmt='%H:%M:%S')

logger = logging.getLogger()

# INSECURE REQUESTS ARE OK in Lab
requests.packages.urllib3.disable_warnings()
urllib_logger = logging.getLogger("urllib3.connectionpool")
urllib_logger.setLevel(logging.WARNING)

requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.WARNING)

watcher_logger = logging.getLogger("watchfiles.main")
watcher_logger.setLevel(logging.FATAL)

faker_log = logging.getLogger("faker.factory")
faker_log.setLevel(logging.FATAL)

# https://sam.hooke.me/note/2023/10/nicegui-binding-propagation-warning/
binding.MAX_PROPAGATION_TIME = 0.05


@ui.page("/")
async def home():
    # Reset service
    app.storage.general["txn_feed_svc"] = False

    # and previous run state if it was hang
    app.storage.user["busy"] = False

    # and log window
    app.storage.user["showlog"] = False

    # reset the cluster info
    if "clusters" not in app.storage.general:
        app.storage.general["clusters"] = {}

    # reset monitor refresh
    # app.storage.user["refresh_interval"] = MON_REFRESH_INTERVAL

    # If user is not set, get from environment
    if "MAPR_USER" not in app.storage.general:
        app.storage.general["MAPR_USER"] = os.environ.get("MAPR_USER", "")
        app.storage.general["MAPR_PASS"] = os.environ.get("MAPR_PASS", "")

    # Header
    with ui.header(elevated=True).classes('items-center justify-between uppercase'):
        ui.label(f"{APP_NAME}: {TITLE}")
        ui.space()

        with ui.row().classes("place-items-center"):
            ui.button("Cluster", on_click=configure_cluster).props("outline color=white")
            ui.link(target=f"https://{app.storage.general.get('cluster', 'localhost')}:8443/", new_tab=True).bind_text_from(app.storage.general, "cluster", backward=lambda x: app.storage.general["clusters"][x] if x else "None").classes("text-white hover:text-blue-600")

        ui.space()
        ui.switch(on_change=toggle_debug).bind_value(app.storage.user, "debugging").tooltip("Debug")

    # Documentation / Intro
    with ui.expansion( 
        TITLE,
        icon="info",
        caption="End to end pipeline processing using Ezmeral Data Fabric",
    ).classes("w-full").classes("text-bold").bind_value(app.storage.general.get("ui", {}), "info"):
        ui.markdown(DEMO["description"]).classes("font-normal")
        ui.image(importlib_resources.files("main").joinpath(DEMO["diagram"])).classes(
            "object-scale-down g-10"
        )
        ui.link(
            "Source",
            target=DEMO.get("link", ""),
            new_tab=True,
        ).bind_visibility_from(DEMO, "link", backward=lambda x: x is not None)

    ui.separator()

    with ui.row().classes("w-full flex flex-nowrap"):
        with ui.list().props("bordered").classes("w-2/3"):
            with ui.expansion("Data Ingestion", caption="Streaming and batch data ingestion", group="flow", value=True):
                ui.code(inspect.getsource(produce)).classes("w-full")
                ui.separator()
                with ui.row():
                    ui.button(on_click=transaction_feed_service).bind_text_from(app.storage.general, "txn_feed_svc", backward=lambda x: "Stop" if x else "Stream").props("flat")
                    ui.space()
                    ui.button("Batch", on_click=customer_data_ingestion).props("flat")
            with ui.expansion("ETL", caption="Realtime processing for incoming data", group="flow"):
                ui.code(inspect.getsource(ingest_transactions)).classes("w-full")
                ui.code(inspect.getsource(consume)).classes("w-full")
                ui.separator()
                with ui.row():
                    ui.button("Consume", on_click=ingest_transactions).props("flat")
        
        # Monitoring charts
        with ui.card().classes("flex-grow shrink"):

            refresh_interval = ui.slider(min=1, max=30, value=3.0).props("label label-always switch-label-side")
            ui.space()
        
            topic_chart = get_echart()
            consumer_chart = get_echart()

            ui.timer(refresh_interval.value, lambda: add_measurement(topic_stats(DEMO["endpoints"]["topic"]), topic_chart))
            ui.timer(refresh_interval.value, lambda: add_measurement(consumer_stats(DEMO["endpoints"]["topic"]), consumer_chart))


    with ui.footer() as footer:
        with ui.row().classes("w-full items-center"):
            ui.button(icon="menu", on_click=toggle_log).props("flat text-color=white")
            ui.label("Log")

            ui.space()

            # Show endpoints
            ui.label(" | ".join([f"{k.upper()}: {v}" for k,v in DEMO["endpoints"].items()])).classes("tracking-wide")

            ui.space()

            ui.spinner("ios", size="2em", color="red").bind_visibility_from(
                app.storage.user, "busy"
            ).tooltip("Busy")

            ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                app.storage.user, "busy", lambda x: not x
            ).tooltip("Ready")

        log = (
            ui.log()
            .classes("w-full h-48 bg-neutral-300/30")
            .style("white-space: pre-wrap")
            .bind_visibility(app.storage.user, "showlog")
        )
        logger.addHandler(LogElementHandler(log, level=logging.INFO))


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title=TITLE,
        dark=None,
        storage_secret=STORAGE_SECRET,
        reload=True,
        port=3000,
    )

