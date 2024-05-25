import inspect
import logging
from nicegui import ui, app

from chart import get_echart
from functions import *
from helpers import *


logger = logging.getLogger()


def app_init():
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


def header():
    with ui.header(elevated=True).classes('items-center justify-between uppercase'):
        ui.label(f"{APP_NAME}: {TITLE}")
        ui.space()

        with ui.row().classes("place-items-center"):
            ui.button("Cluster", on_click=configure_cluster).props("outline color=white")
            ui.link(target=f"https://{app.storage.general.get('cluster', 'localhost')}:8443/", new_tab=True).bind_text_from(app.storage.general, "cluster", backward=lambda x: app.storage.general["clusters"][x] if x else "None").classes("text-white hover:text-blue-600")

        ui.space()
        ui.switch(on_change=toggle_debug).tooltip("Debug").props("color=dark keep-color")


def footer():
    with ui.footer():
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


def info():
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


def demo_steps():
    with ui.row().classes("w-full flex flex-nowrap"):
        with ui.list().props("bordered").classes("w-2/3"):

            with ui.expansion("Data Ingestion", caption="Streaming and batch data ingestion", group="flow", value=True):
                ui.code(inspect.getsource(fake_transaction)).classes("w-full")
                ui.code(inspect.getsource(publish_transaction)).classes("w-full")
                ui.code(inspect.getsource(produce)).classes("w-full")
                ui.separator()
                with ui.row():
                    ui.button(on_click=transaction_feed_service).bind_text_from(app.storage.general, "txn_feed_svc", backward=lambda x: "Stop" if x else "Stream").props("outline")
                    ui.space()
                    ui.button("Batch", on_click=customer_data_ingestion).props("outline")
            
            with ui.expansion("ETL", caption="Realtime processing for incoming data", group="flow"):
                ui.code(inspect.getsource(ingest_transactions)).classes("w-full")
                ui.code(inspect.getsource(consume)).classes("w-full")
                ui.separator()
                with ui.row():
                    ui.button("Consume", on_click=ingest_transactions).props("outline")

        # Monitoring charts
        with ui.card().classes("flex-grow shrink"):
            topic_chart = get_echart().run_chart_method('showLoading')
            
            # consumer_chart = get_echart().run_chart_method('showLoading')

            # ui.timer(MON_REFRESH_INTERVAL, lambda: add_measurement(topic_stats(DEMO["endpoints"]["topic"]), topic_chart))
            # ui.timer(MON_REFRESH_INTERVAL, lambda: add_measurement(consumer_stats(DEMO["endpoints"]["topic"]), consumer_chart))


