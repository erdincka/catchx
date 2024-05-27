import inspect
import logging
from nicegui import ui, app

from functions import *
from helpers import *
from monitoring import *


def app_init():
    # Reset services

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
            ui.button("Cluster", on_click=cluster_configuration_dialog).props("outline color=white")
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
            ui.label(f"Volumes: {' | '.join( DEMO['volumes'].values() )}").classes("tracking-wide uppercase")
            ui.label(f"Tables: {' | '.join( DEMO['tables'] )}").classes("tracking-wide uppercase")
            ui.label(f"Stream: { DEMO['stream'] }").classes("tracking-wide uppercase")
            ui.label(f"Topic: { DEMO['topic'] }").classes("tracking-wide uppercase")

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
        ).on("click", handler=lambda x: print(x)) # TODO: open image in sidebar when clicked
        ui.link(
            "Source",
            target=DEMO.get("link", ""),
            new_tab=True,
        ).bind_visibility_from(DEMO, "link", backward=lambda x: x is not None)


def demo_steps():
    with ui.list().props("bordered").classes("w-2/3"):

        with ui.expansion("Ingestion", caption="Streaming and batch data ingestion", group="flow"):
            with ui.expansion("Producer Functions", caption="Source code for running the stream ingestion", group="ingest").classes("w-full"):
                ui.code(inspect.getsource(fake_transaction)).classes("w-full")
                ui.code(inspect.getsource(publish_transaction)).classes("w-full")
                ui.code(inspect.getsource(produce)).classes("w-full")
            with ui.row():
                ui.button("Start Streaming", on_click=transaction_feed_service).props("outline")

            ui.separator()        
            with ui.expansion("Batch Functions", caption="Source code for running the batch ingestion", group="ingest").classes("w-full"):
                ui.code(inspect.getsource(customer_data_ingestion)).classes("w-full")
            with ui.row():
                ui.button("Start Batch", on_click=customer_data_ingestion).props("outline")
                ui.button("Check Data", on_click=customer_data_list).props("outline")
            
        with ui.expansion("ETL", caption="Realtime processing for incoming data", group="flow"):
            with ui.expansion("Code", caption="Source code for running the ETL tasks").classes("w-full"):
                ui.code(inspect.getsource(ingest_transactions)).classes("w-full")
                ui.code(inspect.getsource(consume)).classes("w-full")
                ui.code(inspect.getsource(create_update_profile)).classes("w-full")
                ui.code(inspect.getsource(upsert_document)).classes("w-full")
            ui.separator()
            with ui.row():
                ui.button("Start ETL", on_click=ingest_transactions).props("outline").bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                ui.button("Check Iceberg ", on_click=ingest_transactions).props("outline").bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)

        with ui.expansion("Cleaning", caption="Integrate with data catalogue and clean/enrich data into silver tier", group="flow"):
            with ui.expansion("Code", caption="Source code for running the Cleaning tasks").classes("w-full"):
                ui.code(inspect.getsource(refine_transaction)).classes("w-full")
            ui.separator()
            with ui.row():
                ui.button("Start Cleaning").props("outline").bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)


def monitoring_charts():
    # Monitoring charts
    with ui.card().classes("flex-grow shrink sticky top-0"):
        topic_chart = get_echart()
        topic_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        ui.timer(MON_REFRESH_INTERVAL, lambda c=topic_chart: update_chart(c, topic_stats))

        consumer_chart = get_echart()
        consumer_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        ui.timer(MON_REFRESH_INTERVAL, lambda c=consumer_chart: update_chart(c, consumer_stats))
