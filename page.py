import inspect
from nicegui import ui, app

from functions import *
from helpers import *
from ingestion import *
from mock import *
from monitoring import *
import streams
import tables


def app_init():
    # Reset services

    # and previous run state if it was hang
    app.storage.user["busy"] = False

    # reset the cluster info
    if "clusters" not in app.storage.general:
        app.storage.general["clusters"] = {}

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
            ui.link(target=f"https://{app.storage.general.get('cluster', 'localhost')}:8443/app/mcs", new_tab=True).bind_text_from(app.storage.general, "cluster", backward=lambda x: app.storage.general["clusters"].get(x, "localhost") if x else "None").classes("text-white hover:text-blue-600")

        ui.space()
        ui.switch(on_change=toggle_debug).tooltip("Debug").props("color=dark keep-color")


def footer():
    with ui.footer():
        with ui.row().classes("w-full items-center"):
            # Show endpoints
            ui.label(f"Volumes: [ {' | '.join( DEMO['volumes'].values() )} ]").classes("tracking-wide uppercase")
            ui.label(f"Tables: [ {' | '.join( DEMO['tables'] )} ]").classes("tracking-wide uppercase")
            ui.label(f"Stream: [ { DEMO['stream'] } ]").classes("tracking-wide uppercase")
            ui.label(f"Topic: [ { DEMO['topic'] } ]").classes("tracking-wide uppercase")

            ui.space()

            ui.spinner("ios", size="2em", color="red").bind_visibility_from(
                app.storage.user, "busy"
            ).tooltip("Busy")

            ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                app.storage.user, "busy", lambda x: not x
            ).tooltip("Ready")


def info():
    with ui.expansion( 
        TITLE,
        icon="info",
        caption="End to end pipeline processing using Ezmeral Data Fabric",
    ).classes("w-full").classes("text-bold"):
        ui.markdown(DEMO["description"]).classes("font-normal")
        ui.image(f"/images/{DEMO['diagram']}").classes("object-scale-down g-10").on("click", handler=lambda x: print(x)) # TODO: open image in sidebar when clicked
        ui.link(
            "Source",
            target=DEMO.get("link", ""),
            new_tab=True,
        ).bind_visibility_from(DEMO, "link", backward=lambda x: x is not None)


def demo_steps():
    with ui.list().props("bordered").classes("w-2/3"):

        with ui.expansion("Generation", caption="Prepare mock data for ingestion", group="flow"):
            with ui.expansion("Create Data", caption="Source code for data generation", group="generate").classes("w-full"):
                ui.code(inspect.getsource(create_csv_files)).classes("w-full mt-6")
                ui.code(inspect.getsource(fake_customer)).classes("w-full")
                ui.code(inspect.getsource(fake_transaction)).classes("w-full")

            with ui.row().classes("w-full place-items-center"):
                ui.button("Create", on_click=create_csv_files)
                ui.button("Peek Data", on_click=peek_demo_data).props("outline")
                ui.button("Into S3", color='warning', on_click=not_implemented).props('outline')

            with ui.expansion("Publish", caption="Source code for Kafka producer", group="generate").classes("w-full"):
                ui.code(inspect.getsource(publish_transactions)).classes("w-full")
                ui.code(inspect.getsource(streams.produce)).classes("w-full")

            with ui.row():
                ui.button("Publish", on_click=publish_transactions)

        with ui.expansion("Ingestion & ETL Processing", caption="Realtime processing on incoming data", group="flow"):
            with ui.expansion("Batch", caption="Source code for batch processing", group="ingest").classes("w-full"):
                ui.code(inspect.getsource(ingest_customers)).classes("w-full")
                ui.code(inspect.getsource(iceberger.write)).classes("w-full")

            with ui.row():
                ui.button("Batch", on_click=ingest_customers)
                ui.button("History", on_click=customer_data_list).props("outline")

            with ui.expansion("Stream", caption="Source code for consuming streaming data", group="ingest").classes("w-full"):
                ui.code(inspect.getsource(ingest_transactions)).classes("w-full")
                ui.code(inspect.getsource(streams.consume)).classes("w-full")
                ui.code(inspect.getsource(upsert_profile)).classes("w-full")
                ui.code(inspect.getsource(tables.upsert_document)).classes("w-full")

            with ui.row():
                ui.button("Stream", on_click=ingest_transactions).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                ui.button("Using Airflow", color='warning', on_click=not_implemented).props("outline").bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
            
        with ui.expansion("Enrich & Clean", caption="Integrate with data catalogue and clean/enrich data into silver tier", group="flow"):
            with ui.expansion("Code", caption="Source code for running the Cleaning tasks").classes("w-full"):
                ui.code(inspect.getsource(refine_transaction)).classes("w-full")

            with ui.row():
                ui.button("Create Silver", on_click=not_implemented).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)

        with ui.expansion("Consolidate", caption="Create data lake for gold tier", group="flow"):
            with ui.expansion("Code", caption="Source code for aggregation").classes("w-full"):
                ui.code(inspect.getsource(not_implemented)).classes("w-full")

            with ui.row():
                ui.button("Create Golden", on_click=not_implemented)


def monitoring_charts():
    # Monitoring charts
    with ui.card().classes("flex-grow shrink sticky top-0"):
        topic_chart = get_echart()
        topic_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        ui.timer(MON_REFRESH_INTERVAL, lambda c=topic_chart: update_chart(c, topic_stats))
        
        # consumer_chart = get_echart()
        # consumer_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        # ui.timer(MON_REFRESH_INTERVAL, lambda c=consumer_chart: update_chart(c, consumer_stats))

        # with ui.card_section().classes("w-full"):
        #     ui.label("Bronze tier")
        #     bronze_chart = get_echart()
        #     bronze_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        #     ui.timer(MON_REFRESH_INTERVAL3, lambda c=bronze_chart: update_chart(c, table_stats, "bronze"))


def cluster_configuration_dialog():
    with ui.dialog().props(f"position=right full-height") as dialog, ui.card().classes("relative bordered"):
        # with close button
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-4 top-2")

        with ui.card_section().classes("w-full mt-6"):
            ui.label("Client files").classes("text-lg w-full")
            ui.label("config.tar and/or jwt_tokens.tar.gz").classes("text-subtitle2")
            ui.upload(label="Upload", on_upload=upload_client_files, multiple=True, auto_upload=True, max_files=2).props("accept='application/x-tar,application/x-gzip' hide-upload-btn").classes("w-full")

        ui.separator()
        with ui.card_section():
            with ui.row().classes("w-full"):
                ui.label("Select cluster").classes("text-lg")
                ui.space()
                ui.button(icon="refresh", on_click=update_clusters).props("flat")
            ui.toggle(app.storage.general["clusters"]).bind_value(app.storage.general, "cluster")

        ui.separator()
        with ui.card_section():
            ui.label("Local user credentials").classes("text-lg w-full")
            ui.label("required for REST API and monitoring").classes("text-subtitle2")
            with ui.row().classes("w-full place-items-center"):
                ui.input("Username").bind_value(app.storage.general, "MAPR_USER")
                ui.input("Password", password=True, password_toggle_button=True).bind_value(app.storage.general, "MAPR_PASS")

        ui.separator()
        with ui.card_section():
            ui.label("Configure and login").classes("text-lg w-full")
            ui.label("login if not using JWT").classes("text-subtitle2")
            with ui.row().classes("w-full place-items-center"):
                ui.button("configure.sh -R", on_click=lambda: run_command_with_dialog("/opt/mapr/server/configure.sh -R"))
                ui.button("maprlogin", on_click=lambda: run_command_with_dialog(f"echo {app.storage.general['MAPR_PASS']} | maprlogin password -user {app.storage.general['MAPR_USER']}"))
                ui.button("remount /mapr", on_click=lambda: run_command_with_dialog(f"umount -l /mapr; mount -t nfs {app.storage.general['cluster']}:/mapr /mapr"))

        ui.separator()
        with ui.card_section():
            ui.label("Create the volumes and the stream").classes("text-lg w-full")
            ui.label("required constructs for the demo")
            with ui.row().classes("w-full place-items-center"):
                ui.button("Create", on_click=create_volumes_and_stream)
                ui.button(f"List {DEMO['basedir']}", on_click=lambda: run_command_with_dialog(f"ls -la /mapr/{get_cluster_name()}{DEMO['basedir']}")).props('outline')

        ui.separator()
        with ui.card_section():
            ui.label("Show volumes").classes("text-lg w-full")
            with ui.row().classes("w-full place-items-center"):
                for vol in DEMO['volumes']:
                    ui.button(f"List {vol}", on_click=lambda v=vol: run_command_with_dialog(f"find /mapr/{get_cluster_name()}{DEMO['basedir']}/{DEMO['volumes'][v]}")).props('outline')

        ui.separator()
        with ui.card_section():
            ui.label("Delete All!").classes("text-lg")
            ui.label("Will remove all volumes and the stream, all will be gone!").classes("text-subtitle2")
            ui.button("DELETE ALL!", on_click=delete_volumes_and_stream, color="negative")

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()

