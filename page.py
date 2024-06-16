import inspect
from nicegui import ui, app

from functions import *
from common import *
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
            ui.button("Data Hub", on_click=cluster_configuration_dialog).props("outline color=white")
            ui.link(target=f"https://{app.storage.general.get('MAPR_USER', 'mapr')}:{app.storage.general.get('MAPR_PASS', 'mapr123')}@{app.storage.general.get('cluster', 'localhost')}:8443/app/mcs", new_tab=True).bind_text_from(app.storage.general, "cluster", backward=lambda x: app.storage.general["clusters"].get(x, "localhost") if x else "None").classes("text-white hover:text-blue-600")


def footer():
    with ui.footer():

        with ui.row().classes("w-full items-center"):

            # Endpoints
            ui.label("Volumes:")

            with ui.button_group().props('flat color=dark'):
                # GNS
                ui.button("GNS", on_click=lambda: run_command_with_dialog(f"df -h {MOUNT_PATH}; ls -lA {MOUNT_PATH}"))
                # App folder
                ui.button("Data Domain", on_click=lambda: run_command_with_dialog(f"ls -lA {MOUNT_PATH}{get_cluster_name()}{BASEDIR}"))
                # Volumes
                ui.button(VOLUME_BRONZE, on_click=lambda: run_command_with_dialog(f"ls -lAR {MOUNT_PATH}{get_cluster_name()}{BASEDIR}/{VOLUME_BRONZE}"))
                ui.button(VOLUME_SILVER, on_click=lambda: run_command_with_dialog(f"ls -lAR {MOUNT_PATH}{get_cluster_name()}{BASEDIR}/{VOLUME_SILVER}"))
                ui.button(VOLUME_GOLD, on_click=show_mysql_tables)
                # ui.button(VOLUME_GOLD, on_click=lambda: run_command_with_dialog(f"ls -lAR {MOUNT_PATH}{get_cluster_name()}{BASEDIR}/{VOLUME_GOLD}"))

            ui.space()

            # ui.button("CDC", on_click=lambda: enable_cdc(source_table_path=f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}", destination_stream_topic=f"{BASEDIR}/{STREAM_MONITORING}:{TOPIC_TRANSACTIONS}"))
            ui.switch("Lights on!", on_change=lights_on).props("flat outline color=dark keep-color").bind_value(app.storage.general, "lightson")
            # ui.space()

            ui.switch(on_change=toggle_debug).tooltip("Debug").props("color=dark keep-color")

            ui.spinner("ios", size="2em", color="red").bind_visibility_from(
                app.storage.user, "busy"
            ).tooltip("Busy")

            ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                app.storage.user, "busy", lambda x: not x
            ).tooltip("Ready")


def demo_steps():
    with ui.list().props("bordered").classes("w-2/3"):

        with ui.expansion("Generation", caption="Prepare and publish mocked data into the pipeline", group="flow", value=True):
            with ui.dialog().props("full-width") as generate_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=generate_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(create_csv_files)).classes("w-full mt-6")
                ui.code(inspect.getsource(fake_customer)).classes("w-full")
                ui.code(inspect.getsource(fake_transaction)).classes("w-full")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Create CSVs: ").classes("w-40")
                ui.button("Create", on_click=create_csv_files)
                ui.button("Peek Data", on_click=peek_mocked_data).props("outline")
                ui.button("Into S3", color='warning', on_click=not_implemented).props('outline').bind_visibility_from(app.storage.general, "S3_SECRET_KEY")
                ui.button("Show Bucket", color='warning', on_click=not_implemented).props('outline').bind_visibility_from(app.storage.general, "S3_SECRET_KEY")
                ui.button("Code", on_click=generate_dialog.open, color="info").props("outline")

            with ui.dialog().props("full-width") as publish_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=publish_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(publish_transactions)).classes("w-full mt-6")
                ui.code(inspect.getsource(streams.produce)).classes("w-full")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Publish to Kafka stream: ").classes("w-40")
                ui.button("Publish", on_click=publish_transactions).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                ui.button("Code", on_click=publish_dialog.open, color="info").props("outline")

        with ui.expansion("Ingestion & ETL Processing (Bronze)", caption="Realtime processing on incoming data", group="flow"):
            with ui.dialog().props("full-width") as batch_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=batch_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(ingest_customers_iceberg)).classes("w-full mt-6")
                ui.code(inspect.getsource(iceberger.write)).classes("w-full")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Batch ingestion: ").classes("w-40")
                ui.button("Customers", on_click=ingest_customers_iceberg).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                ui.button("History", on_click=lambda: iceberg_table_history(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS)).props("outline")
                ui.button("Tail", on_click=lambda: iceberg_table_tail(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS)).props("outline")
                ui.button("Code", on_click=batch_dialog.open, color="info").props("outline")

            with ui.dialog().props("full-width") as stream_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=stream_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(ingest_transactions)).classes("w-full mt-6")
                ui.code(inspect.getsource(streams.consume)).classes("w-full")
                ui.code(inspect.getsource(upsert_profile)).classes("w-full")
                ui.code(inspect.getsource(tables.upsert_document)).classes("w-full")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Stream ingestion: ").classes("w-40")
                ui.button("Transactions", on_click=ingest_transactions).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                ui.button("Peek", on_click=lambda: peek_documents(tablepath=f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}")).props("outline")
                ui.button("Using Airflow", color='warning', on_click=not_implemented).props("outline").bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                ui.button("Code", on_click=stream_dialog.open, color="info").props("outline")

        with ui.expansion("Enrich & Clean (Silver)", caption="Integrate with data catalogue and clean/enrich data into silver tier", group="flow"):
            with ui.dialog().props("full-width") as enrich_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=enrich_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(refine_customers)).classes("w-full mt-6")
                ui.code(inspect.getsource(refine_transactions)).classes("w-full")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Data enrichment: ").classes("w-40")
                ui.button("Customers", on_click=lambda: run.io_bound(refine_customers)).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                ui.button("Transactions", on_click=refine_transactions).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                ui.button("Code", on_click=enrich_dialog.open, color="info").props("outline")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Peek: ").classes("w-40")
                ui.button("Profiles", on_click=lambda: peek_documents(tablepath=f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}")).props("outline")
                ui.button("Customers", on_click=lambda: peek_documents(tablepath=f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}")).props("outline")
                ui.button("Transactions", on_click=lambda: peek_documents(f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}")).props("outline")

        with ui.expansion("Consolidate (Gold)", caption="Create data lake for gold tier", group="flow"):
            with ui.dialog().props("full-width") as aggregate_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=aggregate_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(data_aggregation)).classes("w-full mt-6")
                ui.code(inspect.getsource(tables.get_documents)).classes("w-full mt-6")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Data aggregation: ").classes("w-40")
                ui.button("Summarize", on_click=create_golden).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x).bind_visibility_from(app.storage.general, 'MYSQL_PASS', backward=lambda x: x is not None and len(x) > 0)
                ui.button("Code", on_click=aggregate_dialog.open, color="info").props("outline")

        with ui.expansion("Check transactions for Fraud", caption="Process every transaction and check for fraud", group="flow"):
            with ui.dialog().props("full-width") as fraud_detection_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=fraud_detection_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(fraud_detection)).classes("w-full mt-6")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Run Fraud Detection against AI app: ").classes("w-40")
                ui.button("Run", on_click=fraud_detection)
                ui.button("Code", on_click=fraud_detection_dialog.open, color="info").props("outline")


async def monitoring_charts():
    # Monitoring charts
    with ui.card().classes("flex-grow shrink sticky top-0"):
        ui.label("Realtime Visibility").classes("uppercase")

        # # monitor using /var/mapr/mapr.monitoring/metricstreams/0
        # streams_chart = get_echart()
        # streams_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        # ui.timer(MON_REFRESH_INTERVAL, lambda c=streams_chart, s=mapr_monitoring: chart_listener(c, s), once=True)

        incoming_chart = get_echart()
        incoming_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        ui.timer(MON_REFRESH_INTERVAL3, lambda c=incoming_chart: update_chart(c, incoming_topic_stats))
        
        # TODO: embed grafana dashboard
        # https://10.1.1.31:3000/d/pUfMqVUIz/demo-monitoring?orgId=1

        # consumer_chart = get_echart()
        # consumer_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        # ui.timer(MON_REFRESH_INTERVAL3, lambda c=consumer_chart: update_chart(c, txn_consumer_stats))

        bronze_chart = get_echart()
        bronze_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        ui.timer(MON_REFRESH_INTERVAL10, lambda c=bronze_chart: update_chart(c, bronze_stats))

        silver_chart = get_echart()
        silver_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        await asyncio.sleep(3.0) # delayed start
        ui.timer(MON_REFRESH_INTERVAL10, lambda c=silver_chart: update_chart(c, silver_stats))

        gold_chart = get_echart()
        gold_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
        await asyncio.sleep(3.0) # delayed start
        ui.timer(2 * MON_REFRESH_INTERVAL10 + 5, lambda c=gold_chart: update_chart(c, gold_stats))


def cluster_configuration_dialog():
    with ui.dialog().props("position=right full-height") as dialog, ui.card().classes("relative bordered"):
        # with close button
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        with ui.card_section().classes("w-full mt-6"):
            ui.label("Upload Client Files").classes("text-lg w-full")
            ui.label("config.tar and/or jwt_tokens.tar.gz").classes("text-sm text-italic")
            ui.upload(label="Upload", on_upload=upload_client_files, multiple=True, auto_upload=True, max_files=2).props("accept='application/x-tar,application/x-gzip' hide-upload-btn").classes("w-full")

        ui.separator()
        with ui.card_section():
            with ui.row().classes("w-full place-items-center mt-4"):
                ui.label("Select The Hub").classes("text-lg")
                ui.button(icon="refresh", on_click=update_clusters).props("flat round")
            ui.toggle(app.storage.general["clusters"]).bind_value(app.storage.general, "cluster")

        ui.separator()
        with ui.card_section():
            ui.label("User Credentials").classes("text-lg w-full")
            ui.label("required for REST API and monitoring").classes("text-sm text-italic")
            with ui.row().classes("w-full place-items-center"):
                ui.input("Username").bind_value(app.storage.general, "MAPR_USER")
                ui.input("Password", password=True, password_toggle_button=True).bind_value(app.storage.general, "MAPR_PASS")

            ui.space()

            ui.label("S3 Credentials").classes("text-lg w-full mt-4")
            ui.label("for iceberg and spark").classes("text-sm text-italic")
            with ui.row().classes("w-full place-items-center"):
                ui.input("Access Key").bind_value(app.storage.general, "S3_ACCESS_KEY")
                ui.input("Secret Key", password=True, password_toggle_button=True).bind_value(app.storage.general, "S3_SECRET_KEY")

            ui.space()
            with ui.dialog() as mysql_user_dialog, ui.card():
                mysql_user_script = f"""
                Using MySQL admin user, run these commands to create the database and the user:
                ```
                CREATE DATABASE {DATA_PRODUCT};
                USE {DATA_PRODUCT}
                CREATE USER 'catchx'@'%' IDENTIFIED BY 'catchx';
                GRANT ALL ON {DATA_PRODUCT}.* TO 'catchx'@'%' WITH GRANT OPTION;
                FLUSH PRIVILEGES;
                ```
                """
                ui.code(content=mysql_user_script, language="shell")

            with ui.row().classes("w-full place-items-center mt-4"):
                ui.label("MySQL Credentials").classes("text-lg")
                ui.button(icon="info", on_click=mysql_user_dialog.open).props("flat round")
            ui.label("for gold tier RDBMS").classes("text-sm text-italic")
            with ui.row().classes("w-full place-items-center mt-4"):
                ui.input("Username").bind_value(app.storage.general, "MYSQL_USER")
                ui.input("Password", password=True, password_toggle_button=True).bind_value(app.storage.general, "MYSQL_PASS")

        ui.separator()
        with ui.card_section():
            ui.label("Configure and Login").classes("text-lg w-full")
            ui.label("login if not using JWT").classes("text-sm text-italic")
            with ui.row().classes("w-full place-items-center mt-4"):
                ui.button("configure.sh -R", on_click=lambda: run_command_with_dialog("/opt/mapr/server/configure.sh -R"))
                ui.button("maprlogin", on_click=lambda: run_command_with_dialog(f"echo {app.storage.general['MAPR_PASS']} | maprlogin password -user {app.storage.general['MAPR_USER']}"))
            with ui.row().classes("w-full place-items-center mt-4"):
                ui.button(f"remount {MOUNT_PATH}", on_click=lambda: run_command_with_dialog(f"[ -d {MOUNT_PATH} ] && umount -l {MOUNT_PATH}; [ -d {MOUNT_PATH} ] || mkdir {MOUNT_PATH}; mount -t nfs -o nolock,soft {app.storage.general['cluster']}:/mapr {MOUNT_PATH}"))
                ui.button("List Cluster /", on_click=lambda: run_command_with_dialog(f"ls -la {MOUNT_PATH}{get_cluster_name()}")).props('outline')

        ui.separator()
        with ui.card_section():
            ui.label("Create the Entities").classes("text-lg w-full")
            ui.label("required volumes and streams")
            with ui.row().classes("w-full place-items-center mt-4"):
                ui.button("Volumes", on_click=create_volumes).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                ui.button("Streams", on_click=create_streams).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)

        ui.separator()
        with ui.card_section():
            ui.label("Clean up!").classes("text-lg")
            ui.label("Use when done with the demo. This will remove all volumes and streams, ALL DATA will be gone!").classes("text-sm text-italic")
            ui.button("DELETE ALL!", on_click=delete_volumes_and_streams, color="negative").classes("mt-4").bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


async def lights_on():

    if create_csv_files():
        while app.storage.general.get("lightson", False):

            app.storage.general["lightson"] = True

            await publish_transactions(limit=random.randrange(5, 10))

            await ingest_customers_iceberg()

            await ingest_transactions()

            await refine_customers()

            await refine_transactions()

            await create_golden()

            await fraud_detection()

            await asyncio.sleep(MON_REFRESH_INTERVAL10)
