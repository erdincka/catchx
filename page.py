import inspect
import io
from fastapi.responses import StreamingResponse
from nicegui import ui, app

from functions import *
from common import *
import gui
from ingestion import *
from mock import *
from monitoring import *
import streams
import tables
from codeviewers import *


def header(title: str):
    with ui.header(elevated=True).classes("items-center justify-between uppercase py-1 px-4") as header:
        ui.button(icon="home", on_click=lambda: ui.navigate.to(index_page)).props("flat color=light")

        ui.label(title)

        ui.icon(None, size='lg').bind_name_from(app.storage.general, "demo_mode", backward=lambda x: "s_preview" if x else "s_preview_off").tooltip("Presentation Mode")

        ui.switch("Go Live").bind_value(app.storage.general, 'demo_mode')

        ui.switch("Monitor", on_change=lambda x: toggle_monitoring(x.value))

        ui.space()

        with ui.row().classes("place-items-center"):
            with ui.link(
                target=f"https://{app.storage.general.get('MAPR_USER', 'mapr')}:{app.storage.general.get('MAPR_PASS', 'mapr123')}@{app.storage.general.get('cluster', 'localhost')}:8443/app/mcs/",
                new_tab=True
            ).bind_text_from(
                app.storage.general,
                "cluster",
                backward=lambda x: (
                    app.storage.general["clusters"].get(x, "localhost") if x else "None"
                ),
            ).classes(
                "text-white hover:text-blue-600"
            ).bind_visibility_from(app.storage.general, "cluster", backward=lambda x: len(x) > 0):
                ui.icon("open_in_new")

            ui.label("Not configured!").classes("text-bold red").bind_visibility_from(app.storage.general, "cluster", backward=lambda x: len(x) == 0)

            ui.button(icon="settings", on_click=cluster_configuration_dialog).props(
                "flat color=light"
            )

            ui.icon("error", size="2em", color="red").bind_visibility_from(
                app.storage.general, "cluster", lambda x: len(x) == 0
            ).tooltip("Requires configuration!")

            with ui.element("div").bind_visibility_from(app.storage.general, "cluster", backward=lambda x: len(x) != 0):
                ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                    app.storage.general, "busy", lambda x: not x
                ).tooltip("Ready")
                ui.spinner("ios", size="2em", color="red").bind_visibility_from(
                    app.storage.general, "busy"
                ).tooltip("Busy")

    return header


def footer():
    with ui.footer().classes("py-1 px-4") as footer:
        with ui.row().classes("w-full items-center"):

            # Endpoints
            ui.label("Volumes:")

            with ui.button_group().props("flat color=dark"):
                # GNS
                ui.button(
                    "GNS",
                    on_click=lambda: run_command_with_dialog(
                        f"df -h {MOUNT_PATH}; ls -lA {MOUNT_PATH}"
                    ),
                )
                # App folder
                ui.button(
                    "Data Domain",
                    on_click=lambda: run_command_with_dialog(
                        f"ls -lA {MOUNT_PATH}/{get_cluster_name()}{BASEDIR}"
                    ),
                )
                # Volumes
                ui.button(
                    VOLUME_BRONZE,
                    on_click=lambda: run_command_with_dialog(
                        f"ls -lAR {MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{VOLUME_BRONZE}"
                    ),
                )
                ui.button(
                    VOLUME_SILVER,
                    on_click=lambda: run_command_with_dialog(
                        f"ls -lAR {MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{VOLUME_SILVER}"
                    ),
                )
                ui.button(VOLUME_GOLD, on_click=show_mysql_tables)
                # ui.button(VOLUME_GOLD, on_click=lambda: run_command_with_dialog(f"ls -lAR {MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{VOLUME_GOLD}"))

            ui.space()

            # ui.button("CDC", on_click=lambda: enable_cdc(source_table_path=f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}", destination_stream_topic=f"{BASEDIR}/{STREAM_CHANGELOG}:{TOPIC_TRANSACTIONS}"))
            # ui.switch("Lights on!", on_change=lights_on).props("flat outline color=dark keep-color").bind_value(app.storage.general, "lightson")
            # ui.space()

            ui.switch(on_change=toggle_log).tooltip("Debug").props(
                "color=dark keep-color"
            )

            # Github link
            with ui.link(
                target=DATA_DOMAIN.get("link", ""),
                new_tab=True,
            ).bind_visibility_from(
                DATA_DOMAIN, "link", backward=lambda x: x is not None
            ).classes(
                "place-items-center"
            ):
                ui.html("""
                <div id="c40" class="fill-white scale-125 m-1"><svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"></path></svg>
                </div>
                """)

    return footer


@ui.page("/", title="Data Fabric Demo")
async def index_page():
    
    header("Data Fabric Demo")

    demo_steps().classes("h-lvh")

    monitoring_card().classes(
        "flex-grow shrink absolute top-10 right-0 w-1/4 h-fit opacity-50 hover:opacity-100"
    )

    logging_card().classes(
        "flex-grow shrink absolute bottom-0 left-0 w-full opacity-50 hover:opacity-100"
        # "flex-grow shrink absolute top-64 right-0 w-1/4 opacity-50 hover:opacity-100"
    )
    
    # charts = monitoring_charts()
    # timer = ui.timer(MON_REFRESH_INTERVAL3, lambda c=chart: update_chart(c, txn_consumer_stats), active=False)
    # charts = monitoring_charts()
    # timers = monitoring_timers(charts)

    footer()


@ui.page("/mesh", title="Data Mesh Architecture")
async def mesh_page():

    header("Data Fabric")

    gui.mesh_ii().classes("h-lvh w-fit")
    
    footer()


@ui.page(f"/{DATA_PRODUCT}", title="Fraud Data Domain")
async def domain_page():
    """Draw an interactive image that shows demo pipeline for the data domain"""

    header(DATA_PRODUCT)

    gui.domain_ii()
    # Realtime monitoring information
    monitoring_card()
    # metric_badges_on_ii()
    logging_card()
    
    footer()


def demo_steps():
    with ui.list().classes("w-full") as demo_list:

        with ui.expansion("Demo Overview", caption="Providing an end-to-end pipeline for data ingestion, processsing, preperation and presentation", group="flow", value=False):
            ui.markdown(DOCUMENTATION["Overview"])
            ui.image(DATA_DOMAIN["diagram"]).props("fit=scale-down")

        with ui.expansion("Data Generation", caption="Prepare and publish mocked data into the pipeline", group="flow", value=True):

            ui.markdown(DOCUMENTATION["Source Data Generation"])
            
            with ui.row().classes("w-full place-items-center"):
                ui.label("Create files: ").classes("w-40")
                ui.button("Customers", on_click=create_customers)
                ui.button("Transactions", on_click=create_transactions)

            with ui.row().classes("w-full place-items-center"):
                ui.label("Peek files: ").classes("w-40")
                ui.button("Customers", on_click=peek_mocked_customers).props("outline")
                ui.button("Transactions", on_click=peek_mocked_transactions).props("outline")

            # with ui.row().classes("w-full place-items-center").bind_visibility_from(app.storage.general, "S3_SECRET_KEY"):
            #     ui.label("Create Input files: ").classes("w-40")
            #     ui.button("Into S3", color='notify', on_click=lambda: upload_to_s3(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv")).props('outline')
            #     ui.button("Show Bucket", color='notify', on_click=lambda: ui.navigate.to(f"http://{app.storage.general.get('S3_SERVER', 'localhost:9000')}", new_tab=True)).props('outline')

            with ui.row().classes("w-full place-items-center"):
                ui.label("Publish: ").classes("w-40")
                ui.button("To Kafka", on_click=publish_transactions).bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)

            with ui.row().classes("w-full place-items-center"):
                ui.label("View Code: ").classes("w-40")
                ui.button("Customers", on_click=code_create_customers, color="info").props("outline")
                ui.button("Transaction", on_click=code_create_transactions, color="info").props("outline")
                ui.button("Publish", on_click=code_publish_transactions, color="info").props("outline")

        with ui.expansion("Ingestion & ETL Processing (Bronze)", caption="Realtime processing on incoming data", group="flow"):
            with ui.dialog().props("full-width") as batch_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=batch_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(ingest_customers_iceberg)).classes("w-full mt-6")
                ui.code(inspect.getsource(iceberger.write)).classes("w-full")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Batch ingestion: ").classes("w-40")
                ui.button("Customers", on_click=ingest_customers_iceberg).bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)
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
                ui.button("Transactions", on_click=ingest_transactions).bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)
                ui.button("Peek", on_click=lambda: peek_documents(tablepath=f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}")).props("outline")
                ui.button("Using Airflow", color='notify', on_click=code_airflow_batch).props("outline").bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)
                ui.button("Code", on_click=stream_dialog.open, color="info").props("outline")

        with ui.expansion("Enrich & Clean (Silver)", caption="Integrate with data catalogue and clean/enrich data into silver tier", group="flow"):
            with ui.dialog().props("full-width") as enrich_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=enrich_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(refine_customers)).classes("w-full mt-6")
                ui.code(inspect.getsource(refine_transactions)).classes("w-full")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Data enrichment: ").classes("w-40")
                ui.button("Customers", on_click=refine_customers).bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)
                ui.button("Transactions", on_click=refine_transactions).bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)
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
                ui.button("Summarize", on_click=create_golden).bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x).bind_visibility_from(app.storage.general, 'MYSQL_PASS', backward=lambda x: x is not None and len(x) > 0)
                ui.button("Code", on_click=aggregate_dialog.open, color="info").props("outline")

        with ui.expansion("Check transactions for Fraud", caption="Process every transaction and check for fraud", group="flow"):
            with ui.dialog().props("full-width") as fraud_detection_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=fraud_detection_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(fraud_detection)).classes("w-full mt-6")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Run Fraud Detection against AI app: ").classes("w-40")
                ui.button("Run", on_click=fraud_detection)
                ui.button("Code", on_click=fraud_detection_dialog.open, color="info").props("outline")

    return demo_list

def cluster_configuration_dialog():
    with ui.dialog().props("position=right full-height") as dialog, ui.card().classes("relative bordered"):
        # with close button
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        # save/restore the configuration
        with ui.row():
            ui.button(icon="download", on_click=config_show().open)
            ui.button(icon="upload", on_click=config_load().open)

        with ui.card_section().classes("w-full mt-6"):
            ui.label("Upload Client Files").classes("text-lg w-full")
            ui.label("config.tar and/or jwt_tokens.tar.gz").classes("text-sm text-italic")
            ui.upload(label="Upload", on_upload=upload_client_files, multiple=True, auto_upload=True, max_files=2).props("accept='application/x-tar,application/x-gzip' hide-upload-btn").classes("w-full")

        ui.separator()
        with ui.card_section():
            with ui.row().classes("w-full place-items-center mt-4"):
                ui.label("Select Data Domain").classes("text-lg")
                ui.button(icon="refresh", on_click=update_clusters).props("flat round")
            ui.toggle(app.storage.general.get("clusters", [])).bind_value(app.storage.general, "cluster")

        ui.separator()
        with ui.card_section():
            ui.label("External Data Lakes").classes("text-lg w-full")
            with ui.row().classes("w-full place-items-center"):
                ui.input("Minio Host", placeholder="minio.local").bind_value(app.storage.general, "S3_SERVER")
                ui.input("NFS Server", placeholder="nfs-server.dom").bind_value(app.storage.general, "NFS_SERVER")
                ui.button(
                    "Mount",
                    on_click=lambda: run_command_with_dialog(
                        f"umount -l /mnt; mount -t nfs4 -o nolock,proto=tcp,port=2049,sec=sys {app.storage.general.get('NFS_SERVER', 'localhost')}:/ /mnt; ls -lA /mnt"
                    )
                ).props("")

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
                USE {DATA_PRODUCT};
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
            if "MAPR_USER" in app.storage.general.keys() and "cluster" in app.storage.general.keys():
                cluster = app.storage.general.get("cluster", "127.0.0.1")
                os.environ["CLUSTER_NAME"] = get_cluster_name()
                os.environ["CLUSTER_IP"] = cluster if cluster is not None else "127.0.0.1"
                os.environ["MAPR_USER"] = app.storage.general.get("MAPR_USER", "mapr")
                os.environ["MAPR_PASS"] = app.storage.general.get("MAPR_PASS", "mapr123")
                with ui.row().classes("w-full place-items-center mt-4"):
                        ui.button("Reconfigure", on_click=lambda: run_command_with_dialog("bash /app/reconfigure.sh"))
                        ui.button("maprlogin", on_click=lambda: run_command_with_dialog(f"echo {app.storage.general['MAPR_PASS']} | maprlogin password -user {app.storage.general['MAPR_USER']}"))
                with ui.row().classes("w-full place-items-center mt-4"):
                    ui.button(f"remount {MOUNT_PATH}", on_click=lambda: run_command_with_dialog(f"[ -d {MOUNT_PATH} ] && umount -l {MOUNT_PATH}; [ -d {MOUNT_PATH} ] || mkdir -p {MOUNT_PATH}; mount -t nfs -o nolock,soft {app.storage.general['cluster']}:/mapr {MOUNT_PATH}"))
                    ui.button("List Cluster /", on_click=lambda: run_command_with_dialog(f"ls -la {MOUNT_PATH}/{get_cluster_name()}")).props('outline')

        ui.separator()
        with ui.card_section():
            ui.label("Dashboard").classes("text-lg w-full")
            ui.label("Link to external dashboard page").classes("text-sm text-italic")
            ui.input("Dashboard URL").bind_value(app.storage.general, "DASHBOARD_URL").classes("w-full")

        ui.separator()
        with ui.card_section():
            ui.label("Create the Entities").classes("text-lg w-full")
            ui.label("required volumes and streams")
            with ui.row().classes("w-full place-items-center mt-4"):
                ui.button("Volumes", on_click=create_volumes).bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)
                ui.button("Streams", on_click=create_streams).bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)
                ui.button("Tables", on_click=create_tables).bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)

        ui.separator()
        with ui.card_section():
            ui.label("Clean up!").classes("text-lg")
            ui.label("Use when done with the demo. This will remove all volumes and streams, ALL DATA will be gone!").classes("text-sm text-italic")
            ui.button("DELETE ALL!", on_click=delete_volumes_and_streams, color="negative").classes("mt-4").bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


def config_show():
    with ui.dialog() as config_show, ui.card().classes("grow"):
        ui.code(json.dumps(app.storage.general, indent=2), language="json").classes("w-full text-wrap")
        with ui.row().classes("w-full"):
            ui.button(
                "Save",
                icon="save",
                on_click=lambda: ui.download("/config"),
            )
            ui.space()
            ui.button(
                "Close",
                icon="cancel",
                on_click=config_show.close,
            )
    return config_show


def config_load():
    with ui.dialog() as config_load, ui.card().classes("grow"):
        ui.upload(label="Config JSON", auto_upload=True, on_upload=lambda e: save_config(e.content.read().decode("utf-8"), config_load)).classes(
            "max-w-full"
        ).props("accept='application/json' hide-upload-btn")

    return config_load


def save_config(val: str, dialog):
    try:
        for key, value in json.loads(val.replace("\n", "")).items():
            app.storage.general[key] = value
        dialog.close()
        ui.notify("Settings loaded", type="positive")
        ui.notify("Refresh the page!!", type="error")
    except (TypeError, json.decoder.JSONDecodeError, ValueError) as error:
        ui.notify("Not a valid json", type="negative")
        logger.warning(error)


@app.get("/config")
def download(content: str = None):
    # by default downloading settings
    if content is None:
        content = app.storage.general

    string_io = io.StringIO(json.dumps(content))  # create a file-like object from the string

    headers = {"Content-Disposition": "attachment; filename=config.json"}
    return StreamingResponse(string_io, media_type="text/plain", headers=headers)

