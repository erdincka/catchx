import inspect
import io
from fastapi.responses import StreamingResponse
from nicegui import ui, app

from functions import *
from common import *
from ingestion import *
from mock import *
from monitoring import *
import streams
import tables


def app_init():

    # Reset metrics
    for metric in [
                "in_txn_pushed",
                "in_txn_pulled",
                "brnz_customers",
                "brnz_transactions",
                "slvr_profiles",
                "slvr_transactions",
                "slvr_customers",
                "gold_transactions",
                "gold_customers",
                "gold_fraud",
            ]:
        app.storage.general[metric] = 0

    # and previous run state if it was hang
    app.storage.general["busy"] = False

    # reset the cluster info
    if "clusters" not in app.storage.general:
        app.storage.general["clusters"] = {}

    # If user is not set, get from environment
    if "MAPR_USER" not in app.storage.general:
        app.storage.general["MAPR_USER"] = os.environ.get("MAPR_USER", "mapr")
        app.storage.general["MAPR_PASS"] = os.environ.get("MAPR_PASS", "mapr123")


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
                ui.button("Peek Customer Feed", on_click=peek_mocked_customers).props("outline")
                ui.button("Peek Transaction Feed", on_click=peek_mocked_transactions).props("outline")
                ui.button("Into S3", color='warning', on_click=not_implemented).props('outline').bind_visibility_from(app.storage.general, "S3_SECRET_KEY")
                ui.button("Show Bucket", color='warning', on_click=not_implemented).props('outline').bind_visibility_from(app.storage.general, "S3_SECRET_KEY")
                ui.button("Code", on_click=generate_dialog.open, color="info").props("outline")

            with ui.dialog().props("full-width") as publish_dialog, ui.card().classes("grow relative"):
                ui.button(icon="close", on_click=publish_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
                ui.code(inspect.getsource(publish_transactions)).classes("w-full mt-6")
                ui.code(inspect.getsource(streams.produce)).classes("w-full")

            with ui.row().classes("w-full place-items-center"):
                ui.label("Publish to Kafka stream: ").classes("w-40")
                ui.button("Publish", on_click=publish_transactions).bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)
                ui.button("Code", on_click=publish_dialog.open, color="info").props("outline")

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
                ui.button("Using Airflow", color='warning', on_click=not_implemented).props("outline").bind_enabled_from(app.storage.general, "busy", backward=lambda x: not x)
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
        )

    return config_load


def save_config(val: str, dialog):
    try:
        for key, value in json.loads(val.replace("\n", "")).items():
            app.storage.general[key] = value
        dialog.close()
        ui.notify("Settings loaded", type="positive")
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


# NOT USED
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
