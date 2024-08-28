import io
from fastapi.responses import StreamingResponse
from nicegui import ui, app

from functions import *
from common import *
import gui
from ingestion import *
from mock import *
from monitoring import *
from codeviewers import *

logger = logging.getLogger("page")


def header(title: str):
    with ui.header(elevated=True).classes("items-center justify-between uppercase py-1 px-4") as header:
        ui.button(icon="home", on_click=lambda: ui.navigate.to(index_page)).props("flat color=light")

        ui.label(title)

        # ui.icon(None, size='lg').bind_name_from(app.storage.user, "demo_mode", backward=lambda x: "s_preview" if x else "s_preview_off").tooltip("Presentation Mode")

        ui.switch("Go Live").props("color=accent").bind_value(app.storage.user, 'demo_mode').bind_visibility_from(app.storage.user, "clusterinfo", backward=lambda x: x and len(x) > 0)

        ui.switch("Monitor", on_change=lambda x: toggle_monitoring(x.value)).props("color=accent").bind_visibility_from(app.storage.user, 'demo_mode')

        ui.space()

        with ui.row().classes("place-items-center"):
            ui.link(
                target=f"https://{app.storage.user.get('MAPR_USER', '')}:{app.storage.user.get('MAPR_PASS', '')}@{app.storage.user.get('MAPR_HOST', '')}:8443/app/mcs/",
                new_tab=True
            ).classes(
                "text-white hover:text-blue-600"
            ).bind_text_from(app.storage.user, "clusterinfo", backward=lambda x: x["name"] if x else "No Cluster"
            ).bind_visibility_from(app.storage.user, "clusterinfo", backward=lambda x: x and len(x) > 0)

            # Connect to a cluster
            # ui.label("Not configured!").classes("text-bold red").bind_visibility_from(app.storage.user, "cluster", backward=lambda x: not x or len(x) == 0)
            ui.button(icon="link" if "clusterinfo" in app.storage.user.keys() else "link_off", on_click=cluster_connect).props("flat color=light")

            ui.button(icon="settings", on_click=demo_configuration_dialog).props(
                "flat color=light"
            )

            ui.icon("error", size="2em", color="red").bind_visibility_from(
                app.storage.user, "clusterinfo", lambda x: not x or len(x) == 0
            ).tooltip("Requires configuration!")

            with ui.element("div").bind_visibility_from(app.storage.user, "demo_mode"):
                ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                    app.storage.user, "busy", lambda x: not x
                ).tooltip("Ready")
                ui.spinner("ios", size="2em", color="red").bind_visibility_from(
                    app.storage.user, "busy"
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

                ui.button(VOLUME_GOLD, on_click=lambda: run_command_with_dialog(f"ls -lAR {MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{VOLUME_GOLD}"))

            ui.space()

            # ui.button("CDC", on_click=lambda: enable_cdc(source_table_path=f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}", destination_stream_topic=f"{BASEDIR}/{STREAM_CHANGELOG}:{TOPIC_TRANSACTIONS}"))
            # ui.switch("Lights on!", on_change=lights_on).props("flat outline color=dark keep-color").bind_value(app.storage.user, "lightson")
            # ui.space()

            # NOT USED
            # ui.switch(on_change=toggle_log).tooltip("Debug").props(
            #     "color=dark keep-color"
            # )

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

    with ui.grid(columns=2).classes("w-full gap-0 flex"):

        demo_steps().classes("flex-1")

        # Realtime monitoring information
        monitoring_card().classes("flex")


    logging_card().classes(
    "flex-grow shrink absolute bottom-0 left-0 w-full opacity-50 hover:opacity-100"
    # "w-full"
    )

    # charts = monitoring_charts()
    # timer = ui.timer(MON_REFRESH_INTERVAL3, lambda c=chart: update_chart(c, txn_consumer_stats), active=False)

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

    # with ui.grid(columns=2).classes("w-full gap-0 flex"):

    # Realtime monitoring information
    monitoring_ticker().classes("flex w-full place-items-center") # overflow-x-auto no-wrap

    gui.domain_ii().classes("flex-1")

    logging_card().classes(
        # "flex-grow shrink absolute bottom-0 left-0 w-full opacity-50 hover:opacity-100"
        "flex-grow shrink w-full"
    )

    footer()


def demo_steps():
    with ui.list() as demo_list:

        with ui.expansion("Demo Overview", caption="Providing an end-to-end pipeline for data ingestion, processsing, preperation and presentation", group="flow", value=False):
            ui.markdown(DOCUMENTATION["Overview"])
            ui.image(DATA_DOMAIN["diagram"]).props("fit=scale-down")

        with ui.expansion("Data Ingestion", caption="Review and push data into the pipeline", group="flow", value=True):

            ui.markdown(DOCUMENTATION["Source Data Generation"])

            with ui.list().props('bordered separator').classes("w-full"):
                ui.item_label('Data Sources').props('header').classes('text-bold')
                ui.separator()
                with ui.item():
                    with ui.item_section().props('avatar'):
                        ui.icon('people')
                    with ui.item_section().classes("flex-grow"):
                        ui.item_label('Customers')
                        ui.item_label(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv").props('caption')
                    with ui.item_section().props('side'):
                        with ui.row():
                            ui.button(icon='visibility', color="neutral", on_click=peek_mocked_customers).props('flat dense round').tooltip("Preview data")
                            ui.button(icon='person_add', color="info", on_click=code_create_customers).props('flat dense round').tooltip("View code for new customers")
                            ui.button(icon='person_add', color="secondary", on_click=create_customers).props('flat dense round').tooltip("Create new customers").bind_visibility_from(app.storage.user, "demo_mode")
                with ui.item():
                    with ui.item_section().props('avatar'):
                        ui.icon('payments')
                    with ui.item_section().classes("flex-grow"):
                        ui.item_label('Transactions')
                        ui.item_label(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv").props('caption')
                    with ui.item_section().props('side'):
                        with ui.row():
                            ui.button(icon='visibility', color="neutral", on_click=peek_mocked_transactions).props('flat dense round').tooltip("Preview data")
                            ui.button(icon='add_card', color="info", on_click=code_create_transactions).props('flat dense round').tooltip("View code for new transactions")
                            ui.button(icon='add_card', color="secondary", on_click=create_transactions).props('flat dense round').tooltip("Create new transactions").bind_visibility_from(app.storage.user, "demo_mode")
                            ui.button(icon='code', color="info", on_click=code_publish_transactions).props('flat dense round').tooltip("View code for transactions ingestion")
                            ui.button(icon='rocket_launch', color="positive", on_click=publish_transactions).props('flat dense round').tooltip("Publish transactions").bind_visibility_from(app.storage.user, "demo_mode")

            # with ui.row().classes("w-full place-items-center").bind_visibility_from(app.storage.user, "S3_SECRET_KEY"):
            #     ui.label("Create Input files: ").classes("w-40")
            #     ui.button("Into S3", color='notify', on_click=lambda: upload_to_s3(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv")).props('outline')
            #     ui.button("Show Bucket", color='notify', on_click=lambda: ui.navigate.to(f"http://{app.storage.user.get('S3_SERVER', 'localhost:9000')}", new_tab=True)).props('outline')

        with ui.expansion("Ingestion & ETL Processing (Bronze)", caption="Realtime processing on incoming data", group="flow"):

            ui.markdown(DOCUMENTATION["Data Ingestion and ETL"])

            with ui.list().props('bordered separator').classes("w-full"):
                ui.item_label('ETL Processing').props('header').classes('text-bold')
                ui.separator()
                with ui.item():
                    with ui.item_section().props('avatar'):
                        ui.icon('content_copy')
                    with ui.item_section().classes("flex-grow"):
                        ui.item_label('Batch')
                        ui.item_label(f"into {MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{VOLUME_BRONZE}/{TABLE_CUSTOMERS}").props('caption')
                    with ui.item_section().props('side'):
                        with ui.row():
                            ui.button(icon='visibility', color="neutral", on_click=lambda: iceberg_table_tail(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS)).props('flat dense round').tooltip("Sample data in Iceberg table")
                            ui.button(icon='history', color="neutral", on_click=lambda: iceberg_table_history(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS)).props('flat dense round').tooltip("Iceberg table history")
                            ui.button(icon='code', color="info", on_click=code_batch).props('flat dense round').tooltip("View code for ingesting data into Iceberg table")
                            ui.button(icon='rocket_launch', color="positive", on_click=ingest_customers_iceberg).props('flat dense round').tooltip("Ingest customers to Iceberg table").bind_visibility_from(app.storage.user, "demo_mode")
                with ui.item():
                    with ui.item_section().props('avatar'):
                        ui.icon('sms')
                    with ui.item_section().classes("flex-grow"):
                        ui.item_label('Streaming')
                        ui.item_label(f"into {MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}").props('caption')
                    with ui.item_section().props('side'):
                        with ui.row():
                            ui.button(icon='visibility', color="neutral", on_click=peek_bronze_transactions).props('flat dense round').tooltip("Look at the data in transactions table")
                            ui.button(icon='stream', color="info", on_click=code_nifi_stream).props('flat dense round').tooltip("View code for Nifi")
                            ui.button(icon='stream', color="secondary", on_click=lambda: ui.navigate.to(f"https://{app.storage.user.get('MAPR_HOST', 'localhost')}:12443/nifi/", new_tab=True)).props('flat dense round').tooltip("Open NiFi").bind_visibility_from(app.storage.user, "demo_mode")
                            ui.button(icon='code', color="info", on_click=code_stream).props('flat dense round').tooltip("View code for transactions ingestion")
                            ui.button(icon='rocket_launch', color="positive", on_click=ingest_transactions).props('flat dense round').tooltip("Publish transactions").bind_visibility_from(app.storage.user, "demo_mode")

        with ui.expansion("Enrich & Clean Up (Silver)", caption="Integrate with data catalogue and clean/enrich data into silver tier", group="flow"):
            ui.markdown(DOCUMENTATION["Data Enrichment"])

            # with ui.dialog().props("full-width") as enrich_dialog, ui.card().classes("grow relative"):
            #     ui.button(icon="close", on_click=enrich_dialog.close).props("flat round dense").classes("absolute right-2 top-2")
            #     ui.code(inspect.getsource(refine_customers)).classes("w-full mt-6")
            #     ui.code(inspect.getsource(refine_transactions)).classes("w-full")

            with ui.list().props('bordered separator').classes("w-full"):
                ui.item_label('Data Cleansing').props('header').classes('text-bold')
                ui.separator()
                with ui.item():
                    with ui.item_section().props('avatar'):
                        ui.icon('auto_awesome')
                    with ui.item_section().classes("flex-grow"):
                        ui.item_label('Refine customers')
                        ui.item_label("Add country_name and iso3166_2 county code, hide birthday and current_location").props('caption')
                    with ui.item_section().props('side'):
                        with ui.row():
                            ui.button(icon='visibility', color="neutral", on_click=peek_silver_profiles).props('flat dense round').tooltip("Sample refined profile data")
                            ui.button(icon='visibility', color="neutral", on_click=peek_silver_customers).props('flat dense round').tooltip("Sample refined customer data")
                            ui.button(icon='code', color="info", on_click=code_enrich_customers).props('flat dense round').tooltip("View code for ingesting data into Iceberg table")
                            ui.button(icon='rocket_launch', color="positive", on_click=refine_customers).props('flat dense round').tooltip("Ingest customers to Iceberg table").bind_visibility_from(app.storage.user, "demo_mode")

                with ui.item():
                    with ui.item_section().props('avatar'):
                        ui.icon('auto_awesome')
                    with ui.item_section().classes("flex-grow"):
                        ui.item_label('Refine transactions')
                        ui.item_label(f"Add category to transactions table").props('caption')
                    with ui.item_section().props('side'):
                        with ui.row():
                            ui.button(icon='visibility', color="neutral", on_click=peek_silver_transactions).props('flat dense round').tooltip("Sample refined transaction data")
                            ui.button(icon='code', color="info", on_click=code_enrich_transactions).props('flat dense round').tooltip("View code for ingesting data into Iceberg table")
                            ui.button(icon='rocket_launch', color="positive", on_click=refine_transactions).props('flat dense round').tooltip("Ingest transactions to Iceberg table").bind_visibility_from(app.storage.user, "demo_mode")

        with ui.expansion("Consolidate (Gold)", caption="Create data lake for gold tier", group="flow"):
            ui.markdown(DOCUMENTATION["Data Consolidation"])

            with ui.list().props('bordered separator').classes("w-full"):
                ui.item_label('Summerize').props('header').classes('text-bold')
                ui.separator()
                with ui.item():
                    with ui.item_section().props('avatar'):
                        ui.icon('compare_arrows')
                    with ui.item_section().classes("flex-grow"):
                        ui.item_label('Consolidate')
                        ui.item_label("Create the golden tier").props('caption')
                    with ui.item_section().props('side'):
                        with ui.row():
                            ui.button(icon='visibility', color="neutral", on_click=peek_gold_customers).props('flat dense round').tooltip("Sample consolidated customer data")
                            ui.button(icon='visibility', color="neutral", on_click=peek_gold_transactions).props('flat dense round').tooltip("Sample consolidated transaction data")
                            ui.button(icon='code', color="info", on_click=code_create_golden).props('flat dense round').tooltip("View code for ingesting data into Deltalake table")
                            ui.button(icon='rocket_launch', color="positive", on_click=create_golden).props('flat dense round').tooltip("Ingest customers to Deltalake table").bind_visibility_from(app.storage.user, "demo_mode")

                with ui.item():
                    with ui.item_section().props('avatar'):
                        ui.icon('merge')
                    with ui.item_section().classes("flex-grow"):
                        ui.item_label('Check for Fraud')
                        ui.item_label("Simulate an AI inference").props('caption')
                    with ui.item_section().props('side'):
                        with ui.row():
                            ui.button(icon='visibility', color="neutral", on_click=peek_gold_fraud).props('flat dense round').tooltip("Sample fraud transactions")
                            ui.button(icon='code', color="info", on_click=code_fraud_detection).props('flat dense round').tooltip("View code used for fraud detection")
                            ui.button(icon='rocket_launch', color="positive", on_click=fraud_detection).props('flat dense round').tooltip("Scan and update transactions for fraud").bind_visibility_from(app.storage.user, "demo_mode")

    return demo_list


def cluster_connect():
    with ui.dialog().props("full-width") as cluster_connect_dialog, ui.card():
        with ui.row().classes("w-full place-items-center"):
            ui.input("Hostname / IP Address").classes("flex-grow").bind_value(app.storage.user, "MAPR_HOST")
            ui.input("Username").classes("flex-grow").bind_value(app.storage.user, "MAPR_USER")
            ui.input("Password", password=True, password_toggle_button=True).classes("flex-grow").bind_value(app.storage.user, "MAPR_PASS")
            # ui.space()
            ui.button(icon="rocket_launch", on_click=run_configuration_steps)

        ui.separator()

        with ui.grid(columns=3):
            for step in cluster_configuration_steps:
                ui.label(step["name"])
                ui.label(step["info"])
                ui.icon("", size='sm', color="info").bind_name_from(step, "status")

    cluster_connect_dialog.open()


async def run_configuration_steps():
    logger.info("Connecting to node %s...", app.storage.user['MAPR_HOST'])

    for step in cluster_configuration_steps:
        # Step 1 - Get cluster information
        if step["name"] == "clusterinfo":
            step["status"] = "run_circle"

            try:
                auth = (app.storage.user["MAPR_USER"], app.storage.user["MAPR_PASS"])
                URL = f"https://{app.storage.user['MAPR_HOST']}:8443/rest/dashboard/info"

                async with httpx.AsyncClient(verify=False) as client:
                    response = await client.get(URL, auth=auth)

                    # logger.debug(response.text)

                    if response is None or response.status_code != 200:
                        logger.warning("Response: %s", response.text)
                        step["status"] = "error"

                    else:
                        res = response.json()
                        # logger.debug("Got dashboard data: %s", json.dumps(res))
                        # Set cluster information
                        app.storage.user["clusterinfo"] = res["data"][0]["cluster"]
                        step["status"] = "check"


            except Exception as error:
                logger.error("Failed to connect to cluster.")
                logger.info(error)
                step["status"] = "error"

        # Step 2 - Configure cluster
        elif step["name"] == "reconfigure":
            step["status"] = "run_circle"

            os.environ["CLUSTER_IP"] = res["data"][0]["cluster"]["ip"]
            os.environ["CLUSTER_NAME"] = res["data"][0]["cluster"]["name"]
            os.environ["MAPR_USER"] = app.storage.user["MAPR_USER"]
            os.environ["MAPR_PASS"] = app.storage.user["MAPR_PASS"]
            async for out in run_command("/bin/bash ./cluster_configure_and_attach.sh"):
                logger.info(out.strip())

            step["status"] = "check"

        # Step 3 - Create volumes and streams
        elif step["name"] == "createvolumes":
            step["status"] = "run_circle"
            if await create_volumes() and await create_tables() and await create_streams():
                step["status"] = "check"
            else: step["status"] = "error"

        # Step 4 - Create customers and transactions
        elif step["name"] == "mockdata":
            step["status"] = "run_circle"
            if await create_customers() and await create_transactions():
                step["status"] = "check"
            else: step["status"] = "error"

        else: logger.debug("%s not defined", step["name"])

    # mark configured
    app.storage.user["configured"] = True


def demo_configuration_dialog():
    with ui.dialog().props("position=right full-height") as dialog, ui.card().classes("relative bordered"):
        # with close button
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        # save/restore the configuration
        with ui.row():
            ui.button(icon="download", on_click=config_show().open)
            ui.button(icon="upload", on_click=config_load().open)

        # Configure External Data Lakes
        with ui.card_section():
            ui.label("External Data Lakes").classes("text-lg w-full")
            with ui.row().classes("w-full place-items-center"):
                ui.input("Minio Host", placeholder="minio.local").bind_value(app.storage.user, "S3_SERVER")
                ui.input("NFS Server", placeholder="nfs-server.dom").bind_value(app.storage.user, "NFS_PATH")
                ui.button(
                    "Mount",
                    on_click=lambda: run_command_with_dialog(
                        f"umount -l /mnt; mount -t nfs4 -o nolock,proto=tcp,port=2049,sec=sys {app.storage.user.get('NFS_PATH', 'localhost')} /mnt; ls -lA /mnt"
                    )
                ).props("")

        ui.separator()
        with ui.card_section():

            ui.label("S3 Credentials").classes("text-lg w-full mt-4")
            ui.label("for iceberg and spark").classes("text-sm text-italic")
            with ui.row().classes("w-full place-items-center"):
                ui.input("Access Key").bind_value(app.storage.user, "S3_ACCESS_KEY")
                ui.input("Secret Key", password=True, password_toggle_button=True).bind_value(app.storage.user, "S3_SECRET_KEY")


        ui.separator()
        with ui.card_section():
            ui.label("Dashboard").classes("text-lg w-full")
            ui.label("Link to external dashboard page").classes("text-sm text-italic")
            ui.input("Dashboard URL").bind_value(app.storage.user, "DASHBOARD_URL").classes("w-full")
            ui.label("Catalogue").classes("text-lg w-full")
            ui.label("Link to external catalogue page").classes("text-sm text-italic")
            ui.input("Catalogue URL").bind_value(app.storage.user, "CATALOGUE_URL").classes("w-full")

        ui.separator()
        with ui.card_section():
            ui.label("Create the Entities").classes("text-lg w-full")
            ui.label("required volumes and streams")
            with ui.row().classes("w-full place-items-center mt-4"):
                ui.button("List cluster root", on_click=lambda: run_command_with_dialog(f"ls -lA /mapr")).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                ui.button("Remount", on_click=lambda: run_command_with_dialog(f"([ -d /mapr ] && umount -l /mapr) || mkdir /mapr; mount -t nfs4 -o nolock,soft {app.storage.user.get('MAPR_HOST', '')}:/mapr /mapr")).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                # ui.button("Volumes", on_click=create_volumes).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                # ui.button("Streams", on_click=create_streams).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)
                # ui.button("Tables", on_click=create_tables).bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)

        ui.separator()
        with ui.card_section():
            ui.label("Clean up!").classes("text-lg")
            ui.label("Use when done with the demo. This will remove all volumes and streams, ALL DATA will be gone!").classes("text-sm text-italic")
            ui.button("DELETE ALL!", on_click=delete_volumes_and_streams, color="negative").classes("mt-4").bind_enabled_from(app.storage.user, "busy", backward=lambda x: not x)

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


def config_show():
    with ui.dialog() as config_show, ui.card().classes("grow"):
        ui.code(json.dumps(app.storage.user, indent=2), language="json").classes("w-full text-wrap")
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
        ui.upload(label="Config JSON", auto_upload=True, on_upload=lambda e: config_save(e.content.read().decode("utf-8"), config_load)).classes(
            "max-w-full"
        ).props("accept='application/json' hide-upload-btn")

    return config_load


def config_save(val: str, dialog):
    try:
        for key, value in json.loads(val.replace("\n", "")).items():
            app.storage.user[key] = value
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
        content = app.storage.user

    string_io = io.StringIO(json.dumps(content))  # create a file-like object from the string

    headers = {"Content-Disposition": "attachment; filename=config.json"}
    return StreamingResponse(string_io, media_type="text/plain", headers=headers)
