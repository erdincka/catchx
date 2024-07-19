import os
import shutil
import inspect
import logging
import re
from nicegui import ui, app

from common import *
from functions import *
from ingestion import *
from mock import *
from monitoring import *
import streams
import iceberger
import theme

logger = logging.getLogger("gui")


active_dialog = None


def handle_image_info(e: events.MouseEventArguments):
    logger.debug(e)
    # eventargs is a dict with: type, element_id, image_x, image_y keys.
    element = e["element_id"]

    # Regex to convert "CamelCase" to "Title Case"
    title = " ".join(re.findall(r"[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))", element))

    if element == "Fraud":
        label = "Open the Fraud Data Pipeline"
    elif element == "NFS":
        label = "Bring existing datalakes into the 'Mesh'."
    elif element == "S3":
        label = "Bring existing Object Stores into the 'Mesh'."
    elif element == "IAM":
        label = "Globally manage and integrate with IAM for auth/z"
    elif element == "Edge":
        label = "'Data Domains' without 'Data Products' can join to the Mesh too"
    elif element == "Policies":
        label = "Define and distribute governance policies from federation hub"
    elif element == "Catalogue":
        label = "Enable Data Product registration and publishing from federation hub"

    # Data Domain functions
    elif element == "PublishTransactions":
        label = "Send transactions into the input stream"
    elif element == "PublishTransactionsCode":
        label = "Look at the code that publishes demo data for ingestion"
    elif element == "CreateData":
        label = "Generate mock data to use for ingestion - customers & transactions"
    elif element == "CreateDataCode":
        label = "Look at the code for creating the data for this demo"
    elif element == "IngestTransactions":
        label = "Process incoming transactions with stream consumer."
    elif element == "IngestCustomersIceberg":
        label = "Execute batch data ingestion."
    elif element == "BronzeTransactions":
        label = "Look at the transactions data from Bronze tier"
    elif element == "RefineTransactions":
        label = "Create refined transactions table in the silver tier"
    elif element == "RefineCustomers":
        label = "Create refined customers table in the silver tier"
    else:
        label = "Not defined yet"
    ui.notify(
        message=title,
        caption=label,
        type="info",
        color="secondary",
        progress=True,
        multi_line=True,
        timeout=2000,
    )


# dialogs for various phases
# automatically clear the content for other dialogs
def code_publish_transactions():
    with ui.dialog().props("full-width") as publish_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=publish_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(publish_transactions)).classes("w-full mt-6")
        ui.code(inspect.getsource(streams.produce)).classes("w-full")
    publish_codeview.on("close", lambda d=publish_codeview: d.delete())
    return publish_codeview


def code_create_customers():
    with ui.dialog().props("full-width") as generate_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=generate_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(create_customers)).classes("w-full mt-6")
        ui.code(inspect.getsource(fake_customer)).classes("w-full")
    generate_codeview.on("close", lambda d=generate_codeview: d.delete())
    return generate_codeview


def code_create_transactions():
    with ui.dialog().props("full-width") as generate_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=generate_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(create_transactions)).classes("w-full mt-6")
        ui.code(inspect.getsource(fake_transaction)).classes("w-full")
    generate_codeview.on("close", lambda d=generate_codeview: d.delete())
    return generate_codeview


def code_batch():
    with ui.dialog().props("full-width") as batch_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=batch_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(ingest_customers_iceberg)).classes("w-full mt-6")
        ui.code(inspect.getsource(iceberger.write)).classes("w-full")
    batch_codeview.on("close", lambda d=batch_codeview: d.delete())
    return batch_codeview


def code_airflow_batch():
    with ui.dialog().props("full-width") as batch_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=batch_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        with open("DAG_csv_to_iceberg.py", 'r') as f:
            ui.code(f.read()).classes("w-full mt-6")
        # ui.code(inspect.getsource(DAG_csv_to_iceberg.read_csv_task)).classes("w-full mt-6")
    batch_codeview.on("close", lambda d=batch_codeview: d.delete())
    return batch_codeview


def code_nifi_stream():
    with ui.dialog().props("full-width") as stream_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=stream_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        with open("DAG_csv_to_iceberg.py", 'r') as f:
            ui.code(f.read()).classes("w-full mt-6")
        # ui.code(inspect.getsource(DAG_csv_to_iceberg.read_csv_task)).classes("w-full mt-6")
    stream_codeview.on("close", lambda d=stream_codeview: d.delete())
    return stream_codeview


def code_stream():
    with ui.dialog().props("full-width") as stream_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=stream_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(ingest_transactions)).classes("w-full mt-6")
        ui.code(inspect.getsource(streams.consume)).classes("w-full")
        ui.code(inspect.getsource(tables.upsert_document)).classes("w-full")
    stream_codeview.on("close", lambda d=stream_codeview: d.delete())
    return stream_codeview


def code_profilebuilder():
    with ui.dialog().props("full-width") as profile_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=profile_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(upsert_profile)).classes("w-full mt-6")
    profile_codeview.on("close", lambda d=profile_codeview: d.delete())
    return profile_codeview


def code_enrich_customers():
    with ui.dialog().props("full-width") as enrich_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=enrich_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(refine_customers)).classes("w-full mt-6")
    enrich_codeview.on("close", lambda d=enrich_codeview: d.delete())
    return enrich_codeview


def code_enrich_transactions():
    with ui.dialog().props("full-width") as enrich_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=enrich_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(refine_transactions)).classes("w-full mt-6")
    enrich_codeview.on("close", lambda d=enrich_codeview: d.delete())
    return enrich_codeview


def code_getscore():
    with ui.dialog().props("full-width") as scoring_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=scoring_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(dummy_fraud_score)).classes("w-full mt-6")
    scoring_codeview.on("close", lambda d=scoring_codeview: d.delete())
    return scoring_codeview


def code_create_golden():
    with ui.dialog().props("full-width") as golden_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=golden_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(data_aggregation)).classes("w-full mt-6")
    golden_codeview.on("close", lambda d=golden_codeview: d.delete())
    return golden_codeview


async def handle_image_action(e: events.MouseEventArguments):
    # logger.debug(e)

    element = e["element_id"]

    app.storage.general["busy"] = True

    if element == "Fraud":
        ui.navigate.to(DATA_PRODUCT)
        # await open_dialog(domain_ii)
        # ui.navigate.to(domain_ii, new_tab=True)
    # TODO: add information or configuration pages!
    elif element == "NFS":
        ui.notify("Bringing existing data lakes into the Global Namespace.", type="info")
        # show the mounted path
        await run_command_with_dialog(f"df -h /mnt; ls -lA /mnt; ls -lA /mnt{EXTERNAL_NFS_PATH}")

    elif element == "S3":
        ui.navigate.to(f"http://{app.storage.general.get('S3_SERVER', 'localhost:9000')}", new_tab=True)
        ui.notify("Bring existing object stores into the Global Namespace.", type="info")

    elif element == "IAM":
        ui.navigate.to(f"https://{app.storage.general.get('cluster', 'localhost')}:8443/app/dfui/#/login", new_tab=True)
        ui.notify(
            "Integrate with central IAM provider for consistent access control across the enterprise",
            type="info",
        )
    elif element == "Policies":
        ui.notify(
            "Attach to central policy provider/enforcer, or simply publish policies to govern data products across the enterprise.",
            type="info",
        )
    elif element == "Catalogue":
        ui.notify(
            "Integrate with an external catalogue manager to classify, document and serve various data sources.",
            type="info",
        )
    elif element == "Edge":
        ui.notify(
            "Enable non-data products to become a part of the global namespace, enabling them to access data across the enterprise.",
            type="info",
        )

    # Data Domain functions
    elif element == "CreateTransactions":
        await create_transactions()

    elif element == "CreateTransactionsCode":
        code_create_transactions().open()

    elif element == "PublishTransactions":
        await publish_transactions()

    elif element == "PublishTransactionsCode":
        code_publish_transactions().open()

    elif element == "CreateCustomers":
        await create_customers()

    elif element == "CreateCustomersCode":
        code_create_customers().open()

    elif element == "AirflowBatch":
        ui.navigate.to(f"https://{app.storage.general.get('cluster', 'localhost')}:8780/home", new_tab=True)

    elif element == "IngestCustomersIceberg":
        await ingest_customers_iceberg()
        # ui.button("History", on_click=lambda: iceberg_table_history(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS)).props("outline")

    elif element == "AirflowBatchCode":
        code_airflow_batch().open()

    elif element == "NifiStreamsCode":
        code_nifi_stream().open()

    elif element == "IngestCustomersIcebergCode":
        code_batch().open()

    elif element == "IngestTransactions":
        await ingest_transactions()

    elif element == "NifiStreams":
        ui.navigate.to(f"https://{app.storage.general.get('cluster', 'localhost')}:12443/nifi/", new_tab=True)

    elif element == "IngestTransactionsCode":
        code_stream().open()

    elif element == "ProfileBuilderCode":
        code_profilebuilder().open()

    elif element == "RefineTransactions":
        await refine_transactions()

    elif element == "RefineTransactionsCode":
        code_enrich_transactions().open()

    elif element == "BronzeTransactions":
        peek_documents(f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}")

    elif element == "RefineCustomers":
        await refine_customers()

    elif element == "RefineCustomersCode":
        code_enrich_customers().open()

    elif element == "BronzeCustomers":
        iceberg_table_tail(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS)

    elif element == "SilverCustomers":
        peek_documents(tablepath=f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}")

    elif element == "SilverTransactions":
        peek_documents(tablepath=f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}")

    elif element == "SilverProfiles":
        peek_documents(tablepath=f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}")

    elif element == "Consolidate":
        await create_golden()

    elif element == "ConsolidateCode":
        code_create_golden().open()

    elif element == "CheckFraud":
        await fraud_detection()

    elif element == "GetScoreCode":
        code_getscore().open()

    elif element == "GoldCustomers":
        await peek_sqlrecords([TABLE_FRAUD, TABLE_TRANSACTIONS, TABLE_CUSTOMERS])

    elif element == "ReportView":
        ui.navigate.to(
            app.storage.general.get("DASHBOARD_URL", "about:blank"),
            new_tab=True,
        )

    else:
        logger.warning(element)
        ui.notify(f"{element} not configured yet")

    app.storage.general["busy"] = False

# Interactive Images


async def mesh_ii():
    """Draw an interactive image that shows the data mesh architecture"""

    layer_color = "green"
    opacity = "0.10"
    rest_of_svg = f'fill={layer_color} fill-opacity={opacity} stroke="none" stroke-width:"0" pointer-events="all" cursor="pointer"'

    with ui.interactive_image(
        DIAGRAM,
        content=f"""
        <rect id="NFS" x="15" y="10" rx="30" ry="30" width="465" height="210" {rest_of_svg} />
        <rect id="Fraud" x="718" y="130" rx="60" ry="60" width="1345" height="1020" {rest_of_svg} />
        <rect id="S3" x="7070" y="70" rx="30" ry="30" width="415" height="150" {rest_of_svg} />
        <rect id="IAM" x="670" y="1380" rx="30" ry="30" width="450" height="250" {rest_of_svg} />
        <rect id="Policies" x="5745" y="1365" rx="90" ry="30" width="495" height="280" {rest_of_svg} />
        <rect id="Catalogue" x="6290" y="1365" rx="90" ry="30" width="590" height="280" {rest_of_svg} />
        <rect id="Edge" x="250" y="2810" width="860" height="280" {rest_of_svg} />
        # """,
        # ).on("svg:pointerover", lambda e: open_popup_for(e.args["element_id"])).classes(
    ).on(
        "svg:pointerup",
        lambda e: handle_image_action(e.args),
        # ).on("svg:pointerover", lambda e: handle_image_info(e.args)
    ).classes(
        "relative m-0 p-0"
    ):
        # if External S3 is configured and transactions csv exist
        async def s3_upload():
            transactions_file = (
                f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv"
            )
            if (
                os.path.lexists(transactions_file)
                and len(app.storage.general.get("S3_SECRET_KEY", "")) > 0
            ):
                ui.notify(
                    f"Copying {TABLE_TRANSACTIONS} to external S3 bucket", type="success"
                )
                await upload_to_s3(transactions_file)

        def nfs_upload():
            # Copy customers if NFS is mounted and customers csv exist
            customers_file = (
                f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv"
            )
            if os.path.lexists(customers_file) and os.path.exists(
                f"/mnt{EXTERNAL_NFS_PATH}"
            ):
                ui.notify(
                    f"Copying {TABLE_CUSTOMERS} to external NFS server", type="success"
                )
                logger.info(
                    shutil.copyfile(
                        customers_file, f"/mnt{EXTERNAL_NFS_PATH}/{TABLE_CUSTOMERS}"
                    )
                )

        ui.button(icon="upload", on_click=nfs_upload).classes(
            "absolute top-10 left-5"
        ).props("flat").bind_visibility_from(
            app.storage.general, "NFS_SERVER", backward=lambda x: len(x) > 0
        ).tooltip(
            "Upload Customers"
        )

        ui.button(icon="upload", on_click=s3_upload).classes(
            "absolute top-10 right-3"
        ).props("flat").bind_visibility_from(
            app.storage.general, "S3_SERVER", backward=lambda x: len(x) > 0
        ).tooltip(
            "Upload Transactions"
        )

@ui.page(f"/{DATA_PRODUCT}", dark=False, title="Fraud Data Domain")
async def domain_ii():
    """Draw an interactive image that shows demo pipeline for the data domain"""

    action_color = HPE_COLORS["purple"]
    secondary_action_color = HPE_COLORS["orange"]
    code_color = HPE_COLORS["teal"]
    info_color = HPE_COLORS["darkblue"]
    opacity = "0.3"
    rest_of_svg = f'fill-opacity={opacity} stroke="none" stroke-linecap="round" stroke-width:"0" pointer-events="all" cursor="pointer"'

    # with ui.row().classes("w-full flex flex-nowrap relative"):
    with theme.frame(DATA_PRODUCT):

        svg_overlay = f"""
            <rect id="PublishTransactionsCode" x="200" y="1560" rx="60" ry="60" width="350" height="350" fill={code_color} {rest_of_svg} />
            <rect id="PublishTransactions" x="580" y="1685" rx="40" ry="40" width="330" height="100" fill={action_color} {rest_of_svg} />
        #"""
        # <rect id="CreateTransactions" x="210" y="1555" rx="80" ry="80" width="350" height="340" fill={action_color} {rest_of_svg} />
        # <rect id="CreateTransactionsCode" x="580" y="1690" rx="20" ry="20" width="330" height="80" fill={code_color} {rest_of_svg} />
        # <rect id="CreateCustomers" x="210" y="2500" rx="80" ry="80" width="350" height="340" fill={action_color} {rest_of_svg} />
        # <rect id="CreateCustomersCode" x="580" y="2620" rx="20" ry="20" width="330" height="80" fill={code_color} {rest_of_svg} />
        # <g>
        #     <rect id="PublishTransactions" x="580" y="1520" rx="40" ry="40" width="330" height="150" fill={secondary_action_color} {rest_of_svg} />
        #     <text x="635" y="1620" font-family="Verdana" font-size="60" fill="blue">Publish</text>
        #     <rect id="PublishTransactionsCode" x="860" y="1550" rx="20" ry="20" width="140" height="90" fill={code_color} {rest_of_svg} />
        #     <text x="865" y="1610" font-family="Verdana" font-size="50" fill="blue">< /></text>
        # </g>
        # <rect id="NifiStreams" x="980" y="1580" rx="30" ry="30" width="435" height="266" fill={action_color} {rest_of_svg} />
        # <rect id="NifiStreamsCode" x="1430" y="1695" rx="20" ry="20" width="630" height="80" fill={code_color} {rest_of_svg} />
        # <rect id="IngestTransactions" x="1050" y="1860" rx="30" ry="30" width="260" height="170" fill={secondary_action_color} {rest_of_svg} />
        # <rect id="IngestTransactionsCode" x="1260" y="1930" rx="20" ry="20" width="140" height="90" fill={code_color} {rest_of_svg} />
        # <rect id="IngestCustomersIceberg" x="1070" y="2830" rx="30" ry="30" width="260" height="160" fill={secondary_action_color} {rest_of_svg} />
        # <rect id="IngestCustomersIcebergCode" x="1260" y="2930" rx="20" ry="20" width="140" height="90" fill={code_color} {rest_of_svg} />
        # <rect id="AirflowBatch" x="970" y="2540" rx="30" ry="30" width="431" height="268" fill={action_color} {rest_of_svg} />
        # <rect id="AirflowBatchCode" x="1430" y="2640" rx="20" ry="20" width="600" height="80" fill={code_color} {rest_of_svg} />
        # <rect id="ProfileBuilderCode" x="1760" y="820" rx="0" ry="20" width="1520" height="80" fill={code_color} {rest_of_svg} />
        # <rect id="ProfileBuilderCode" x="1690" y="820" rx="20" ry="20" width="80" height="870" fill={code_color} {rest_of_svg} />
        # <rect id="BronzeTransactions" x="2070" y="1450" rx="30" ry="30" width="350" height="430" fill={info_color} {rest_of_svg} />
        # <rect id="RefineTransactionsCode" x="2440" y="1690" rx="20" ry="20" width="830" height="80" fill={code_color} {rest_of_svg} />
        # <rect id="RefineTransactions" x="2590" y="1630" rx="30" ry="30" width="240" height="200" fill={action_color} {rest_of_svg} />
        # <rect id="BronzeCustomers" x="2060" y="2460" rx="30" ry="30" width="350" height="410" fill={info_color} {rest_of_svg} />
        # <rect id="RefineCustomersCode" x="2450" y="2630" rx="20" ry="20" width="830" height="80" fill={code_color} {rest_of_svg} />
        # <rect id="RefineCustomers" x="2590" y="2570" rx="30" ry="30" width="240" height="200" fill={action_color} {rest_of_svg} />
        # <rect id="SilverCustomers" x="3330" y="2470" rx="30" ry="30" width="350" height="410" fill={info_color} {rest_of_svg} />
        # <rect id="SilverTransactions" x="3340" y="1550" rx="30" ry="30" width="320" height="380" fill={action_color} {rest_of_svg} />
        # <rect id="SilverProfiles" x="3360" y="770" rx="30" ry="30" width="300" height="360" fill={action_color} {rest_of_svg} />
        # <rect id="GetScoreCode" x="3800" y="825" rx="0" ry="20" width="355" height="80" fill={code_color} {rest_of_svg} />
        # <rect id="GetScoreCode" x="4080" y="905" rx="20" ry="0" width="80" height="430" fill={code_color} {rest_of_svg} />
        # <rect id="ConsolidateCode" x="3750" y="2630" rx="20" ry="20" width="750" height="80" fill={code_color} {rest_of_svg} />
        # <rect id="Consolidate" x="4000" y="2180" rx="30" ry="30" width="240" height="600" fill={action_color} {rest_of_svg} />
        # <rect id="CheckFraud" x="3970" y="1330" rx="30" ry="30" width="300" height="260" fill={action_color} {rest_of_svg} />
        # <rect id="GoldCustomers" x="4530" y="2400" rx="30" ry="30" width="350" height="410" fill={info_color} {rest_of_svg} />
        # <rect id="ReportView" x="5810" y="2500" rx="30" ry="30" width="390" height="265" fill={info_color} {rest_of_svg} />

        with ui.interactive_image(
            DATA_DOMAIN["diagram"],
            content=svg_overlay,
        ).on(
            "svg:pointerup",
            lambda e: handle_image_action(e.args),
            ).on("svg:pointerover", lambda e: handle_image_info(e.args)
        ).classes(
            "relative"
        ).props(
            "fit=scale-down"
        ):
            with ui.list().props("bordered dense").classes("w-80 absolute top-10 left-2"):
                ui.item_label("Source data").props("header").classes("text-bold")
                ui.separator()
                with ui.row().classes("w-full no-wrap p-0"):
                    with ui.button_group().props("flat"):
                        ui.button(icon="o_library_add", on_click=create_customers).classes("mx-0 px-1").props("flat")
                        ui.button(icon="o_integration_instructions", on_click=code_create_customers).classes("mx-0 px-1").props("flat")
                    with ui.item(on_click=peek_mocked_customers).props("dense").classes("mx-0 gx-0"):
                        with ui.item_section():
                            ui.item_label(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv")

                with ui.row().classes("w-full no-wrap p-0"):
                    with ui.button_group().props("flat"):
                        ui.button(icon="o_library_add", on_click=create_transactions).classes("mx-0 px-1").props("flat")
                        ui.button(icon="o_integration_instructions", on_click=code_create_transactions).classes("mx-0 px-1").props("flat")
                    with ui.item(on_click=peek_mocked_transactions).props("dense").classes("mx-0 gx-0"):
                        with ui.item_section():
                            ui.item_label(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv")


                # with ui.row(wrap=True, align_items="start"):
                #     ui.button(
                #         f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv",
                #         on_click=peek_mocked_customers,
                #     ).props("flat").classes("lowercase").tooltip("Mocked customers")

                #     ui.button(
                #         f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv",
                #         on_click=peek_mocked_transactions,
                #     ).props("flat").classes("lowercase").tooltip("Mocked transactions")

    # Block interaction when working
    with ui.dialog().props("persistent").bind_value_from(
        app.storage.general, "busy"
    ), ui.card():
        ui.spinner("ios", color="red", size="3em")

    # # Run monitoring for metrics
    # ui.timer(MON_REFRESH_INTERVAL10, monitoring_metrics)


async def open_dialog(awaitable_content):
    with ui.dialog().props("full-width full-height") as dialog, ui.card().classes(
        "relative"
    ):
        ui.button(icon="close", on_click=dialog.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        await awaitable_content()

    dialog.open()

# async def domain_page():
#     with ui.dialog().props("full-width full-height") as dialog, ui.card().classes(
#         "relative"
#     ):
#         ui.button(icon="close", on_click=dialog.close).props(
#             "flat round dense"
#         ).classes("absolute right-2 top-2")

#         # ui.label(
#         #     "End to end data pipeline using Ezmeral Data Fabric for financial transaction processing"
#         # )
#         # ui.markdown(DATA_DOMAIN["description"]).classes("font-normal")
#         # ui.image(f"/images/{DATA_DOMAIN['diagram']}").classes("object-scale-down g-10")
#         await domain_ii()


#     dialog.open()


def metric_badges_on_ii():
    """
    Place badges with counters in real-time as they are updated by monitoring_metrics()
    """

    # raw counts
    ui.badge("0", color="teal").bind_text_from(
        app.storage.general,
        "raw_transactions",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[250px] left-[100px]")
    ui.badge("0", color="orange").bind_text_from(
        app.storage.general,
        "raw_customers",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[420px] left-[100px]")

    # ingest counts
    ui.badge("0", color="lightteal").bind_text_from(
        app.storage.general,
        "ingest_transactions_published",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[250px] left-[280px]").tooltip("published transactions")
    ui.badge("0", color="teal").bind_text_from(
        app.storage.general,
        "ingest_transactions_consumed",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[280px] left-[280px]").tooltip("consumed transactions")
    ui.badge("0", color="orange").bind_text_from(
        app.storage.general,
        "ingest_customers",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[460px] left-[280px]").tooltip("# of customers")

    # bronze counts
    ui.badge("0", color="teal").bind_text_from(
        app.storage.general,
        "bronze_transactions",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[280px] left-[450px]").tooltip("# of transactions")
    ui.badge("0", color="orange").bind_text_from(
        app.storage.general,
        "bronze_customers",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[450px] left-[450px]").tooltip("# of customers")

    # silver counts
    ui.badge("0", color="darkturquoise").bind_text_from(
        app.storage.general,
        "silver_profiles",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[140px] left-[680px]").tooltip("# of profiles")
    ui.badge("0", color="teal").bind_text_from(
        app.storage.general,
        "silver_transactions",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[280px] left-[680px]").tooltip("# of transactions")
    ui.badge("0", color="orange").bind_text_from(
        app.storage.general,
        "silver_customers",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[450px] left-[680px]").tooltip("# of customers")

    # gold counts
    ui.badge("0", color="red").bind_text_from(
        app.storage.general,
        "gold_fraud",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[420px] left-[870px]").tooltip("# of fraud")
    ui.badge("0", color="teal").bind_text_from(
        app.storage.general,
        "gold_transactions",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[440px] left-[870px]").tooltip("# of transactions")
    ui.badge("0", color="orange").bind_text_from(
        app.storage.general,
        "gold_customers",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[460px] left-[870px]").tooltip("# of customers")


# NOT USED
def add_elements_to_svg(x: int, y: int, color: str, metric: str):
    """
    HTML element to insert into SVG
    # x/y placement not working for nicegui elements
    :param x int: pixels from left
    :param y int: pixels from top
    :param color str: color of the badge
    :param metric str: badge count to read from app.storage.general

    """
    return f"""
        <foreignObject x="{x}" y="{y}">
            {
                ui.badge("0", color=color).bind_text_from(
                    app.storage.general,
                    metric,
                    lambda x: x if x is not None else 0,
                )
            }
        </foreignObject>
    """
