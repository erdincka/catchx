import inspect
import logging
import re
from nicegui import ui, app

from common import *
from functions import *
from ingestion import *
from mock import *
from monitoring import monitoring_metrics
import streams
import iceberger

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
    elif element == "IngestStreams":
        label = "Process incoming transactions with stream consumer."
    elif element == "IngestBatch":
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
def code_publish():
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


def code_generate():
    with ui.dialog().props("full-width") as generate_codeview, ui.card().classes(
        "grow relative"
    ):
        ui.button(icon="close", on_click=generate_codeview.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        ui.code(inspect.getsource(create_csv_files)).classes("w-full mt-6")
        ui.code(inspect.getsource(fake_customer)).classes("w-full")
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
    logger.debug(e)
    element = e["element_id"]

    if element == "Fraud":
        await domain_page()
    # TODO: add information or configuration pages!
    elif element == "NFS":
        ui.notify("Bring existing data lakes into the Global Namespace.", type="info")
    elif element == "S3":
        ui.notify(
            "Bring existing object stores into the Global Namespace.", type="info"
        )
    elif element == "IAM":
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
    elif element == "PublishTransactions":
        app.storage.user["busy"] = True
        await publish_transactions()
        app.storage.user["busy"] = False

    elif element == "PublishTransactionsCode":
        code_publish().open()

    elif element == "CreateData":
        app.storage.user["busy"] = True
        create_csv_files()
        app.storage.user["busy"] = False

    elif element == "CreateDataCode":
        code_generate().open()

    elif element == "IngestBatch":
        app.storage.user["busy"] = True
        await ingest_customers_iceberg()
        app.storage.user["busy"] = False
        # ui.button("History", on_click=lambda: iceberg_table_history(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS)).props("outline")

    elif element == "IngestBatchCode":
        code_batch().open()

    elif element == "IngestStreams":
        app.storage.user["busy"] = True
        await ingest_transactions()
        app.storage.user["busy"] = False

    elif element == "IngestStreamsCode":
        code_stream().open()

    elif element == "ProfileBuilderCode":
        code_profilebuilder().open()

    elif element == "RefineTransactions":
        app.storage.user["busy"] = True
        await refine_transactions()
        app.storage.user["busy"] = False

    elif element == "RefineTransactionsCode":
        code_enrich_transactions().open()

    elif element == "BronzeTransactions":
        peek_documents(f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}")

    elif element == "RefineCustomers":
        app.storage.user["busy"] = True
        await refine_customers()
        app.storage.user["busy"] = False

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
        app.storage.user["busy"] = True
        await create_golden()
        app.storage.user["busy"] = False

    elif element == "ConsolidateCode":
        code_create_golden().open()

    elif element == "CheckFraud":
        app.storage.user["busy"] = True
        await fraud_detection()
        app.storage.user["busy"] = False

    elif element == "GoldCustomers":
        peek_sqlrecords([TABLE_FRAUD, TABLE_TRANSACTIONS, TABLE_CUSTOMERS])

    elif element == "ReportView":
        ui.navigate.to(
            "https://superset.ua.kaya.home/superset/dashboard/p/vLp7yv8xkez/",
            new_tab=True,
        )

    else:
        logger.warning(element)
        ui.notify(f"{element} not configured yet")


# Interactive Images


def mesh_ii():
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
        "relative"
    ):

        # TODO: replace this with a view for documentation
        # ui.button(on_click=lambda: ui.notify("thumbs up"), icon="thumb_up").props(
        #     "flat fab color=blue"
        # ).classes("absolute bottom-0 right-0")
        pass


async def domain_ii():
    """Draw an interactive image that shows demo pipeline for the data domain"""

    action_color = "darkorange"
    code_color = "powderblue"
    info_color = "deepskyblue"
    opacity = "0.35"
    rest_of_svg = f'fill-opacity={opacity} stroke="none" stroke-linecap="round" stroke-width:"0" pointer-events="all" cursor="pointer"'

    with ui.row().classes("w-full flex flex-nowrap relative"):

        with ui.interactive_image(
            DATA_DOMAIN["diagram"],
            content=f"""
            <rect id="PublishTransactions" x="200" y="1460" rx="80" ry="80" width="350" height="350" fill={action_color} {rest_of_svg} />
            <rect id="PublishTransactionsCode" x="600" y="1595" rx="20" ry="20" width="330" height="80" fill={code_color} {rest_of_svg} />
            <rect id="CreateData" x="200" y="2390" rx="80" ry="80" width="350" height="350" fill={action_color} {rest_of_svg} />
            <rect id="CreateDataCode" x="600" y="2530" rx="20" ry="20" width="330" height="80" fill={code_color} {rest_of_svg} />
            <rect id="IngestStreams" x="984" y="1485" rx="30" ry="30" width="435" height="266" fill={action_color} {rest_of_svg} />
            <rect id="IngestStreamsCode" x="1430" y="1595" rx="20" ry="20" width="630" height="80" fill={code_color} {rest_of_svg} />
            <rect id="IngestBatch" x="970" y="2450" rx="30" ry="30" width="431" height="270" fill={action_color} {rest_of_svg} />
            <rect id="IngestBatchCode" x="1430" y="2550" rx="20" ry="20" width="600" height="80" fill={code_color} {rest_of_svg} />
            <rect id="ProfileBuilderCode" x="1760" y="730" rx="0" ry="20" width="1520" height="80" fill={code_color} {rest_of_svg} />
            <rect id="ProfileBuilderCode" x="1680" y="730" rx="20" ry="20" width="80" height="870" fill={code_color} {rest_of_svg} />
            <rect id="BronzeTransactions" x="2075" y="1350" rx="30" ry="30" width="350" height="430" fill={info_color} {rest_of_svg} />
            <rect id="RefineTransactionsCode" x="2450" y="1600" rx="20" ry="20" width="830" height="80" fill={code_color} {rest_of_svg} />
            <rect id="RefineTransactions" x="2590" y="1530" rx="30" ry="30" width="240" height="200" fill={action_color} {rest_of_svg} />
            <rect id="BronzeCustomers" x="2065" y="2370" rx="30" ry="30" width="350" height="410" fill={info_color} {rest_of_svg} />
            <rect id="RefineCustomersCode" x="2450" y="2530" rx="20" ry="20" width="830" height="80" fill={code_color} {rest_of_svg} />
            <rect id="RefineCustomers" x="2590" y="2470" rx="30" ry="30" width="240" height="200" fill={action_color} {rest_of_svg} />
            <rect id="SilverCustomers" x="3330" y="2370" rx="30" ry="30" width="350" height="410" fill={info_color} {rest_of_svg} />
            <rect id="SilverTransactions" x="3340" y="1450" rx="30" ry="30" width="320" height="380" fill={action_color} {rest_of_svg} />
            <rect id="SilverProfiles" x="3360" y="670" rx="30" ry="30" width="300" height="360" fill={action_color} {rest_of_svg} />
            <rect id="GetScoreCode" x="3800" y="725" rx="0" ry="20" width="355" height="80" fill={code_color} {rest_of_svg} />
            <rect id="GetScoreCode" x="4080" y="805" rx="20" ry="0" width="80" height="430" fill={code_color} {rest_of_svg} />
            <rect id="ConsolidateCode" x="3750" y="2530" rx="20" ry="20" width="750" height="80" fill={code_color} {rest_of_svg} />
            <rect id="Consolidate" x="4000" y="2080" rx="30" ry="30" width="240" height="600" fill={action_color} {rest_of_svg} />
            <rect id="CheckFraud" x="3970" y="1230" rx="30" ry="30" width="300" height="280" fill={action_color} {rest_of_svg} />
            <rect id="GoldCustomers" x="4530" y="2300" rx="30" ry="30" width="350" height="410" fill={info_color} {rest_of_svg} />
            <rect id="ReportView" x="5805" y="2400" rx="30" ry="30" width="400" height="280" fill={info_color} {rest_of_svg} />
            # """,
        ).on(
            "svg:pointerup",
            lambda e: handle_image_action(e.args),
            # ).on("svg:pointerover", lambda e: handle_image_info(e.args)
        ).classes(
            "relative"
        ):
            ui.button(on_click=peek_mocked_data, icon="source").props(
                'flat round size="2em" color="primary"'
            ).classes("absolute top-10 left-2").tooltip("Source data")

            # raw counts
            ui.badge("0", color="teal").bind_text_from(
                app.storage.general,
                "raw_transactions",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[330px] left-[120px]")
            ui.badge("0", color="orange").bind_text_from(
                app.storage.general,
                "raw_customers",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[545px] left-[120px]")

            # ingest counts
            ui.badge("0", color="lightteal").bind_text_from(
                app.storage.general,
                "ingest_transactions_published",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[335px] left-[320px]").tooltip("published transactions")
            ui.badge("0", color="teal").bind_text_from(
                app.storage.general,
                "ingest_transactions_consumed",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[355px] left-[320px]").tooltip("consumed transactions")
            ui.badge("0", color="orange").bind_text_from(
                app.storage.general,
                "ingest_customers",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[560px] left-[320px]").tooltip("# of customers")

            # bronze counts
            ui.badge("0", color="teal").bind_text_from(
                app.storage.general,
                "bronze_transactions",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[305px] left-[555px]").tooltip("# of transactions")
            ui.badge("0", color="orange").bind_text_from(
                app.storage.general,
                "bronze_customers",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[545px] left-[550px]").tooltip("# of customers")

            # silver counts
            ui.badge("0", color="darkturquoise").bind_text_from(
                app.storage.general,
                "silver_profiles",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[145px] left-[840px]").tooltip("# of profiles")
            ui.badge("0", color="teal").bind_text_from(
                app.storage.general,
                "silver_transactions",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[330px] left-[840px]").tooltip("# of transactions")
            ui.badge("0", color="orange").bind_text_from(
                app.storage.general,
                "silver_customers",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[545px] left-[845px]").tooltip("# of customers")

            # gold counts
            ui.badge("0", color="red").bind_text_from(
                app.storage.general,
                "gold_fraud",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[555px] left-[1130px]").tooltip("# of fraud")
            ui.badge("0", color="teal").bind_text_from(
                app.storage.general,
                "gold_transactions",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[575px] left-[1130px]").tooltip("# of transactions")
            ui.badge("0", color="orange").bind_text_from(
                app.storage.general,
                "gold_customers",
                lambda x: x if x is not None else 0,
            ).classes("absolute top-[595px] left-[1130px]").tooltip("# of customers")

    # Block interaction when working
    with ui.dialog().props("persistent").bind_value_from(
        app.storage.user, "busy"
    ), ui.card():
        ui.spinner("ios", color="red", size="3em")

    ui.timer(MON_REFRESH_INTERVAL5, monitoring_metrics)

async def domain_page():
    with ui.dialog().props("full-width full-height") as dialog, ui.card().classes(
        "relative"
    ):
        ui.button(icon="close", on_click=dialog.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")

        ui.label(
            "End to end data pipeline using Ezmeral Data Fabric for financial transaction processing"
        )
        ui.markdown(DATA_DOMAIN["description"]).classes("font-normal")
        # ui.image(f"/images/{DATA_DOMAIN['diagram']}").classes("object-scale-down g-10")
        await domain_ii()


    dialog.open()
