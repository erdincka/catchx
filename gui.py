import inspect
import logging
from math import pi
from nicegui import ui, app

from common import *
from functions import *
from ingestion import *
from mock import *
import streams
import iceberger

logger = logging.getLogger("gui")


active_dialog = None


def handle_image_info(e: events.MouseEventArguments):
    logger.debug(e)
    # eventargs is a dict with: type, element_id, image_x, image_y keys.
    element = e["element_id"]

    # Regex to convert "CamelCase" to "Title Case"
    title = " ".join(re.findall(r'[A-Z](?:[a-z]+|[A-Z]*(?=[A-Z]|$))', element))

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
    ui.notify(message=title, caption=label, type="info", color="secondary", progress=True, multi_line=True, timeout=2000)

# dialogs for various phases
# automatically clear the content for other dialogs
def code_publish():
    with ui.dialog().props("full-width") as publish_codeview, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=publish_codeview.close).props("flat round dense").classes("absolute right-2 top-2")
        ui.code(inspect.getsource(publish_transactions)).classes("w-full mt-6")
        ui.code(inspect.getsource(streams.produce)).classes("w-full")
    publish_codeview.on("close", lambda d=publish_codeview: d.delete())
    return publish_codeview


def code_generate():
    with ui.dialog().props("full-width") as generate_codeview, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=generate_codeview.close).props("flat round dense").classes("absolute right-2 top-2")
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
        ui.code(inspect.getsource(upsert_profile)).classes("w-full")
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
        ui.code(inspect.getsource(refine_transactions)).classes("w-full")
    enrich_codeview.on("close", lambda d=enrich_codeview: d.delete())
    return enrich_codeview


async def handle_image_action(e: events.MouseEventArguments):
    logger.debug(e)
    element = e["element_id"]

    if element == "Fraud": domain_page()
    # TODO: add information or configuration pages!
    elif element == "NFS": ui.notify(element)
    elif element == "S3": ui.notify(element)
    elif element == "IAM": ui.notify(element)
    elif element == "Policies": ui.notify(element)
    elif element == "Catalog": ui.notify(element)
    elif element == "Edge": ui.notify(element)

    # Data Domain functions
    elif element == "PublishTransactions":
        await publish_transactions()

    elif element == "PublishTransactionsCode":
        code_publish().open()

    elif element == "CreateData":
        create_csv_files()

    elif element == "CreateDataCode":
        code_generate().open()

    elif element == "IngestBatch":
        await ingest_customers_iceberg()
        # ui.button("History", on_click=lambda: iceberg_table_history(tier=VOLUME_BRONZE, tablename=TABLE_CUSTOMERS)).props("outline")

    elif element == "IngestBatchCode":
        code_batch().open()

    elif element == "IngestStreams":
        await ingest_transactions()

    elif element == "IngestStreamsCode":
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
    
    elif element == "SilverProfiles":
        peek_documents(tablepath=f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}")

    else: 
        logger.warning(element)
        ui.notify("Nothing here!")


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
    ).on("svg:pointerup", lambda e: handle_image_action(e.args)
    # ).on("svg:pointerover", lambda e: handle_image_info(e.args)
    ).classes(
        "relative"
    ):

        # TODO: replace this with a view for documentation
        # ui.button(on_click=lambda: ui.notify("thumbs up"), icon="thumb_up").props(
        #     "flat fab color=blue"
        # ).classes("absolute bottom-0 right-0")
        pass


def domain_ii():
    """Draw an interactive image that shows demo pipeline for the data domain"""

    action_color = "red"
    code_color = "cyan"
    info_color = "blue"
    opacity = "0.35"
    rest_of_svg = f'fill-opacity={opacity} stroke="none" stroke-linecap="round" stroke-width:"0" pointer-events="all" cursor="pointer"'

    with ui.interactive_image(
        DATA_DOMAIN["diagram"],
        content=f"""
        <rect id="PublishTransactions" x="200" y="1460" rx="80" ry="80" width="350" height="350" fill={action_color} {rest_of_svg} />
        <rect id="PublishTransactionsCode" x="600" y="1585" rx="20" ry="20" width="330" height="100" fill={code_color} {rest_of_svg} />
        <rect id="CreateData" x="200" y="2390" rx="80" ry="80" width="350" height="350" fill={action_color} {rest_of_svg} />
        <rect id="CreateDataCode" x="600" y="2520" rx="20" ry="20" width="330" height="100" fill={code_color} {rest_of_svg} />
        <rect id="IngestStreams" x="984" y="1485" rx="30" ry="30" width="435" height="266" fill={action_color} {rest_of_svg} />
        <rect id="IngestStreamsCode" x="1430" y="1585" rx="20" ry="20" width="630" height="100" fill={code_color} {rest_of_svg} />
        <rect id="IngestBatch" x="970" y="2450" rx="30" ry="30" width="431" height="270" fill={action_color} {rest_of_svg} />
        <rect id="IngestBatchCode" x="1430" y="2540" rx="20" ry="20" width="600" height="100" fill={code_color} {rest_of_svg} />
        <rect id="ProfileBuilderCode" x="1700" y="725" rx="20" ry="20" width="360" height="100" fill={code_color} {rest_of_svg} />
        <rect id="BronzeTransactions" x="2075" y="1350" rx="30" ry="30" width="350" height="430" fill={action_color} {rest_of_svg} />
        <rect id="RefineTransactionsCode" x="2450" y="1590" rx="20" ry="20" width="830" height="100" fill={code_color} {rest_of_svg} />
        <rect id="RefineTransactions" x="2590" y="1530" rx="30" ry="30" width="240" height="200" fill={action_color} {rest_of_svg} />
        <rect id="BronzeCustomers" x="2065" y="2370" rx="30" ry="30" width="350" height="410" fill={action_color} {rest_of_svg} />
        <rect id="RefineCustomersCode" x="2450" y="2520" rx="20" ry="20" width="830" height="100" fill={code_color} {rest_of_svg} />
        <rect id="RefineCustomers" x="2590" y="2470" rx="30" ry="30" width="240" height="200" fill={action_color} {rest_of_svg} />
        <rect id="SilverCustomers" x="3330" y="2370" rx="30" ry="30" width="350" height="410" fill={action_color} {rest_of_svg} />
        <rect id="SilverTransactions" x="3340" y="1450" rx="30" ry="30" width="320" height="380" fill={action_color} {rest_of_svg} />
        <rect id="SilverProfiles" x="3360" y="670" rx="30" ry="30" width="300" height="360" fill={action_color} {rest_of_svg} />
        <rect id="ConsolidateCustomers" x="4000" y="2470" rx="30" ry="30" width="240" height="200" fill={action_color} {rest_of_svg} />
        <rect id="ConsolidateTransactions" x="4000" y="2080" rx="30" ry="30" width="240" height="200" fill={action_color} {rest_of_svg} />
        <rect id="CheckFraud" x="3960" y="1200" rx="30" ry="30" width="340" height="300" fill={action_color} {rest_of_svg} />
        <rect id="GoldCustomers" x="4530" y="2300" rx="30" ry="30" width="350" height="410" fill={action_color} {rest_of_svg} />
        <rect id="ReportView" x="5805" y="2400" rx="30" ry="30" width="400" height="280" fill={action_color} {rest_of_svg} />
        # """,
    ).on("svg:pointerup", lambda e: handle_image_action(e.args)
    # ).on("svg:pointerover", lambda e: handle_image_info(e.args)
    ):
        ui.button(on_click=peek_mocked_data, icon="source").props(
            'flat round size="2em" color="secondary"'
        ).classes("absolute bottom-10 left-0").tooltip("Source data")


def datamesh3d():
    with ui.row().classes("w-full"):
        with ui.scene(background_color="#222").on_click(lambda x: print(x)).classes("h-90") as scene:
            scene.box().move(-3,-2).with_name("NFSv4")
            scene.extrusion([[0, 0], [0, 1], [1, 1]], 1).material("#ff8888").move(
                2, -1
            )
            scene.sphere().material("#4488ff")
            scene.cylinder(1, 1, 1, 32).material("#ff8800", opacity=0.5).move(
                -2, 1
            ).rotate(pi/2, 0, 0)

            scene.line([-4, 0, 0], [-4, 2, 0]).material("#ff0000")
            # logo = 'https://avatars.githubusercontent.com/u/2843826'
            # scene.texture(logo, [[[0.5, 2, 0], [2.5, 2, 0]],
            #                     [[0.5, 0, 0], [2.5, 0, 0]]]).move(1, -3)

            # teapot = 'https://upload.wikimedia.org/wikipedia/commons/9/93/Utah_teapot_(solid).stl'
            # scene.stl(teapot).scale(0.2).move(-3, 4)

            # avocado = 'https://raw.githubusercontent.com/KhronosGroup/glTF-Sample-Assets/main/Models/Avocado/glTF-Binary/Avocado.glb'
            # scene.gltf(avocado).scale(40).move(-2, -3, 0.5)

            scene.text('Data Hub', 'background: rgba(0, 0, 0, 0.2); border-radius: 5px; padding: 5px').move(z=2)
            # scene.text3d('3D', 'background: rgba(0, 0, 0, 0.2); border-radius: 5px; padding: 5px').move(y=-2).scale(.05)


def domain_page():
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
        domain_ii()

    dialog.open()
