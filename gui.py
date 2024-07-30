import logging
from nicegui import ui, app

from common import *
from codeviewers import *
from functions import *
from mock import *
from monitoring import *


logger = logging.getLogger("gui")

action_color = HPE_COLORS["purple"]
secondary_action_color = HPE_COLORS["orange"]
code_color = HPE_COLORS["teal"]
info_color = HPE_COLORS["darkblue"]
opacity = "0.3"
rest_of_svg = f'fill-opacity={opacity} stroke="none" stroke-linecap="round" stroke-width:"0" pointer-events="all" cursor="pointer"'

### Not used anymore
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
# <rect id="ProfileBuilder" x="1730" y="820" rx="0" ry="20" width="330" height="80" fill={code_color} {rest_of_svg} />
# <rect id="ProfileBuilderCode" x="1690" y="820" rx="20" ry="20" width="80" height="870" fill={code_color} {rest_of_svg} />
# <rect id="GetScoreCode" x="3800" y="825" rx="0" ry="20" width="310" height="80" fill={code_color} {rest_of_svg} />
# <rect id="GetScoreCode" x="4080" y="905" rx="20" ry="0" width="80" height="430" fill={code_color} {rest_of_svg} />

svg_overlay = f"""
    <rect id="PublishTransactions" x="580" y="1685" rx="40" ry="40" width="330" height="100" fill={action_color} {rest_of_svg} />
    <rect id="PublishTransactionsCode" x="200" y="1560" rx="60" ry="60" width="350" height="350" fill={code_color} {rest_of_svg} />
    <rect id="NifiStreams" x="1430" y="1695" rx="20" ry="20" width="300" height="80" fill={secondary_action_color} {rest_of_svg} />
    <rect id="NifiStreamsCode" x="983" y="1585" rx="20" ry="20" width="432" height="266" fill={code_color} {rest_of_svg} />
    <rect id="IngestTransactions" x="1340" y="1900" rx="20" ry="20" width="380" height="90" fill={action_color} {rest_of_svg} />
    <rect id="IngestTransactionsCode" x="1070" y="1860" rx="20" ry="20" width="260" height="180" fill={code_color} {rest_of_svg} />
    <rect id="IngestCustomersIceberg" x="1350" y="2890" rx="20" ry="20" width="290" height="90" fill={action_color} {rest_of_svg} />
    <rect id="IngestCustomersIcebergCode" x="1070" y="2830" rx="20" ry="20" width="260" height="180" fill={code_color} {rest_of_svg} />
    <rect id="AirflowBatch" x="1430" y="2645" rx="20" ry="20" width="220" height="80" fill={secondary_action_color} {rest_of_svg} />
    <rect id="AirflowBatchCode" x="970" y="2553" rx="20" ry="20" width="431" height="260" fill={code_color} {rest_of_svg} />
    <rect id="BronzeTransactions" x="2070" y="1450" rx="20" ry="20" width="350" height="430" fill={info_color} {rest_of_svg} />
    <rect id="BronzeCustomers" x="2060" y="2460" rx="20" ry="20" width="350" height="410" fill={info_color} {rest_of_svg} />
    <rect id="SilverCustomers" x="3330" y="2470" rx="20" ry="20" width="350" height="410" fill={info_color} {rest_of_svg} />
    <rect id="SilverTransactions" x="3340" y="1550" rx="20" ry="20" width="320" height="380" fill={info_color} {rest_of_svg} />
    <rect id="SilverProfiles" x="3360" y="770" rx="20" ry="20" width="300" height="360" fill={info_color} {rest_of_svg} />
    <rect id="GoldCustomers" x="4530" y="2400" rx="20" ry="20" width="350" height="410" fill={info_color} {rest_of_svg} />
    <rect id="ProfileBuilderCode" x="2590" y="770" rx="20" ry="20" width="240" height="200" fill={code_color} {rest_of_svg} />
    <rect id="RefineTransactions" x="2840" y="1690" rx="20" ry="20" width="420" height="80" fill={action_color} {rest_of_svg} />
    <rect id="RefineTransactionsCode" x="2590" y="1630" rx="20" ry="20" width="240" height="200" fill={code_color} {rest_of_svg} />
    <rect id="RefineCustomers" x="2840" y="2630" rx="20" ry="20" width="420" height="80" fill={action_color} {rest_of_svg} />
    <rect id="RefineCustomersCode" x="2590" y="2570" rx="20" ry="20" width="240" height="200" fill={code_color} {rest_of_svg} />
    <rect id="ConsolidateCode" x="4000" y="2180" rx="20" ry="20" width="240" height="600" fill={code_color} {rest_of_svg} />
    <rect id="Consolidate" x="4250" y="2630" rx="20" ry="20" width="250" height="80" fill={action_color} {rest_of_svg} />
    <rect id="Consolidate" x="4250" y="2435" rx="20" ry="20" width="250" height="80" fill={action_color} {rest_of_svg} transform="rotate(35 4375 2490)" />
    <rect id="CheckFraudCode" x="3970" y="1330" rx="20" ry="20" width="300" height="260" fill={code_color} {rest_of_svg} />
    <rect id="CheckFraud" x="4300" y="1425" rx="20" ry="20" width="400" height="80" fill={action_color} {rest_of_svg} />
    <rect id="ReportView" x="5805" y="2505" rx="20" ry="20" width="390" height="265" fill={info_color} {rest_of_svg} />
    <g>
        <rect id="legend" x="6500" y="3250" rx="20" ry="20" width="400" height="100" fill={action_color} pointer-events="none" cursor="default" {rest_of_svg} />
        <text x="6590" y="3320" font-family="Verdana" font-size="50" fill="blue">Run task</text>
        <rect id="legend" x="6500" y="3425" rx="20" ry="20" width="400" height="100" fill={secondary_action_color} pointer-events="none" cursor="default" {rest_of_svg} />
        <text x="6590" y="3495" font-family="Verdana" font-size="50" fill="blue">Open tool</text>
        <rect id="legend" x="6500" y="3600" rx="20" ry="20" width="400" height="100" fill={code_color} pointer-events="none" cursor="default" {rest_of_svg} />
        <text x="6570" y="3670" font-family="Verdana" font-size="50" fill="blue">Show code</text>
    </g>

#"""


def set_demo_mode(image: ui.interactive_image, switch: bool):
    image.set_content(svg_overlay if switch else "")
    app.storage.general['demo_mode'] = switch


async def mesh_ii():
    """Draw an interactive image that shows the data mesh architecture"""

    with ui.interactive_image(
        DIAGRAM,
        content=f"""
        <rect id="NFS" x="15" y="10" rx="20" ry="20" width="465" height="210" fill={action_color} {rest_of_svg} />
        <rect id="Fraud" x="718" y="130" rx="60" ry="60" width="1345" height="1020" fill={action_color} {rest_of_svg} />
        <rect id="S3" x="7070" y="70" rx="20" ry="20" width="415" height="150" fill={action_color} {rest_of_svg} />
        <rect id="IAM" x="670" y="1380" rx="20" ry="20" width="450" height="250" fill={secondary_action_color} {rest_of_svg} />
        <rect id="Policies" x="5745" y="1365" rx="90" ry="20" width="495" height="280" fill={info_color} {rest_of_svg} />
        <rect id="Catalogue" x="6290" y="1365" rx="90" ry="20" width="590" height="280" fill={info_color} {rest_of_svg} />
        <rect id="Edge" x="250" y="2810" width="860" height="280" fill={info_color} {rest_of_svg} />
        # """,
        # ).on("svg:pointerover", lambda e: open_popup_for(e.args["element_id"])).classes(
    ).on(
        "svg:pointerup",
        lambda e: handle_image_action(e.args),
        # ).on("svg:pointerover", lambda e: handle_image_info(e.args)
    ).classes(
        "relative m-0 p-0"
    ) as mesh_image:

        mesh_image.client.content.classes('p-2') # reduce default gap
        
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


async def domain_ii():

    with ui.interactive_image(
        DATA_DOMAIN["diagram"]
    ).on(
        "svg:pointerup",
        lambda e: handle_image_action(e.args),
        # ).on("svg:pointerover", lambda e: handle_image_info(e.args)
    ).classes(
        "relative"
    ).props(
        "fit=scale-down"
    ) as domain_image:

        domain_image.client.content.classes('p-2') # remove the default gap

        ui.switch("Go Live", on_change=lambda x, i=domain_image: set_demo_mode(i, x.value)).classes("absolute top-0 left-2").bind_value(app.storage.general, 'demo_mode')

        with ui.list().props("bordered dense").classes("w-96 absolute top-10 left-2").bind_visibility_from(app.storage.general, 'demo_mode'):
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
                    ui.button(icon="o_library_add", on_click=lambda: create_transactions(100)).classes("mx-0 px-1").props("flat").tooltip("Generate bulk transactions for NiFi")
                    ui.button(icon="o_integration_instructions", on_click=code_create_transactions).classes("mx-0 px-1").props("flat")
                with ui.item(on_click=sample_transactions).props("dense").classes("mx-0 gx-0"):
                    with ui.item_section():
                        ui.item_label(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.json")

        # Realtime monitoring information
        monitoring_card()
        monitoring_timers = await monitoring_charts()
        # metric_badges_on_ii()
        ui.switch("Monitor", on_change=lambda x, t=monitoring_timers: switch_monitoring(x.value, t)).classes("absolute top-2 right-2").bind_visibility_from(app.storage.general, "demo_mode")
        logging_card()


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


### NOT USED
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
        "in_txn_pushed",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[250px] left-[280px]").tooltip("published transactions")
    ui.badge("0", color="teal").bind_text_from(
        app.storage.general,
        "in_txn_pulled",
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
        "brnz_transactions",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[280px] left-[450px]").tooltip("# of transactions")
    ui.badge("0", color="orange").bind_text_from(
        app.storage.general,
        "brnz_customers",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[450px] left-[450px]").tooltip("# of customers")

    # silver counts
    ui.badge("0", color="darkturquoise").bind_text_from(
        app.storage.general,
        "slvr_profiles",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[140px] left-[680px]").tooltip("# of profiles")
    ui.badge("0", color="teal").bind_text_from(
        app.storage.general,
        "slvr_transactions",
        lambda x: x if x is not None else 0,
    ).classes("absolute top-[280px] left-[680px]").tooltip("# of transactions")
    ui.badge("0", color="orange").bind_text_from(
        app.storage.general,
        "slvr_customers",
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

