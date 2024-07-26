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
            ):
                ui.icon("open_in_new")

            ui.button(icon="settings", on_click=cluster_configuration_dialog).props(
                "flat color=light"
            )

            ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                app.storage.general, "busy", lambda x: not x
            ).tooltip("Ready")
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

            ui.switch(on_change=toggle_debug).tooltip("Debug").props(
                "color=dark keep-color"
            )

            ui.spinner("ios", size="2em", color="red").bind_visibility_from(
                app.storage.general, "busy"
            ).tooltip("Busy")

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


@ui.page("/")
async def index_page():    
    # Main
    header("Mesh")
    await gui.mesh_ii()
    footer()
    # with ui.row().classes("w-full flex flex-nowrap relative"):
    #     demo_steps()


@ui.page(f"/{DATA_PRODUCT}", dark=None, title="Fraud Data Domain")
async def domain_ii():
    """Draw an interactive image that shows demo pipeline for the data domain"""

    action_color = HPE_COLORS["purple"]
    secondary_action_color = HPE_COLORS["orange"]
    code_color = HPE_COLORS["teal"]
    info_color = HPE_COLORS["darkblue"]
    opacity = "0.3"
    rest_of_svg = f'fill-opacity={opacity} stroke="none" stroke-linecap="round" stroke-width:"0" pointer-events="all" cursor="pointer"'

    header(DATA_PRODUCT)

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
    
        def update_overlay(switch):
            domain_image.set_content(svg_overlay if switch else "")
            app.storage.general['demo_mode'] = switch

        ui.switch("Demo", on_change=lambda x: update_overlay(x.value)).classes("absolute top-0 left-2").bind_value(app.storage.general, 'demo_mode')

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
                    ui.button(icon="o_library_add", on_click=lambda: create_transactions(1000)).classes("mx-0 px-1").props("flat").tooltip("Generate bulk transactions for NiFi")
                    ui.button(icon="o_integration_instructions", on_click=code_create_transactions).classes("mx-0 px-1").props("flat")
                with ui.item(on_click=sample_transactions).props("dense").classes("mx-0 gx-0"):
                    with ui.item_section():
                        ui.item_label(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.json")

        # Realtime monitoring information
        monitoring_card()
        await monitoring_charts()
        # metric_badges_on_ii()

    footer()


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
