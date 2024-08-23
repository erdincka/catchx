import inspect
from nicegui import ui

from functions import *
from ingestion import *

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
        with open("DAGs/csv_to_iceberg_DAG.py", 'r') as f:
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
        from jinja2 import Environment, FileSystemLoader
        environment = Environment(loader=FileSystemLoader("templates/"))
        template = environment.get_template("TransactionFlow.xml.j2")
        content = template.render(
            hive_db_connect_url = "jdbc:hive2://localhost:10000/default;auth=maprsasl;ssl=true",
            database_connection_url = f"jdbc:mariadb://{app.storage.user.get('cluster', 'localhost')}:3306/{DATA_PRODUCT}",
            database_driver_location = f"{MOUNT_PATH}/{DATA_PRODUCT}/user/root/mariadb-java-client-3.4.1.jar",
            database_user = app.storage.user.get("MYSQL_USER", "mysql"),
            database_password = app.storage.user.get("MYSQL_PASS", "mysql"),
            hive3_table_name = TABLE_TRANSACTIONS,
            hive3_external_table_location = f"{BASEDIR}/{VOLUME_SILVER}/hive{TABLE_TRANSACTIONS}",
            app_dir = BASEDIR,
            incoming_bulk_file = f"{TABLE_TRANSACTIONS}-bulk.csv",
            app_logs_failed = f"{BASEDIR}/logs/failed",
            app_logs = f"{BASEDIR}/logs",
            dir_app_logs_failed = f"{MOUNT_PATH}/{DATA_PRODUCT}{BASEDIR}/logs/failed",
            hive3_external_table_location_gold = f"{BASEDIR}/{VOLUME_GOLD}",
            put_db_record_table_name = TABLE_TRANSACTIONS,
            hbase_table_name_silver = f"{BASEDIR}/{VOLUME_SILVER}/b{TABLE_TRANSACTIONS}",
            hbase_table_name_bronze = f"{BASEDIR}/{VOLUME_BRONZE}/b{TABLE_TRANSACTIONS}",
            incoming_topic = f"{BASEDIR}/{STREAM_INCOMING}:{TOPIC_TRANSACTIONS}",
            sasl_username = app.storage.user.get("MAPR_USER", ""),
            sasl_password = app.storage.user.get("MAPR_PASS", ""),
            bronze_transactions_dir = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}",
        )
        ui.code(content).classes("w-full mt-6")
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
        ui.code(inspect.getsource(dummy_fraud_score)).classes("w-full mt-6")
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

    app.storage.user["busy"] = True

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
        ui.navigate.to(f"http://{app.storage.user.get('S3_SERVER', 'http://localhost:9000')}", new_tab=True)
        ui.notify("Bring existing object stores into the Global Namespace.", type="info")

    elif element == "IAM":
        ui.navigate.to(f"https://{app.storage.user.get('cluster', 'localhost')}:8443/app/dfui/#/login", new_tab=True)
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
        ui.navigate.to(app.storage.user.get('CATALOGUE_URL', ''), new_tab=True)
    elif element == "Edge":
        ui.notify(
            "Enable non-data products to become a part of the global namespace, enabling them to access data across the enterprise.",
            type="info",
        )

    # Data Domain functions
    elif element == "CreateTransactions":
        # await get_new_transactions()
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
        ui.navigate.to(f"https://{app.storage.user.get('cluster', 'localhost')}:8780/home", new_tab=True)

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
        ui.navigate.to(f"https://{app.storage.user.get('cluster', 'localhost')}:12443/nifi/", new_tab=True)

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

    elif element == "CheckFraudCode":
        await code_getscore().open()

    elif element == "GetScoreCode":
        code_getscore().open()

    elif element == "GoldCustomers":
        await peek_sqlrecords([TABLE_FRAUD, TABLE_TRANSACTIONS, TABLE_CUSTOMERS])

    elif element == "ReportView":
        ui.navigate.to(
            app.storage.user.get("DASHBOARD_URL", "about:blank"),
            new_tab=True,
        )

    elif element == "legend":
        pass

    else:
        logger.warning(element)
        ui.notify(f"{element} not configured yet")

    app.storage.user["busy"] = False
