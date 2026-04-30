import logging

from nicegui import ui, app

from config import (
    HPE_COLORS, DATA_PRODUCT, DIAGRAM, DATA_DOMAIN,
    VOLUME_BRONZE, TABLE_CUSTOMERS, TABLE_TRANSACTIONS,
    TABLE_PROFILES, VOLUME_SILVER, VOLUME_GOLD,
)
from client import api_post, api_get, handle_response

logger = logging.getLogger("gui")

_action = HPE_COLORS["purple"]
_secondary = HPE_COLORS["orange"]
_code = HPE_COLORS["teal"]
_info = HPE_COLORS["darkblue"]
_opacity = "0.3"
_base = f'fill-opacity={_opacity} stroke="none" stroke-linecap="round" stroke-width:"0" pointer-events="all" cursor="pointer"'

SVG_DOMAIN = f"""
    <rect id="PublishTransactions" x="580" y="1585" rx="40" ry="40" width="330" height="100" fill={_action} {_base} />
    <rect id="PublishTransactionsCode" x="200" y="1460" rx="60" ry="60" width="350" height="350" fill={_code} {_base} />
    <rect id="NifiStreams" x="1430" y="1595" rx="20" ry="20" width="300" height="80" fill={_secondary} {_base} />
    <rect id="NifiStreamsCode" x="983" y="1485" rx="20" ry="20" width="432" height="266" fill={_code} {_base} />
    <rect id="IngestTransactions" x="1340" y="1800" rx="20" ry="20" width="380" height="90" fill={_action} {_base} />
    <rect id="IngestTransactionsCode" x="1070" y="1760" rx="20" ry="20" width="260" height="180" fill={_code} {_base} />
    <rect id="IngestCustomersIceberg" x="1350" y="2810" rx="20" ry="20" width="290" height="90" fill={_action} {_base} />
    <rect id="IngestCustomersIcebergCode" x="1070" y="2750" rx="20" ry="20" width="260" height="180" fill={_code} {_base} />
    <rect id="AirflowBatch" x="1430" y="2545" rx="20" ry="20" width="220" height="80" fill={_secondary} {_base} />
    <rect id="AirflowBatchCode" x="970" y="2453" rx="20" ry="20" width="431" height="260" fill={_code} {_base} />
    <rect id="BronzeTransactions" x="2070" y="1350" rx="20" ry="20" width="350" height="430" fill={_info} {_base} />
    <rect id="BronzeCustomers" x="2060" y="2360" rx="20" ry="20" width="350" height="410" fill={_info} {_base} />
    <rect id="SilverCustomers" x="3330" y="2370" rx="20" ry="20" width="350" height="410" fill={_info} {_base} />
    <rect id="SilverTransactions" x="3340" y="1450" rx="20" ry="20" width="320" height="380" fill={_info} {_base} />
    <rect id="SilverProfiles" x="3360" y="670" rx="20" ry="20" width="300" height="360" fill={_info} {_base} />
    <rect id="GoldCustomers" x="4530" y="2300" rx="20" ry="20" width="350" height="410" fill={_info} {_base} />
    <rect id="ProfileBuilderCode" x="2590" y="670" rx="20" ry="20" width="240" height="200" fill={_code} {_base} />
    <rect id="RefineTransactions" x="2840" y="1590" rx="20" ry="20" width="420" height="80" fill={_action} {_base} />
    <rect id="RefineTransactionsCode" x="2590" y="1530" rx="20" ry="20" width="240" height="200" fill={_code} {_base} />
    <rect id="RefineCustomers" x="2840" y="2530" rx="20" ry="20" width="420" height="80" fill={_action} {_base} />
    <rect id="RefineCustomersCode" x="2590" y="2470" rx="20" ry="20" width="240" height="200" fill={_code} {_base} />
    <rect id="ConsolidateCode" x="4000" y="2080" rx="20" ry="20" width="240" height="600" fill={_code} {_base} />
    <rect id="Consolidate" x="4250" y="2530" rx="20" ry="20" width="250" height="80" fill={_action} {_base} />
    <rect id="Consolidate" x="4250" y="2335" rx="20" ry="20" width="250" height="80" fill={_action} {_base} transform="rotate(35 4375 2390)" />
    <rect id="CheckFraudCode" x="3970" y="1230" rx="20" ry="20" width="300" height="260" fill={_code} {_base} />
    <rect id="CheckFraud" x="4300" y="1325" rx="20" ry="20" width="400" height="80" fill={_action} {_base} />
    <rect id="ReportView" x="5805" y="2405" rx="20" ry="20" width="390" height="265" fill={_info} {_base} />
    <g>
        <rect id="legend" x="6500" y="3250" rx="20" ry="20" width="400" height="100" fill={_action} pointer-events="none" cursor="default" {_base} />
        <text x="6590" y="3320" font-family="Verdana" font-size="50" fill="blue">Run task</text>
        <rect id="legend" x="6500" y="3425" rx="20" ry="20" width="400" height="100" fill={_secondary} pointer-events="none" cursor="default" {_base} />
        <text x="6590" y="3495" font-family="Verdana" font-size="50" fill="blue">Open tool</text>
        <rect id="legend" x="6500" y="3600" rx="20" ry="20" width="400" height="100" fill={_code} pointer-events="none" cursor="default" {_base} />
        <text x="6570" y="3670" font-family="Verdana" font-size="50" fill="blue">Show code</text>
    </g>
    <rect id="Catalogue" x="5820" y="1250" rx="30" ry="30" width="550" height="220" fill={_info} {_base} />
"""


def mesh_ii():
    with ui.interactive_image(
        DIAGRAM,
        content=f"""
        <rect id="NFS" x="15" y="10" rx="20" ry="20" width="465" height="210" fill={_action} {_base} />
        <rect id="Fraud" x="718" y="130" rx="60" ry="60" width="1345" height="1020" fill={_action} {_base} />
        <rect id="S3" x="7070" y="70" rx="20" ry="20" width="415" height="150" fill={_action} {_base} />
        <rect id="IAM" x="670" y="1380" rx="20" ry="20" width="450" height="250" fill={_secondary} {_base} />
        <rect id="Policies" x="5745" y="1365" rx="90" ry="20" width="495" height="280" fill={_info} {_base} />
        <rect id="Catalogue" x="6290" y="1365" rx="90" ry="20" width="590" height="280" fill={_info} {_base} />
        <rect id="Edge" x="250" y="2810" width="860" height="280" fill={_info} {_base} />
        """,
    ).on(
        "svg:pointerup",
        lambda e: _handle_mesh_action(e.args),
    ).classes("relative m-0 p-0").props("fit=scale-down ratio=1") as img:
        img.client.content.classes("p-2")

        ui.button(icon="upload", on_click=_nfs_upload).classes(
            "absolute top-10 left-5"
        ).props("flat color=white").bind_visibility_from(
            app.storage.user, "NFS_PATH", backward=lambda x: bool(x)
        ).tooltip("Upload Customers via NFS")

        ui.button(icon="upload", on_click=_s3_upload).classes(
            "absolute top-10 right-3"
        ).props("flat color=white").bind_visibility_from(
            app.storage.user, "S3_SERVER", backward=lambda x: bool(x)
        ).tooltip("Upload Transactions to S3")

    return img


def domain_ii():
    img = ui.interactive_image(DATA_DOMAIN["diagram"]).on(
        "svg:pointerup",
        lambda e: _handle_domain_action(e.args),
    ).props("fit=scale-down")

    img.bind_content_from(
        app.storage.user, "demo_mode", backward=lambda x: SVG_DOMAIN if x else ""
    )
    img.client.content.classes("p-2")

    with img:
        with ui.list().props("bordered dense").classes(
            "w-72 absolute top-10 left-2"
        ).bind_visibility_from(app.storage.user, "demo_mode"):
            ui.item_label("Source data").props("header").classes("text-bold text-primary")
            ui.separator()
            with ui.row().classes("w-full no-wrap p-0"):
                with ui.button_group().props("flat"):
                    ui.button(icon="o_preview", on_click=_peek_customers).props("flat").tooltip(
                        "Sample customers"
                    )
                    ui.button(
                        icon="o_integration_instructions", on_click=lambda: _show_code("create_customers")
                    ).props("flat").tooltip("Code for create_customers")
                    ui.button(icon="o_library_add", on_click=_create_customers).props("flat").tooltip(
                        "Create new customers"
                    )
            with ui.row().classes("w-full no-wrap p-0"):
                with ui.button_group().props("flat"):
                    ui.button(icon="o_preview", on_click=_peek_transactions).props("flat").tooltip(
                        "Sample transactions"
                    )
                    ui.button(
                        icon="o_integration_instructions", on_click=lambda: _show_code("create_transactions")
                    ).props("flat").tooltip("Code for create_transactions")
                    ui.button(
                        icon="o_library_add", on_click=lambda: _create_transactions(100)
                    ).props("flat").tooltip("Generate bulk transactions")

    return img


# ── Mesh action handler ──────────────────────────────────────────────────────

async def _handle_mesh_action(e: dict):
    element = e.get("element_id", "")
    app.storage.user["busy"] = True

    if element == "Fraud":
        ui.navigate.to(f"/{DATA_PRODUCT}")

    elif element == "NFS":
        from components.dialogs import command_output_dialog
        command_output_dialog("df -h /mnt; ls -lA /mnt")

    elif element == "S3":
        s3 = app.storage.user.get("S3_SERVER", "localhost:9000")
        ui.navigate.to(f"http://{s3}", new_tab=True)

    elif element == "IAM":
        host = app.storage.user.get("MAPR_HOST", "localhost")
        ui.navigate.to(f"https://{host}:8443/app/dfui/#/login", new_tab=True)

    elif element in ("Policies", "Edge"):
        ui.notify(f"{element}: informational — no action configured", type="info")

    elif element == "Catalogue":
        url = app.storage.user.get("CATALOGUE_URL", "")
        if not url:
            _prompt_url("CATALOGUE_URL", "Catalogue URL")
        else:
            ui.navigate.to(url, new_tab=True)

    else:
        if element:
            ui.notify(f"{element}: not configured yet", type="info")

    app.storage.user["busy"] = False


# ── Domain action handler ────────────────────────────────────────────────────

async def _handle_domain_action(e: dict):
    element = e.get("element_id", "")
    app.storage.user["busy"] = True

    actions = {
        "PublishTransactions": _publish_transactions,
        "PublishTransactionsCode": lambda: _show_code("publish_transactions"),
        "NifiStreams": _open_nifi,
        "NifiStreamsCode": lambda: _show_code("nifi_template"),
        "IngestTransactions": _ingest_transactions,
        "IngestTransactionsCode": lambda: _show_code("ingest_transactions"),
        "IngestCustomersIceberg": _ingest_customers,
        "IngestCustomersIcebergCode": lambda: _show_code("ingest_customers_iceberg"),
        "AirflowBatch": _open_airflow,
        "AirflowBatchCode": lambda: _show_code("airflow_dag"),
        "BronzeTransactions": lambda: _peek_tier(VOLUME_BRONZE, TABLE_TRANSACTIONS),
        "BronzeCustomers": _peek_bronze_customers_iceberg,
        "SilverCustomers": lambda: _peek_tier(VOLUME_SILVER, TABLE_CUSTOMERS),
        "SilverTransactions": lambda: _peek_tier(VOLUME_SILVER, TABLE_TRANSACTIONS),
        "SilverProfiles": lambda: _peek_tier(VOLUME_SILVER, TABLE_PROFILES),
        "GoldCustomers": lambda: _peek_tier(VOLUME_GOLD, TABLE_CUSTOMERS),
        "ProfileBuilderCode": lambda: _show_code("upsert_profile"),
        "RefineTransactions": _refine_transactions,
        "RefineTransactionsCode": lambda: _show_code("refine_transactions"),
        "RefineCustomers": _refine_customers,
        "RefineCustomersCode": lambda: _show_code("refine_customers"),
        "Consolidate": _consolidate,
        "ConsolidateCode": lambda: _show_code("create_golden"),
        "CheckFraud": _fraud_detection,
        "CheckFraudCode": lambda: _show_code("fraud_detection"),
        "ReportView": _open_dashboard,
        "Catalogue": _open_catalogue,
        "legend": lambda: None,
    }

    handler = actions.get(element)
    if handler:
        import asyncio
        if asyncio.iscoroutinefunction(handler):
            await handler()
        else:
            handler()
    elif element:
        logger.warning("No handler for element: %s", element)
        ui.notify(f"{element}: not configured yet", type="info")

    app.storage.user["busy"] = False


# ── Action implementations ───────────────────────────────────────────────────

def _show_code(name: str):
    import asyncio
    from components.dialogs import show_code_dialog
    asyncio.create_task(show_code_dialog(name))


async def _peek_tier(tier: str, table: str):
    from components.dialogs import show_table_dialog
    r = await api_get(f"/api/data/peek/{tier}/{table}")
    if r:
        data = r.json()
        await show_table_dialog(f"{tier} › {table}", data.get("records", []))


async def _peek_bronze_customers_iceberg():
    from components.dialogs import show_table_dialog
    r = await api_get(f"/api/data/iceberg/{VOLUME_BRONZE}/{TABLE_CUSTOMERS}/tail")
    if r:
        data = r.json()
        await show_table_dialog(f"bronze › customers (Iceberg)", data.get("records", []))


async def _peek_customers():
    from components.dialogs import show_table_dialog
    r = await api_get("/api/data/customers/preview")
    if r:
        await show_table_dialog("Customers preview", r.json().get("records", []))


async def _peek_transactions():
    from components.dialogs import show_table_dialog
    r = await api_get("/api/data/transactions/preview")
    if r:
        await show_table_dialog("Transactions preview", r.json().get("records", []))


async def _create_customers():
    r = await api_post("/api/data/customers/create")
    handle_response(r)
    if r and r.json().get("status") == "ok":
        ui.notify(f"Created {r.json()['count']} customers", type="positive")


async def _create_transactions(count: int = 100):
    r = await api_post("/api/data/transactions/create", json=None)
    handle_response(r)


async def _publish_transactions():
    r = await api_post("/api/data/transactions/publish")
    if r:
        data = r.json()
        if data.get("status") == "ok":
            ui.notify(f"Published {data['count']} transactions", type="positive")
        else:
            ui.notify(data.get("message", "Failed"), type="negative")


async def _ingest_transactions():
    r = await api_post("/api/data/ingest/transactions")
    if r:
        data = r.json()
        if data.get("status") == "ok":
            ui.notify(f"Ingested {data.get('count', 0)} transactions", type="positive")
        else:
            ui.notify(data.get("message", "Failed"), type="negative")


async def _ingest_customers():
    r = await api_post("/api/data/ingest/customers")
    if r:
        data = r.json()
        if data.get("status") == "ok":
            ui.notify(f"Ingested {data.get('count', 0)} customers", type="positive")
        else:
            ui.notify(data.get("message", "Failed"), type="negative")


async def _refine_customers():
    r = await api_post("/api/data/refine/customers")
    handle_response(r)


async def _refine_transactions():
    r = await api_post("/api/data/refine/transactions")
    handle_response(r)


async def _consolidate():
    r = await api_post("/api/data/consolidate")
    handle_response(r)


async def _fraud_detection():
    r = await api_post("/api/data/fraud")
    if r:
        data = r.json()
        if data.get("status") == "ok":
            ui.notify(
                f"Fraud: {data['fraud_count']} flagged, {data['non_fraud_count']} clean",
                type="warning",
            )
        else:
            ui.notify(data.get("message", "Failed"), type="negative")


def _open_nifi():
    host = app.storage.user.get("MAPR_HOST", "localhost")
    ui.navigate.to(f"https://{host}:12443/nifi/", new_tab=True)


def _open_airflow():
    host = app.storage.user.get("MAPR_HOST", "localhost")
    ui.navigate.to(f"https://{host}:8780/home", new_tab=True)


def _open_dashboard():
    url = app.storage.user.get("DASHBOARD_URL", "")
    if not url:
        _prompt_url("DASHBOARD_URL", "Dashboard URL")
    else:
        ui.navigate.to(url, new_tab=True)


def _open_catalogue():
    url = app.storage.user.get("CATALOGUE_URL", "")
    if not url:
        _prompt_url("CATALOGUE_URL", "Catalogue URL")
    else:
        ui.navigate.to(url, new_tab=True)


def _nfs_upload():
    import shutil
    from config import MOUNT_PATH, TABLE_CUSTOMERS, BASEDIR
    cluster = app.storage.user.get("clusterinfo", {})
    name = cluster.get("name", "") if cluster else ""
    src = f"{MOUNT_PATH}/{name}{BASEDIR}/{TABLE_CUSTOMERS}.csv"
    try:
        shutil.copyfile(src, f"/mnt/{TABLE_CUSTOMERS}.csv")
        ui.notify(f"Copied {TABLE_CUSTOMERS}.csv to /mnt/", type="positive")
    except Exception as e:
        ui.notify(str(e), type="negative")


async def _s3_upload():
    s3 = app.storage.user.get("S3_SERVER", "")
    ak = app.storage.user.get("S3_ACCESS_KEY", "")
    sk = app.storage.user.get("S3_SECRET_KEY", "")
    if not s3 or not sk:
        ui.notify("Configure S3 settings first", type="warning")
        return
    r = await api_post(f"/api/data/s3/upload?s3_server={s3}&access_key={ak}&secret_key={sk}")
    handle_response(r)


def _prompt_url(key: str, label: str):
    with ui.dialog() as dlg, ui.card().classes("p-4"):
        ui.label(label).classes("text-sm font-semibold mb-2")
        ui.input(label).bind_value(app.storage.user, key).classes("w-80")
        ui.button("Go", on_click=dlg.close).props("color=primary")
    dlg.open()
