from nicegui import ui, app, background_tasks

from config import (
    DOCUMENTATION, VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD,
    TABLE_CUSTOMERS, TABLE_TRANSACTIONS, TABLE_PROFILES, TABLE_FRAUD,
    DATA_DOMAIN, BASEDIR, MOUNT_PATH,
)
from client import api_post, api_get
from components.dialogs import show_code_dialog, show_table_dialog, show_history_dialog


def demo_steps():
    with ui.list().classes("w-full") as demo_list:

        # ── Overview ──────────────────────────────────────────────────────────
        with ui.expansion(
            "Demo Overview",
            caption="End-to-end data pipeline with medallion architecture",
            group="flow",
            value=False,
        ).props("header-class='text-primary font-semibold'"):
            ui.markdown(DOCUMENTATION["Overview"]).classes("text-sm text-gray-600 mb-3")
            ui.image(DATA_DOMAIN["diagram"]).props("fit=scale-down").classes("max-h-48")

        # ── Data Sources ──────────────────────────────────────────────────────
        with ui.expansion(
            "Source Data",
            caption="Generate and preview source data",
            group="flow",
            value=True,
        ).props("header-class='text-primary font-semibold'"):
            ui.markdown(DOCUMENTATION["Source Data Generation"]).classes("text-sm text-gray-600 mb-3")

            with ui.list().props("bordered separator").classes("w-full"):
                ui.item_label("Data Sources").props("header").classes("text-bold")
                ui.separator()

                _source_item(
                    icon="people",
                    label="Customers",
                    caption="Batch data — CSV file",
                    preview_fn=_preview_customers,
                    code_fn=lambda: background_tasks.create(show_code_dialog("create_customers")),
                    action_fn=_create_customers,
                    action_tooltip="Generate new customers",
                )

                _source_item(
                    icon="payments",
                    label="Transactions",
                    caption="Streaming data — Kafka topic",
                    preview_fn=_preview_transactions,
                    code_fn=lambda: background_tasks.create(show_code_dialog("publish_transactions")),
                    action_fn=_publish_transactions,
                    action_tooltip="Publish transactions to stream",
                    extra_buttons=[
                        ("code", lambda: background_tasks.create(show_code_dialog("create_transactions")), "Code for create_transactions"),
                        ("add_card", _create_transactions, "Generate new transaction file"),
                    ],
                )

        # ── Bronze (Ingest) ───────────────────────────────────────────────────
        with ui.expansion(
            "Ingest & ETL (Bronze)",
            caption="Raw data ingestion into bronze tier",
            group="flow",
        ).props("header-class='text-primary font-semibold'"):
            ui.markdown(DOCUMENTATION["Data Ingestion and ETL"]).classes("text-sm text-gray-600 mb-3")

            with ui.list().props("bordered separator").classes("w-full"):
                ui.item_label("ETL Processing").props("header").classes("text-bold")
                ui.separator()

                _pipeline_item(
                    icon="content_copy",
                    label="Batch ingest — Customers",
                    caption=f"{VOLUME_BRONZE}/{TABLE_CUSTOMERS} (Iceberg)",
                    preview_fn=lambda: background_tasks.create(_peek_iceberg_tail(VOLUME_BRONZE, TABLE_CUSTOMERS)),
                    history_fn=lambda: background_tasks.create(show_history_dialog(VOLUME_BRONZE, TABLE_CUSTOMERS)),
                    code_fn=lambda: background_tasks.create(show_code_dialog("ingest_customers_iceberg")),
                    action_fn=_ingest_customers,
                )

                _pipeline_item(
                    icon="sms",
                    label="Stream ingest — Transactions",
                    caption=f"{VOLUME_BRONZE}/{TABLE_TRANSACTIONS} (DocumentDB)",
                    preview_fn=lambda: background_tasks.create(_peek_table(VOLUME_BRONZE, TABLE_TRANSACTIONS)),
                    code_fn=lambda: background_tasks.create(show_code_dialog("ingest_transactions")),
                    action_fn=_ingest_transactions,
                )

        # ── Silver (Enrich) ───────────────────────────────────────────────────
        with ui.expansion(
            "Enrich & Clean (Silver)",
            caption="Enrich and mask data into silver tier",
            group="flow",
        ).props("header-class='text-primary font-semibold'"):
            ui.markdown(DOCUMENTATION["Data Enrichment"]).classes("text-sm text-gray-600 mb-3")

            with ui.list().props("bordered separator").classes("w-full"):
                ui.item_label("Data Cleansing").props("header").classes("text-bold")
                ui.separator()

                _pipeline_item(
                    icon="auto_awesome",
                    label="Refine customers",
                    caption="Add country name, iso3166_2; mask birthdate and location",
                    preview_fn=lambda: background_tasks.create(_peek_table(VOLUME_SILVER, TABLE_CUSTOMERS)),
                    code_fn=lambda: background_tasks.create(show_code_dialog("refine_customers")),
                    action_fn=_refine_customers,
                    extra_preview=lambda: background_tasks.create(_peek_table(VOLUME_SILVER, TABLE_PROFILES)),
                )

                _pipeline_item(
                    icon="auto_awesome",
                    label="Refine transactions",
                    caption="Add category to transactions",
                    preview_fn=lambda: background_tasks.create(_peek_table(VOLUME_SILVER, TABLE_TRANSACTIONS)),
                    code_fn=lambda: background_tasks.create(show_code_dialog("refine_transactions")),
                    action_fn=_refine_transactions,
                )

        # ── Gold (Consolidate) ────────────────────────────────────────────────
        with ui.expansion(
            "Consolidate (Gold)",
            caption="Create analytics-ready gold tier data lake",
            group="flow",
        ).props("header-class='text-primary font-semibold'"):
            ui.markdown(DOCUMENTATION["Data Consolidation"]).classes("text-sm text-gray-600 mb-3")

            with ui.list().props("bordered separator").classes("w-full"):
                ui.item_label("Consolidation").props("header").classes("text-bold")
                ui.separator()

                _pipeline_item(
                    icon="compare_arrows",
                    label="Consolidate to gold",
                    caption="Merge customers + transactions, remove PII",
                    preview_fn=lambda: background_tasks.create(_peek_gold_all()),
                    code_fn=lambda: background_tasks.create(show_code_dialog("create_golden")),
                    action_fn=_create_golden,
                )

                _pipeline_item(
                    icon="policy",
                    label="Fraud detection",
                    caption="Simulate AI inference on transactions",
                    preview_fn=lambda: background_tasks.create(_peek_table(VOLUME_GOLD, TABLE_FRAUD, query="fraud == True")),
                    code_fn=lambda: background_tasks.create(show_code_dialog("fraud_detection")),
                    action_fn=_fraud_detection,
                )

    return demo_list


# ── Item builders ─────────────────────────────────────────────────────────────

def _source_item(icon, label, caption, preview_fn, code_fn, action_fn, action_tooltip, extra_buttons=None):
    with ui.item():
        with ui.item_section().props("avatar"):
            ui.icon(icon, color="primary")
        with ui.item_section().classes("flex-grow"):
            ui.item_label(label)
            ui.item_label(caption).props("caption")
        with ui.item_section().props("side"):
            with ui.row().classes("gap-0"):
                ui.button(icon="visibility", on_click=preview_fn).props(
                    "flat dense round color=grey"
                ).tooltip("Preview data")
                ui.button(icon="code", on_click=code_fn).props(
                    "flat dense round color=info"
                ).tooltip("View code")
                if extra_buttons:
                    for btn_icon, btn_fn, btn_tip in extra_buttons:
                        ui.button(icon=btn_icon, on_click=btn_fn).props(
                            "flat dense round color=info"
                        ).tooltip(btn_tip)
                ui.button(icon="rocket_launch", on_click=action_fn).props(
                    "flat dense round color=positive"
                ).tooltip(action_tooltip).bind_visibility_from(app.storage.user, "demo_mode")


def _pipeline_item(icon, label, caption, preview_fn, code_fn, action_fn, history_fn=None, extra_preview=None):
    with ui.item():
        with ui.item_section().props("avatar"):
            ui.icon(icon, color="secondary")
        with ui.item_section().classes("flex-grow"):
            ui.item_label(label)
            ui.item_label(caption).props("caption")
        with ui.item_section().props("side"):
            with ui.row().classes("gap-0"):
                ui.button(icon="visibility", on_click=preview_fn).props(
                    "flat dense round color=grey"
                ).tooltip("Preview data")
                if extra_preview:
                    ui.button(icon="manage_search", on_click=extra_preview).props(
                        "flat dense round color=grey"
                    ).tooltip("Preview profiles")
                if history_fn:
                    ui.button(icon="history", on_click=history_fn).props(
                        "flat dense round color=grey"
                    ).tooltip("Table history")
                ui.button(icon="code", on_click=code_fn).props(
                    "flat dense round color=info"
                ).tooltip("View code")
                ui.button(icon="rocket_launch", on_click=action_fn).props(
                    "flat dense round color=positive"
                ).tooltip("Execute").bind_visibility_from(app.storage.user, "demo_mode")


# ── API call helpers ──────────────────────────────────────────────────────────

async def _create_customers():
    from nicegui import ui
    r = await api_post("/api/data/customers/create")
    if r:
        data = r.json()
        ui.notify(f"Created {data.get('count', 0)} customers", type="positive" if data.get("status") == "ok" else "negative")


async def _create_transactions():
    from nicegui import ui
    r = await api_post("/api/data/transactions/create")
    if r:
        data = r.json()
        ui.notify(f"Created {data.get('count', 0)} transactions", type="positive" if data.get("status") == "ok" else "negative")


async def _publish_transactions():
    from nicegui import ui
    r = await api_post("/api/data/transactions/publish")
    if r:
        data = r.json()
        ui.notify(f"Published {data.get('count', 0)} transactions", type="positive" if data.get("status") == "ok" else "negative")


async def _ingest_customers():
    from nicegui import ui
    r = await api_post("/api/data/ingest/customers")
    if r:
        data = r.json()
        ui.notify(f"Ingested {data.get('count', 0)} customers", type="positive" if data.get("status") == "ok" else "negative")


async def _ingest_transactions():
    from nicegui import ui
    r = await api_post("/api/data/ingest/transactions")
    if r:
        data = r.json()
        ui.notify(f"Ingested {data.get('count', 0)} transactions", type="positive" if data.get("status") == "ok" else "negative")


async def _refine_customers():
    from nicegui import ui
    r = await api_post("/api/data/refine/customers")
    if r:
        data = r.json()
        ui.notify(data.get("message", "Done"), type="positive" if data.get("status") == "ok" else "negative")


async def _refine_transactions():
    from nicegui import ui
    r = await api_post("/api/data/refine/transactions")
    if r:
        data = r.json()
        ui.notify(data.get("message", "Done"), type="positive" if data.get("status") == "ok" else "negative")


async def _create_golden():
    from nicegui import ui
    r = await api_post("/api/data/consolidate")
    if r:
        data = r.json()
        ui.notify(data.get("message", "Done"), type="positive" if data.get("status") == "ok" else "negative")


async def _fraud_detection():
    from nicegui import ui
    r = await api_post("/api/data/fraud")
    if r:
        data = r.json()
        if data.get("status") == "ok":
            ui.notify(f"Fraud: {data['fraud_count']} flagged, {data['non_fraud_count']} clean", type="warning")
        else:
            ui.notify(data.get("message", "Failed"), type="negative")


async def _peek_table(tier: str, table: str, query: str = None):
    params = {}
    if query:
        params["query"] = query
    r = await api_get(f"/api/data/peek/{tier}/{table}", params=params)
    if r:
        await show_table_dialog(f"{tier} › {table}", r.json().get("records", []))


async def _peek_iceberg_tail(tier: str, table: str):
    r = await api_get(f"/api/data/iceberg/{tier}/{table}/tail")
    if r:
        await show_table_dialog(f"{tier} › {table} (Iceberg)", r.json().get("records", []))


async def _preview_customers():
    r = await api_get("/api/data/customers/preview")
    if r:
        await show_table_dialog("Customers (source file)", r.json().get("records", []))


async def _preview_transactions():
    r = await api_get("/api/data/transactions/preview")
    if r:
        await show_table_dialog("Transactions (source file)", r.json().get("records", []))


async def _peek_gold_all():
    for table in [TABLE_CUSTOMERS, TABLE_TRANSACTIONS]:
        r = await api_get(f"/api/data/peek/{VOLUME_GOLD}/{table}")
        if r:
            await show_table_dialog(f"gold › {table}", r.json().get("records", []))
