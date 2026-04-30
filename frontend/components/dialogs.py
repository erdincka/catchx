import asyncio
import json
import logging

import httpx
from nicegui import ui, app

from config import (
    BACKEND_URL, HPE_COLORS, cluster_configuration_steps,
    VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD, TABLE_CUSTOMERS, TABLE_TRANSACTIONS,
    TABLE_PROFILES, TABLE_FRAUD, BASEDIR,
)
from client import api_get, api_post, api_delete

logger = logging.getLogger("dialogs")


def cluster_connect_dialog():
    steps = [dict(s) for s in cluster_configuration_steps]

    with ui.dialog().props("persistent") as dlg, ui.card().classes("w-full max-w-2xl p-6"):
        with ui.row().classes("w-full items-center mb-4"):
            ui.label("Connect to HPE Data Fabric").classes("text-lg font-semibold flex-1")
            ui.button(icon="close", on_click=dlg.close).props("flat round dense")

        with ui.row().classes("w-full gap-2"):
            host_input = ui.input("Hostname / IP Address").classes("flex-1").bind_value(
                app.storage.user, "MAPR_HOST"
            )
            user_input = ui.input("Username").classes("flex-1").bind_value(
                app.storage.user, "MAPR_USER"
            )
            pass_input = ui.input("Password", password=True, password_toggle_button=True).classes(
                "flex-1"
            ).bind_value(app.storage.user, "MAPR_PASS")

        ui.separator().classes("my-3")

        status_rows = {}
        with ui.column().classes("w-full gap-2"):
            for step in steps:
                with ui.row().classes("w-full items-center gap-3"):
                    icon = ui.icon("radio_button_unchecked", size="sm", color="grey")
                    status_rows[step["name"]] = icon
                    with ui.column().classes("flex-1"):
                        ui.label(step["info"]).classes("text-sm")

        ui.separator().classes("my-3")

        async def run_setup():
            host = app.storage.user.get("MAPR_HOST", "")
            user = app.storage.user.get("MAPR_USER", "")
            password = app.storage.user.get("MAPR_PASS", "")

            if not host:
                ui.notify("Enter a hostname", type="warning")
                return

            for step in steps:
                status_rows[step["name"]].set_name("radio_button_unchecked")
                status_rows[step["name"]].props("color=grey")

            try:
                async with httpx.AsyncClient() as client:
                    async with client.stream(
                        "POST",
                        f"{BACKEND_URL}/api/cluster/setup",
                        json={"host": host, "user": user, "password": password},
                        timeout=120.0,
                    ) as response:
                        async for line in response.aiter_lines():
                            if not line.startswith("data:"):
                                continue
                            data = json.loads(line[5:].strip())
                            name = data.get("name", "")
                            status = data.get("status", "")
                            msg = data.get("message", "")

                            if name in status_rows:
                                if status == "check":
                                    status_rows[name].set_name("check_circle")
                                    status_rows[name].props("color=positive")
                                elif status == "error":
                                    status_rows[name].set_name("error")
                                    status_rows[name].props("color=negative")
                                else:
                                    status_rows[name].set_name("run_circle")
                                    status_rows[name].props("color=warning")

                            if msg:
                                ui.notify(msg, type="info" if status == "check" else "negative")

                ui.notify("Setup complete!", type="positive")
                app.storage.user["configured"] = True

            except Exception as error:
                logger.warning("Setup stream error: %s", error)
                ui.notify(f"Setup failed: {error}", type="negative")

        with ui.row().classes("w-full justify-end mt-2"):
            ui.button("Connect & Setup", icon="rocket_launch", on_click=run_setup).props(
                "color=primary"
            )

    return dlg


def settings_dialog():
    with ui.dialog().props("position=right full-height persistent") as dlg, ui.card().classes(
        "relative h-full overflow-y-auto p-4 min-w-72"
    ):
        ui.button(icon="close", on_click=dlg.close).props("flat round dense").classes(
            "absolute right-2 top-2"
        )
        ui.label("Settings").classes("text-lg font-semibold mb-4")

        with ui.card_section():
            ui.label("External Data Lakes").classes("text-sm font-semibold uppercase text-gray-500 mb-2")
            with ui.column().classes("gap-2 w-full"):
                ui.input("S3 / Minio Host", placeholder="minio.local").bind_value(
                    app.storage.user, "S3_SERVER"
                ).classes("w-full")
                ui.input("NFS Server Path", placeholder="nfs-server:/export").bind_value(
                    app.storage.user, "NFS_PATH"
                ).classes("w-full")
                ui.button(
                    "Mount NFS",
                    on_click=lambda: command_output_dialog(
                        f"umount -l /mnt; mount -t nfs -o nolock,soft {app.storage.user.get('NFS_PATH', 'localhost')} /mnt; ls -lA /mnt"
                    ),
                ).props("outline color=primary no-caps")

        ui.separator().classes("my-3")

        with ui.card_section():
            ui.label("S3 Credentials").classes("text-sm font-semibold uppercase text-gray-500 mb-2")
            ui.caption("For Iceberg and Spark access")
            with ui.column().classes("gap-2 w-full"):
                ui.input("Access Key").bind_value(app.storage.user, "S3_ACCESS_KEY").classes("w-full")
                ui.input("Secret Key", password=True, password_toggle_button=True).bind_value(
                    app.storage.user, "S3_SECRET_KEY"
                ).classes("w-full")

        ui.separator().classes("my-3")

        with ui.card_section():
            ui.label("External Links").classes("text-sm font-semibold uppercase text-gray-500 mb-2")
            with ui.column().classes("gap-2 w-full"):
                ui.input("Dashboard URL").bind_value(app.storage.user, "DASHBOARD_URL").classes("w-full")
                ui.input("Catalogue URL").bind_value(app.storage.user, "CATALOGUE_URL").classes("w-full")

        ui.separator().classes("my-3")

        with ui.card_section():
            ui.label("Cluster Mount").classes("text-sm font-semibold uppercase text-gray-500 mb-2")
            with ui.row().classes("gap-2 flex-wrap"):
                ui.button("List /mapr", on_click=lambda: command_output_dialog("ls -lA /mapr")).props(
                    "outline no-caps"
                )
                ui.button(
                    "Remount",
                    on_click=lambda: command_output_dialog(
                        f"umount -l /mapr; mount -t nfs -o nolock,soft {app.storage.user.get('MAPR_HOST', '')}:/mapr /mapr"
                    ),
                ).props("outline no-caps")

        ui.separator().classes("my-3")

        with ui.card_section():
            ui.label("Danger Zone").classes("text-sm font-semibold uppercase text-red-500 mb-2")
            ui.caption("Removes ALL volumes and data permanently")

            async def do_cleanup():
                result = await api_delete("/api/cluster/cleanup")
                if result and result.status_code == 200:
                    data = result.json()
                    for msg in data.get("messages", []):
                        ui.notify(msg, type="warning")
                    for metric in app.storage.user.get("_metrics_keys", []):
                        app.storage.user[metric] = 0

            ui.button("DELETE ALL!", icon="warning", on_click=do_cleanup).props(
                "color=negative no-caps"
            ).classes("mt-2")

    return dlg


def command_output_dialog(command: str):
    async def run():
        process = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        stdout, _ = await process.communicate()
        log_element.push(stdout.decode())

    with ui.dialog().props("full-width") as dlg, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dlg.close).props("flat round dense").classes(
            "absolute right-2 top-2"
        )
        ui.label(f"$ {command[:80]}{'...' if len(command) > 80 else ''}").classes(
            "text-xs font-mono text-gray-500 mt-1 mb-2"
        )
        log_element = ui.log().classes("w-full h-64 text-xs font-mono")

    dlg.on("close", lambda d=dlg: d.delete())
    dlg.open()
    asyncio.create_task(run())


async def show_code_dialog(function_name: str, extra_params: dict = None):
    params = extra_params or {}
    cluster = app.storage.user.get("clusterinfo", {})
    if cluster:
        params.setdefault("cluster", cluster.get("name", ""))
    params.setdefault("mapr_user", app.storage.user.get("MAPR_USER", ""))
    params.setdefault("mapr_pass", app.storage.user.get("MAPR_PASS", ""))

    response = await api_get(f"/api/code/{function_name}", params=params)
    if response is None or response.status_code != 200:
        ui.notify(f"Could not load code for '{function_name}'", type="negative")
        return

    data = response.json()

    with ui.dialog().props("full-width") as dlg, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dlg.close).props("flat round dense").classes(
            "absolute right-2 top-2"
        )
        ui.label(f"{function_name}").classes("text-sm font-semibold font-mono text-gray-600 mt-1")
        ui.label(data.get("module", "")).classes("text-xs text-gray-400 mb-3")
        ui.code(data["source"], language="python").classes("w-full")

    dlg.on("close", lambda d=dlg: d.delete())
    dlg.open()


async def show_table_dialog(title: str, records: list):
    if not records:
        ui.notify(f"No records in {title}", type="warning")
        return

    import pandas as pd

    with ui.dialog().props("full-width") as dlg, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dlg.close).props("flat round dense").classes(
            "absolute right-2 top-2"
        )
        ui.label(title).classes("text-sm font-semibold mb-3")
        ui.label(f"{len(records)} records").classes("text-xs text-gray-400 mb-2")
        df = pd.DataFrame(records)
        ui.table.from_pandas(df, row_key="_id" if "_id" in df.columns else None).classes(
            "w-full"
        ).props("dense flat bordered")

    dlg.on("close", lambda d=dlg: d.delete())
    dlg.open()


async def show_history_dialog(tier: str, table: str):
    response = await api_get(f"/api/data/iceberg/{tier}/{table}/history")
    if response is None:
        return

    data = response.json()
    history = data.get("history", [])

    with ui.dialog().props("full-width") as dlg, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dlg.close).props("flat round dense").classes(
            "absolute right-2 top-2"
        )
        ui.label(f"Iceberg history: {tier}.{table}").classes("text-sm font-semibold mb-3")
        log = ui.log().classes("w-full h-48 text-xs font-mono")
        for h in history:
            log.push(h)

    dlg.on("close", lambda d=dlg: d.delete())
    dlg.open()
