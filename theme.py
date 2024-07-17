from contextlib import contextmanager

from nicegui import ui

from common import *
from main import index_page
from page import *


@contextmanager
def frame(navigation_title: str):
    """Custom page frame to share the same styling and behavior across all pages"""
    # ui.colors(
    #     primary=HPE_COLORS["darkgreen"], secondary=HPE_COLORS["darkpurple"], accent=HPE_COLORS["darkblue"], positive=HPE_COLORS["darkteal"], negative=HPE_COLORS["darkred"], warning=HPE_COLORS["darkorange"]
    # )
    with ui.header(elevated=True).classes("items-center justify-between uppercase"):
        ui.button(icon="home", on_click=lambda: ui.navigate.to(index_page)).props("flat color=light")

        ui.label(navigation_title)
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

    with ui.footer():
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

            ui.link(
                "Source",
                target=DATA_DOMAIN.get("link", ""),
                new_tab=True,
            ).bind_visibility_from(
                DATA_DOMAIN, "link", backward=lambda x: x is not None
            ).classes(
                "text-white hover:text-blue-600"
            )
            ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                app.storage.general, "busy", lambda x: not x
            ).tooltip("Ready")

    with ui.column().classes('items-center h-full'):
        yield
