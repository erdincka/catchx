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

            ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                app.storage.general, "busy", lambda x: not x
            ).tooltip("Ready")

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

    with ui.column().classes('items-center h-full'):
        yield
