from nicegui import ui, app
from config import HPE_COLORS, DATA_PRODUCT
from theme import dark_header_style


def header(title: str):
    with ui.header(elevated=True).style(dark_header_style()).classes(
        "items-center justify-between py-2 px-4"
    ) as h:
        # Left: home + title
        with ui.row().classes("items-center gap-2"):
            ui.button(icon="home", on_click=lambda: ui.navigate.to("/")).props(
                "flat round"
            ).style("color: white")
            ui.label(title).classes("text-white font-semibold text-lg uppercase tracking-wider")

        ui.space()

        # Center: live/monitor toggles
        with ui.row().classes("items-center gap-3"):
            ui.switch("Go Live").props("color=accent dark").classes(
                "text-white text-sm"
            ).bind_value(app.storage.user, "demo_mode").bind_visibility_from(
                app.storage.user, "clusterinfo", backward=lambda x: bool(x)
            )

            ui.switch("Monitor", on_change=lambda x: _toggle_monitoring(x.value)).props(
                "color=accent dark"
            ).classes("text-white text-sm").bind_visibility_from(
                app.storage.user, "demo_mode"
            )

        ui.space()

        # Right: cluster name + buttons
        with ui.row().classes("items-center gap-1"):
            cluster_link = ui.link("", new_tab=True).classes(
                "text-white text-sm hover:text-teal-300 no-underline"
            ).bind_text_from(
                app.storage.user, "clusterinfo", backward=lambda x: x["name"] if x else ""
            ).bind_visibility_from(
                app.storage.user, "clusterinfo", backward=lambda x: bool(x)
            )
            cluster_link.bind_href_from(
                app.storage.user, "MAPR_HOST",
                backward=lambda h: f"https://{app.storage.user.get('MAPR_USER','')}:{app.storage.user.get('MAPR_PASS','')}@{h}:8443/app/mcs/"
            )

            ui.icon("error", size="sm").style("color: #ff6b6b").bind_visibility_from(
                app.storage.user, "clusterinfo", backward=lambda x: not x
            ).tooltip("Cluster not connected — click link icon to configure")

            ui.button(icon="link", on_click=_open_cluster_connect).props("flat round").style(
                "color: white"
            ).tooltip("Connect to cluster")

            ui.button(icon="settings", on_click=_open_settings).props("flat round").style(
                "color: white"
            ).tooltip("Settings")

    return h


def _toggle_monitoring(value: bool):
    from components.monitoring import set_monitoring_active
    set_monitoring_active(value)


def _open_cluster_connect():
    from components.dialogs import cluster_connect_dialog
    cluster_connect_dialog().open()


def _open_settings():
    from components.dialogs import settings_dialog
    settings_dialog().open()
