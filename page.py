import logging
from nicegui import ui, app

from functions import *
from helpers import *


logger = logging.getLogger()


def header():
    with ui.header(elevated=True).classes('items-center justify-between uppercase'):
        ui.label(f"{APP_NAME}: {TITLE}")
        ui.space()

        with ui.row().classes("place-items-center"):
            ui.button("Cluster", on_click=configure_cluster).props("outline color=white")
            ui.link(target=f"https://{app.storage.general.get('cluster', 'localhost')}:8443/", new_tab=True).bind_text_from(app.storage.general, "cluster", backward=lambda x: app.storage.general["clusters"][x] if x else "None").classes("text-white hover:text-blue-600")

        ui.space()
        ui.switch(on_change=toggle_debug).tooltip("Debug").props("color=dark keep-color")


def footer():
    with ui.footer():
        with ui.row().classes("w-full items-center"):
            ui.button(icon="menu", on_click=toggle_log).props("flat text-color=white")
            ui.label("Log")

            ui.space()

            # Show endpoints
            ui.label(" | ".join([f"{k.upper()}: {v}" for k,v in DEMO["endpoints"].items()])).classes("tracking-wide")

            ui.space()

            ui.spinner("ios", size="2em", color="red").bind_visibility_from(
                app.storage.user, "busy"
            ).tooltip("Busy")

            ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                app.storage.user, "busy", lambda x: not x
            ).tooltip("Ready")

        log = (
            ui.log()
            .classes("w-full h-48 bg-neutral-300/30")
            .style("white-space: pre-wrap")
            .bind_visibility(app.storage.user, "showlog")
        )
        logger.addHandler(LogElementHandler(log, level=logging.INFO))

