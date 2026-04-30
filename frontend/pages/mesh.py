from nicegui import ui

from theme import apply_colors
from components.header import header
from components.footer import footer
from components.gui import mesh_ii
from components.monitoring import init_monitoring_timer


@ui.page("/", title="Data Fabric Mesh")
async def mesh_page():
    apply_colors()
    header("Data Fabric")
    mesh_ii().classes("flex-1")
    footer()
    init_monitoring_timer()
