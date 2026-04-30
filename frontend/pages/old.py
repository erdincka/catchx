from nicegui import ui

from theme import apply_colors
from components.header import header
from components.footer import footer
from components.monitoring import monitoring_card, logging_card, init_monitoring_timer
from components.demo_steps import demo_steps


@ui.page("/old", title="Data Fabric Demo")
async def old_page():
    apply_colors()
    header("Data Fabric Demo")

    with ui.grid(columns=2).classes("w-full gap-0 flex-1"):
        demo_steps().classes("flex-1")
        monitoring_card().classes("w-64 shrink-0 p-2")

    logging_card().classes(
        "fixed bottom-16 left-0 right-0 mx-4 opacity-50 hover:opacity-100 transition-opacity z-10"
    )

    footer()
    init_monitoring_timer()
