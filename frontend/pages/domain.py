from nicegui import ui

from config import DATA_PRODUCT
from theme import apply_colors
from components.header import header
from components.footer import footer
from components.gui import domain_ii
from components.monitoring import monitoring_ticker, logging_card, init_monitoring_timer


@ui.page(f"/{DATA_PRODUCT}", title="Fraud Data Domain")
async def domain_page():
    apply_colors()
    header(DATA_PRODUCT.title())
    monitoring_ticker().classes("w-full")
    domain_ii().classes("flex-1")
    logging_card().classes("w-full opacity-60 hover:opacity-100 transition-opacity")
    footer()
    init_monitoring_timer()
