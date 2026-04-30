from contextlib import contextmanager
from nicegui import ui
from config import HPE_COLORS


def apply_colors():
    ui.colors(
        primary=HPE_COLORS["green"],
        secondary=HPE_COLORS["purple"],
        accent=HPE_COLORS["teal"],
        positive=HPE_COLORS["darkteal"],
        negative=HPE_COLORS["darkred"],
        warning=HPE_COLORS["darkorange"],
        info=HPE_COLORS["darkblue"],
        dark=HPE_COLORS["darkgreen"],
    )


def dark_header_style() -> str:
    return f"background: {HPE_COLORS['darkgreen']}; color: white;"


def card_style() -> str:
    return "border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.12);"


def badge_chip(text: str, color: str = "primary"):
    return ui.badge(text).props(f"color={color} rounded").classes("text-xs px-2 py-1")


def section_label(text: str):
    return ui.label(text).classes("text-xs font-semibold uppercase tracking-widest text-gray-500 mt-3 mb-1")


def metric_card(label: str, metric_key: str):
    """A compact metric display card bound to app.storage.user[metric_key]"""
    from nicegui import app
    with ui.card().classes("p-2 min-w-24 text-center").style(card_style()):
        ui.label(label).classes("text-xs text-gray-400 uppercase tracking-wide")
        ui.badge("0").bind_text_from(app.storage.user, metric_key).props(
            "color=negative rounded"
        ).classes("text-sm font-bold mt-1")
