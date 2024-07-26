from contextlib import contextmanager

from nicegui import ui

from page import header, footer


@contextmanager
def frame(page_title: str):
    """Custom page frame to share the same styling and behavior across all pages"""
    # ui.colors(
    #     primary=HPE_COLORS["darkgreen"], secondary=HPE_COLORS["darkpurple"], accent=HPE_COLORS["darkblue"], positive=HPE_COLORS["darkteal"], negative=HPE_COLORS["darkred"], warning=HPE_COLORS["darkorange"]
    # )

    header(page_title)

    footer()

    with ui.column().classes('items-center'):
        yield
