import logging
from nicegui import ui

logger = logging.getLogger("gui")


active_dialog =  None

def open_popup_for(element: str):
    logger.info("Enter %s", element)

    global active_dialog
    # clear existing dialog
    if active_dialog:
        active_dialog.clear()

    with ui.dialog(value=True).props(":backdrop-filter=saturate(80%) seamless position=right") as dialog, ui.card().classes("w-64 relative opacity-50"):
        ui.label(element.replace("_", " ").title())
        ui.space()
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")
        with ui.card_actions():
            ui.button("Code", on_click=lambda: ui.notify(element))
            ui.button("Zort", on_click=lambda: ui.notify("Zort")).props("outline")

    # set existing dialog
    active_dialog = dialog

    dialog.open()
    dialog.on("close", dialog.clear) # delete dialog from dom


# Interactive Image
def ii():
    """Return demo steps marked on an interactive image"""

    with ui.interactive_image("/images/DataPipeline.png", content='''
        <rect id="publish_transactions" x="200" y="1470" rx="80" ry="80" width="360" height="360" fill="none" stroke="red" stroke-width:"5" pointer-events="all" cursor="pointer" />
        <rect id="create_csv_files" x="200" y="2400" rx="80" ry="80" width="360" height="360" fill="none" stroke="red" stroke-width:"5" pointer-events="all" cursor="pointer" />
        # ''')\
        .on('svg:pointerover', lambda e: open_popup_for(e.args['element_id'])).classes("relative"):

        ui.button(on_click=lambda: ui.notify('thumbs up'), icon='thumb_up') \
        .props('flat fab color=blue') \
        .classes('absolute top-0 left-0 m-2')
