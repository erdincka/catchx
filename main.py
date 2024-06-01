from nicegui import app, ui

from map import meshmap
from monitoring import *
from functions import *
from page import *


# catch-all exceptions
app.on_exception(gracefully_fail)

app.add_static_files('/images', 'images')

set_logging()

@ui.page("/")
async def home():
    # Initialize app parameters
    app_init()

    # Page header
    header()

    # Data Mesh
    # with ui.expansion("Data Mesh", caption="Build a globally distributed mesh with delegated data products", icon="dashboard").classes("w-full").bind_value(app.storage.user, "mapview"):
    #     meshmap()

    ui.separator()

    # Documentation / Intro
    info()

    ui.separator()

    # Canvas
    with ui.interactive_image("/images/DataPipeline.png", content='''
        <rect id="publish_transactions" x="200" y="1470" rx="80" ry="80" width="360" height="360" fill="none" stroke="red" stroke-width:"5" pointer-events="all" cursor="pointer" />
        <rect id="create_csv_files" x="200" y="2400" rx="80" ry="80" width="360" height="360" fill="none" stroke="red" stroke-width:"5" pointer-events="all" cursor="pointer" />
        ''').on('svg:pointerup', lambda e: ui.notify(f"Selected: {e.args['element_id']}")):

        ui.button(on_click=lambda: ui.notify('thumbs up'), icon='thumb_up') \
        .props('flat fab color=blue') \
        .classes('absolute top-0 left-0 m-2')

    # Main
    with ui.row().classes("w-full flex flex-nowrap relative"):
        demo_steps()

        monitoring_charts()
        
    # Page footer
    footer()

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title=TITLE,
        dark=None,
        storage_secret=STORAGE_SECRET,
        reload=True,
        port=3000,
    )

