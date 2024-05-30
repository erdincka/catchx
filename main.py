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
    with ui.expansion("Data Mesh", caption="Build a globally distributed mesh with delegated data products", icon="dashboard").classes("w-full"):
        # meshmap()
        ui.button("Namespace", on_click=lambda: run_command_with_dialog("df -h /mapr; ls -lA /mapr/"))

    ui.separator()

    # Documentation / Intro
    info()

    ui.separator()

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

