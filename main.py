from nicegui import app, ui

from chart import *
from functions import *
from page import *


# catch-all exceptions
app.on_exception(gracefully_fail)

set_logging()

@ui.page("/")
async def home():
    # Initialize app parameters
    app_init()

    # Page header
    header()

    # Documentation / Intro
    info()

    ui.separator()

    # Demo steps
    demo_steps()

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

