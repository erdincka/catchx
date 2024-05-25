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

    # Main
    with ui.row().classes("w-full flex flex-nowrap"):
        demo_steps()

        # Monitoring charts
        with ui.card().classes("flex-grow shrink"):
            topic_chart = get_echart().run_chart_method('showLoading')
            # consumer_chart = get_echart().run_chart_method('showLoading')

            # ui.timer(MON_REFRESH_INTERVAL, lambda: update_chart(topic_stats(DEMO["endpoints"]["topic"]), topic_chart))
            # ui.timer(MON_REFRESH_INTERVAL, lambda: update_chart(consumer_stats(DEMO["endpoints"]["topic"]), consumer_chart))

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

