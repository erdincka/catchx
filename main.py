from nicegui import app, ui

from monitoring import *
from functions import *
from page import *
import gui
import theme


# catch-all exceptions
app.on_exception(gracefully_fail)
app.on_disconnect(app_init)

# serve images
app.add_static_files("/images", local_directory="/app/images")

# configure the logging
configure_logging()

logger = logging.getLogger("main")


@ui.page("/")
async def index_page():
    # Initialize app parameters
    app_init()

    # Main
    with theme.frame("Hub"):
        # TODO: proper/better description below
        # ui.markdown(
        #     """
        #     Create a globally distributed Data Mesh architecture using HPE Ezmeral Data Fabric.

        #     Data Fabric provides a modern data platform on hybrid deployment scenarios and enables organisations with advanced capabilities,
        #     such as the ability to implement data products across different organisations, projects, teams to own and share their Data Products.

        #     With its multi-model, multi-protocol data handling capabilities, as well as it enterprise features and cloud-scale, organisations can
        #     realise the true value from a living data system.
        #     """
        # )
        await gui.mesh_ii()

    # with ui.row().classes("w-full flex flex-nowrap relative"):
    #     demo_steps()


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title=TITLE,
        dark=False,
        storage_secret=STORAGE_SECRET,
        reload=True,
        port=3000,
    )
