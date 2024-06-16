from nicegui import app, ui

from map import meshmap
from monitoring import *
from functions import *
from page import *
import gui


# catch-all exceptions
app.on_exception(gracefully_fail)

# serve images
app.add_static_files("/images", local_directory="/app/images")

# configure the logging
configure_logging()

logger = logging.getLogger("main")


@ui.page("/")
async def home():
    # Initialize app parameters
    app_init()

    # Page header
    header()
    # Page footer
    footer()

    # gui.datamesh3d()
    # meshmap()

    # Data Mesh
    with ui.expansion(
        "Data Mesh",
        caption="Build a globally distributed mesh with federated data domains",
        group="navigation",
        icon="dashboard",
    ).classes("w-full").bind_value(app.storage.user, "meshview"):
        # TODO: proper/better description below
        ui.markdown(
            """
            Create a globally distributed Data Mesh architecture using HPE Ezmeral Data Fabric.
                    
            Data Fabric provides a modern data platform on hybrid deployment scenarios and enables organisations with advanced capabilities,
            such as the ability to implement data products across different organisations, projects, teams to own and share their Data Products.

            With its multi-model, multi-protocol data handling capabilities, as well as it enterprise features and cloud-scale, organisations can
            realise the true value from a living data system.
            """
        )
        # ui.image("/images/hubspoke.png").classes("object-scale-down g-10")
        gui.mesh_ii()

    ui.separator()

    # Data Domain
    with ui.expansion(
        "Data Domain",
        icon="info",
        group="navigation",
        caption="End to end data pipeline using Ezmeral Data Fabric for financial transaction processing",
    ).classes("w-full text-bold"):
        ui.markdown(DATA_DOMAIN["description"]).classes("font-normal")
        ui.image(f"/images/{DATA_DOMAIN['diagram']}").classes("object-scale-down g-10")
        ui.link(
            "Source",
            target=DATA_DOMAIN.get("link", ""),
            new_tab=True,
        ).bind_visibility_from(DATA_DOMAIN, "link", backward=lambda x: x is not None)

    ui.separator()

    # Main
    with ui.row().classes("w-full flex flex-nowrap relative"):
        demo_steps()

        # await monitoring_charts()


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title=TITLE,
        dark=None,
        storage_secret=STORAGE_SECRET,
        reload=True,
        port=3000,
    )
