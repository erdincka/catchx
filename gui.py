import os
import shutil
import logging
from nicegui import ui, app

from common import *
from functions import *
from ingestion import *
from mock import *
from monitoring import *
from codeviewers import *


logger = logging.getLogger("gui")

active_dialog = None


async def mesh_ii():
    """Draw an interactive image that shows the data mesh architecture"""

    layer_color = "green"
    opacity = "0.10"
    rest_of_svg = f'fill={layer_color} fill-opacity={opacity} stroke="none" stroke-width:"0" pointer-events="all" cursor="pointer"'

    with ui.interactive_image(
        DIAGRAM,
        content=f"""
        <rect id="NFS" x="15" y="10" rx="20" ry="20" width="465" height="210" {rest_of_svg} />
        <rect id="Fraud" x="718" y="130" rx="60" ry="60" width="1345" height="1020" {rest_of_svg} />
        <rect id="S3" x="7070" y="70" rx="20" ry="20" width="415" height="150" {rest_of_svg} />
        <rect id="IAM" x="670" y="1380" rx="20" ry="20" width="450" height="250" {rest_of_svg} />
        <rect id="Policies" x="5745" y="1365" rx="90" ry="20" width="495" height="280" {rest_of_svg} />
        <rect id="Catalogue" x="6290" y="1365" rx="90" ry="20" width="590" height="280" {rest_of_svg} />
        <rect id="Edge" x="250" y="2810" width="860" height="280" {rest_of_svg} />
        # """,
        # ).on("svg:pointerover", lambda e: open_popup_for(e.args["element_id"])).classes(
    ).on(
        "svg:pointerup",
        lambda e: handle_image_action(e.args),
        # ).on("svg:pointerover", lambda e: handle_image_info(e.args)
    ).classes(
        "relative m-0 p-0"
    ):
        # if External S3 is configured and transactions csv exist
        async def s3_upload():
            transactions_file = (
                f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv"
            )
            if (
                os.path.lexists(transactions_file)
                and len(app.storage.general.get("S3_SECRET_KEY", "")) > 0
            ):
                ui.notify(
                    f"Copying {TABLE_TRANSACTIONS} to external S3 bucket", type="success"
                )
                await upload_to_s3(transactions_file)

        def nfs_upload():
            # Copy customers if NFS is mounted and customers csv exist
            customers_file = (
                f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv"
            )
            if os.path.lexists(customers_file) and os.path.exists(
                f"/mnt{EXTERNAL_NFS_PATH}"
            ):
                ui.notify(
                    f"Copying {TABLE_CUSTOMERS} to external NFS server", type="success"
                )
                logger.info(
                    shutil.copyfile(
                        customers_file, f"/mnt{EXTERNAL_NFS_PATH}/{TABLE_CUSTOMERS}"
                    )
                )

        ui.button(icon="upload", on_click=nfs_upload).classes(
            "absolute top-10 left-5"
        ).props("flat").bind_visibility_from(
            app.storage.general, "NFS_SERVER", backward=lambda x: len(x) > 0
        ).tooltip(
            "Upload Customers"
        )

        ui.button(icon="upload", on_click=s3_upload).classes(
            "absolute top-10 right-3"
        ).props("flat").bind_visibility_from(
            app.storage.general, "S3_SERVER", backward=lambda x: len(x) > 0
        ).tooltip(
            "Upload Transactions"
        )


# NOT USED
def add_elements_to_svg(x: int, y: int, color: str, metric: str):
    """
    HTML element to insert into SVG
    # x/y placement not working for nicegui elements
    :param x int: pixels from left
    :param y int: pixels from top
    :param color str: color of the badge
    :param metric str: badge count to read from app.storage.general

    """
    return f"""
        <foreignObject x="{x}" y="{y}">
            {
                ui.badge("0", color=color).bind_text_from(
                    app.storage.general,
                    metric,
                    lambda x: x if x is not None else 0,
                )
            }
        </foreignObject>
    """
