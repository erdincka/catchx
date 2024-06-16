import logging
from math import pi
from nicegui import ui

logger = logging.getLogger("gui")


active_dialog = None


def handle_mesh_element(element: str):
    logger.debug("Entered %s", element)

    # clear existing dialog
    global active_dialog
    if active_dialog:
        active_dialog.clear()

    with ui.dialog(value=True).props(
        f"seamless position=bottom"
    ) as dialog, ui.card().classes("w-fit relative opacity-90"):
        ui.label(element.replace("_", " "))
        ui.button(icon="close", on_click=dialog.close).props(
        "flat round dense"
        ).classes("absolute right-2 top-2")

        with ui.card_section():
            if element == "NFS": label = "Bring existing datalakes into the 'Mesh'."
            elif element == "S3": label = "Bring existing Object Stores into the 'Mesh'."
            elif element == "ADaaS": label = "Globally manage and integrate with IAM for auth/z"
            elif element == "Edge": label = "'Data Domains' without 'Data Products' can join to the Mesh too"
            elif element == "Policies": label = "Policies"
            elif element == "Catalogue": label = "Catalogue"
            else: label = "Not defined yet"
            ui.label(label)

        with ui.card_actions():
            if element == "NFS": ui.button("Info", on_click=lambda: ui.notify(element))
            elif element == "S3": ui.button("Info", on_click=lambda: ui.notify(element))
            elif element == "ADaaS": ui.button("Info", on_click=lambda: ui.notify(element))
            elif element == "Edge": ui.button("Info", on_click=lambda: ui.notify(element))
            else: ui.button("Noop", on_click=lambda: ui.notify("Nothing here!")).props("outline")

    # set existing dialog
    active_dialog = dialog

    dialog.open()
    dialog.on("close", dialog.clear)  # delete dialog from dom


# Interactive Images
def mesh_ii():
    """Draw an interactive image that shows the data mesh architecture"""

    layer_color = "green"
    opacity = "0.10"

    with ui.interactive_image(
        "/images/hubspoke.png",
        content=f"""
        <rect id="NFS" x="15" y="10" rx="30" ry="30" width="465" height="210" fill={layer_color} fill-opacity={opacity} stroke={layer_color} pointer-events="all" cursor="pointer" />
        <rect id="Fraud" x="718" y="130" rx="30" ry="30" width="1335" height="1008" fill={layer_color} fill-opacity={opacity} stroke={layer_color} pointer-events="all" cursor="pointer" />
        <rect id="S3" x="7075" y="70" rx="30" ry="30" width="415" height="145" fill={layer_color} fill-opacity={opacity} stroke={layer_color} pointer-events="all" cursor="pointer" />
        <rect id="ADaaS" x="670" y="1380" rx="30" ry="30" width="450" height="240" fill={layer_color} fill-opacity={opacity} stroke={layer_color} pointer-events="all" cursor="pointer" />
        <rect id="Policies" x="5740" y="1360" rx="60" ry="30" width="495" height="280" fill={layer_color} fill-opacity={opacity} stroke={layer_color} pointer-events="all" cursor="pointer" />
        <rect id="Catalogue" x="6285" y="1360" rx="60" ry="30" width="585" height="280" fill={layer_color} fill-opacity={opacity} stroke={layer_color} pointer-events="all" cursor="pointer" />
        <rect id="Edge" x="250" y="2810" width="865" height="280" fill={layer_color} fill-opacity={opacity} stroke={layer_color} pointer-events="all" cursor="pointer" />
        # """,
        # ).on("svg:pointerover", lambda e: open_popup_for(e.args["element_id"])).classes(
    ).on("svg:pointerup", lambda e: handle_mesh_element(e.args["element_id"])).classes(
        "relative"
    ):

        ui.button(on_click=lambda: ui.notify("thumbs up"), icon="thumb_up").props(
            "flat fab color=blue"
        ).classes("absolute bottom-0 right-0")


def domain_ii():
    """Draw an interactive image that shows demo pipeline for the data domain"""

    layer_color = "red"
    opacity = "0.25"

    with ui.interactive_image(
        "/images/datapipeline.png",
        content=f"""
        <rect id="publish_transactions" x="200" y="1470" rx="80" ry="80" width="360" height="360" fill={layer_color} fill-opacity={opacity} stroke={layer_color} stroke-width:"5" pointer-events="all" cursor="pointer" />
        <rect id="create_csv_files" x="200" y="2400" rx="80" ry="80" width="360" height="360" fill={layer_color} fill-opacity={opacity} stroke={layer_color} stroke-width:"5" pointer-events="all" cursor="pointer" />
        # """,
    ).on("svg:pointerover", lambda e: handle_mesh_element(e.args["element_id"])).classes(
        "relative"
    ):

        ui.button(on_click=lambda: ui.notify("thumbs up"), icon="thumb_up").props(
            "flat fab color=blue"
        ).classes("absolute top-0 left-0 m-2")


def datamesh3d():
    with ui.row().classes("w-full"):
        with ui.scene(background_color="#222").on_click(lambda x: print(x)).classes("h-90") as scene:
            scene.box().move(-3,-2).with_name("NFSv4")
            scene.extrusion([[0, 0], [0, 1], [1, 1]], 1).material("#ff8888").move(
                2, -1
            )
            scene.sphere().material("#4488ff")
            scene.cylinder(1, 1, 1, 32).material("#ff8800", opacity=0.5).move(
                -2, 1
            ).rotate(pi/2, 0, 0)

            scene.line([-4, 0, 0], [-4, 2, 0]).material("#ff0000")
            # logo = 'https://avatars.githubusercontent.com/u/2843826'
            # scene.texture(logo, [[[0.5, 2, 0], [2.5, 2, 0]],
            #                     [[0.5, 0, 0], [2.5, 0, 0]]]).move(1, -3)

            # teapot = 'https://upload.wikimedia.org/wikipedia/commons/9/93/Utah_teapot_(solid).stl'
            # scene.stl(teapot).scale(0.2).move(-3, 4)

            # avocado = 'https://raw.githubusercontent.com/KhronosGroup/glTF-Sample-Assets/main/Models/Avocado/glTF-Binary/Avocado.glb'
            # scene.gltf(avocado).scale(40).move(-2, -3, 0.5)

            scene.text('Data Hub', 'background: rgba(0, 0, 0, 0.2); border-radius: 5px; padding: 5px').move(z=2)
            # scene.text3d('3D', 'background: rgba(0, 0, 0, 0.2); border-radius: 5px; padding: 5px').move(y=-2).scale(.05)
