from nicegui import ui, app
from config import HPE_COLORS, DATA_DOMAIN, VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD
from theme import dark_header_style


def footer():
    with ui.footer().style(dark_header_style()).classes("py-1 px-4") as f:
        with ui.row().classes("w-full items-center gap-2"):

            ui.label("Volumes:").classes("text-white text-xs opacity-70")

            with ui.button_group().props("flat"):
                ui.button("GNS", on_click=lambda: _run_command("df -h /mapr; ls -lA /mapr")).classes(
                    "text-xs"
                ).props("flat color=white no-caps")

                ui.button("Domain", on_click=_list_domain).classes("text-xs").props(
                    "flat color=white no-caps"
                )

                for vol, color in [
                    (VOLUME_BRONZE, "orange"),
                    (VOLUME_SILVER, "grey-4"),
                    (VOLUME_GOLD, "yellow"),
                ]:
                    _vol = vol
                    ui.button(vol.capitalize(), on_click=lambda v=_vol: _list_volume(v)).classes(
                        "text-xs"
                    ).props(f"flat color={color} no-caps")

            ui.space()

            # GitHub link
            with ui.link(target=DATA_DOMAIN.get("link", ""), new_tab=True).classes("flex items-center"):
                ui.html("""
                <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="white" opacity="0.7">
                  <path d="M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"/>
                </svg>
                """)

    return f


def _run_command(cmd: str):
    import asyncio
    from nicegui import ui

    async def run():
        process = await asyncio.create_subprocess_shell(
            cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        stdout, _ = await process.communicate()
        log_el.push(stdout.decode())

    with ui.dialog().props("full-width") as dlg, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dlg.close).props("flat round dense").classes("absolute right-2 top-2")
        ui.label(cmd[:80]).classes("text-xs font-mono text-gray-500 mb-2")
        log_el = ui.log().classes("w-full h-64 text-xs font-mono")

    dlg.on("close", lambda d=dlg: d.delete())
    dlg.open()
    asyncio.create_task(run())


def _list_domain():
    cluster = app.storage.user.get("clusterinfo", {})
    name = cluster.get("name", "") if cluster else ""
    from config import BASEDIR, MOUNT_PATH
    _run_command(f"ls -lA {MOUNT_PATH}/{name}{BASEDIR}")


def _list_volume(vol: str):
    cluster = app.storage.user.get("clusterinfo", {})
    name = cluster.get("name", "") if cluster else ""
    from config import BASEDIR, MOUNT_PATH
    _run_command(f"ls -lAR {MOUNT_PATH}/{name}{BASEDIR}/{vol}")
