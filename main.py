import inspect
import logging

import requests
from nicegui import app, ui, binding

from functions import *
import apprunner


logger = logging.getLogger()
# Console logger
ch = logging.StreamHandler()
ch.setFormatter(
    logging.Formatter(
        "%(asctime)s:%(levelname)s:%(name)s:%(funcName)s:%(lineno)d: %(message)s",
        datefmt="%H:%M:%S",
    )
)
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

# https://sam.hooke.me/note/2023/10/nicegui-binding-propagation-warning/
binding.MAX_PROPAGATION_TIME = 0.05


@ui.page("/")
def home():
    # Keep app view between runs
    if "ui" not in app.storage.general:
        app.storage.general["ui"] = {}

    # and previous run state if it was hang
    app.storage.user["busy"] = False

    # and log window
    app.storage.user["showlog"] = True

    # and ui counters
    app.storage.user["ui"] = {}

    # Documentation / Intro
    with ui.expansion(
        "End-to-End Pipeline Demo",
        icon="info",
        caption="End to end pipeline processing using Ezmeral Data Fabric",
    ).classes("w-full").classes("text-bold") as info:
        ui.markdown(DEMO["description"]).classes("font-normal")
        # ui.image(importlib_resources.files("app").joinpath(DEMO["diagram"])).classes(
        ui.image(DEMO["diagram"]).classes(
            "object-scale-down g-10"
        )
        ui.link(
            "Source",
            target=DEMO.get("link", ""),
            new_tab=True,
        ).bind_visibility_from(DEMO, "link", backward=lambda x: x is not None)

    info.bind_value(app.storage.general.get("ui", {}), "info")

    ui.separator()

    # with ui.expansion(
    #     "Set up the demo environment", icon="engineering", caption="Prepare to run"
    # ).classes("w-full").classes("text-bold") as setup:
    #     with ui.row().classes("align-middle"):
    #         ui.input("CLDB Host").bind_value(app.storage.general, "host")
    #         ui.input("Username").bind_value(app.storage.general, "username")
    #         ui.input("Password", password=True, password_toggle_button=True).bind_value(
    #             app.storage.general, "password"
    #         )

    #     ui.label("Setup the target cluster")
    #     ui.label("Create the volume, topics and tables to use with this demo.")

    #     with ui.expansion("Code").classes("w-full"):
    #         ui.code(inspect.getsource(prepare)).classes("w-full")
    #     ui.button("Setup", on_click=prepare).bind_enabled_from(
    #         app.storage.user, "busy", lambda x: not x
    #     )

    # setup.bind_value(app.storage.general.get('ui', {}), "setup")
    # ui.separator()

    with ui.expansion(
        "Fraud Detection using Data Fabric",
        icon="credit_score",
        caption="React to every transaction  in realtime ",
    ).classes("w-full").classes("text-bold") as main:
        with ui.row():
            ui.label("Using volume:")
            ui.label(DEMO["volume"])

        with ui.stepper().props("vertical header-nav").classes("w-full") as stepper:
            for step in DEMO["steps"]:
                with ui.step(step["id"], title=step["name"].title()).props(
                    f"active-icon=play_arrow caption=\"using {step['runner']}\""
                ):
                    ui.markdown(step["description"]).classes("font-normal")
                    if step["runner"] == "rest":
                        ui.label(
                            f"https://{app.storage.general['host']}:8443{step['runner_target']}"
                        )
                    elif step['runner'] in ['app', 'noop']:

                        if step['runner'] != "noop":
                            # Display function params if 'count' is provided
                            if 'count' in step.keys():
                                ui.label().bind_text_from(
                                    app.storage.user,
                                    f"count__{DEMO['name'].replace(' ', '_')}__{step['id']}",
                                    # format message to show function(param, count) format
                                    backward=lambda x, y=step.get(
                                        "runner_parameters", None
                                    ), p=step["runner_target"]: f"Will be running: {p}( {y}, {x} )"
                                )
                            else:
                                ui.label(f"Will be running: {step['runner_target']} ( {step.get('runner_parameters', None)} )" )

                        # Insert count slider if step wants one
                        if step.get("count", None):
                            with ui.row().classes("w-full"):
                                slider = (
                                    ui.slider(
                                        min=0,
                                        max=3 * step["count"],
                                        step=1,
                                        value=step["count"],
                                        # saving selection in user storage, with demo name (spaces substituted with _), step id and parameter
                                    )
                                    .bind_value_to(
                                        app.storage.user,
                                        f"count__{DEMO['name'].replace(' ', '_')}__{step['id']}",
                                    )
                                    .classes("w-5/6 self-center")
                                )
                                ui.label().bind_text_from(
                                    slider, "value"
                                ).classes("self-center")

                        # Get input if step wants one
                        if step.get("input", None):
                            ui.input(
                                step["input"],
                                placeholder="We need this input to proceed",
                            ).bind_value_to(
                                app.storage.user,
                                # saving selection in user storage, with demo name (spaces substituted with _), step id and parameter
                                f"input__{DEMO['name'].replace(' ', '_')}__{step['id']}",
                            ).classes(
                                "w-full"
                            )

                        with ui.stepper_navigation():
                            ui.button(
                                "Run",
                                icon="play_arrow",
                                on_click=lambda demo=DEMO, step=step, pager=stepper: run_step(
                                    demo, step, pager
                                ),
                            ).bind_enabled_from(
                                app.storage.user,
                                "busy",
                                backward=lambda x: not x,
                            ).bind_visibility_from(
                                step, "runner", backward=lambda x: x != "noop"
                            )
                            ui.button(
                                "Next",
                                icon="fast_forward",
                                on_click=stepper.next,
                            ).props("color=secondary flat")
                            ui.button(
                                "Back",
                                icon="fast_rewind",
                                on_click=stepper.previous,
                            ).props("flat").bind_visibility_from(
                                step, "id", backward=lambda x: x != 1
                            )  # don't show for the first step

    main.bind_value(app.storage.general.get("ui", {}), "demo")

    with ui.footer() as footer:
        with ui.row().classes("w-full items-center"):
            ui.button(icon="menu", on_click=toggle_log).props("flat text-color=white")
            ui.label("Log")
            ui.space()
            ui.spinner("ios", size="2em", color="red").bind_visibility_from(
                app.storage.user, "busy"
            )
            ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                app.storage.user, "busy", lambda x: not x
            )
        log = (
            ui.log()
            .classes("w-full h-48 bg-neutral-300/30")
            .style("white-space: pre-wrap")
            .bind_visibility(app.storage.user, "showlog")
        )
        logger.addHandler(LogElementHandler(log, level=logging.INFO))


async def run_step(demo, step, pager: ui.stepper):
    app.storage.user["busy"] = True

    if step["runner"] == "rest":
        url = str(step["runner_target"])

        if step.get("use_demo_input", None):
            op = step["use_demo_input"]
            if op == "append":
                url += app.storage.user[f"input__{DEMO['name'].replace(' ', '_')}"]
            elif op == "replace":
                url = url.replace(
                    "PLACEHOLDER",
                    app.storage.user[f"input__{DEMO['name'].replace(' ', '_')}"],
                )
                # logger.debug("URL %s", url)
            elif op == "prepend":
                url = app.storage.user[f"input__{DEMO['name'].replace(' ', '_')}"] + url
            else:
                logger.warning("Unknown input operation %s", op)

        response = await run.io_bound(restrunner.post, url)

        if response is None:
            ui.notify("No response", type="negative")

        elif response.ok:
            resjson = response.json()
            # logger.debug("DEBUG RESTRUNNER RETURN %s", resjson)
            # I took the lazy approach here since different rest calls have different return formats (except the 'status')
            ui.notify(
                resjson, type="positive" if resjson["status"] == "OK" else "warning"
            )

            if resjson["status"] == "OK":
                pager.next()

        else:  # http error returned
            logger.warning("REST HTTP ERROR %s", response.text)
            ui.notify(message=response.text, html=True, type="warning")

    # elif step["runner"] == "restfile":
    #     for response in await run.io_bound(
    #         restrunner.postfile, DEMOS, step["runner_target"]
    #     ):
    #         if isinstance(response, Exception):
    #             ui.notify(response, type="negative")
    #         elif response.ok:
    #             try:
    #                 # logger.debug("DEBUG: RESPONSE FROM RESTRUNNER: %s", response)
    #                 if response.status_code == 201:
    #                     ui.notify("Folder created")
    #                 else:
    #                     resjson = response.json()
    #                     ui.notify(resjson)
    #             except Exception as error:
    #                 logger.debug("RESTFILE ERROR: %s", error)

    #             pager.next()
    #         else:  # http error returned
    #             ui.notify(message=response.text, html=True, type="warning")

    elif step["runner"] == "app":
        func = getattr(apprunner, step["runner_target"])

        if "count" in step.keys():
            app.storage.general["ui"]["counting"] = 0
            # keep user selected count in sync
            count = app.storage.user[
                f"count__{DEMO['name'].replace(' ', '_')}__{step['id']}"
            ]

            # add progressbar if 'count'ing
            ui.linear_progress().bind_value_from(
                app.storage.general["DEMO"],
                "counting",
                backward=lambda x: f"{100 * int(x/count)} %",
            )

            if inspect.isgeneratorfunction(func):
                for response in await run.io_bound(
                    func, step["runner_parameters"], count
                ):
                    ui.notify(response)
            else:
                await run.io_bound(func, step["runner_parameters"], count)

        else:
            if inspect.isgeneratorfunction(func):
                for response in await run.io_bound(
                    func, step.get("runner_parameters", None)
                ):
                    ui.notify(response)

            else:
                await run.io_bound(func, step.get("runner_parameters", None))

        # pager.next()

    else:
        ui.notify(
            f"Would run {step['runner_target']} using {step['runner']} but it is not here yet!"
        )
        pager.next()

    app.storage.user["busy"] = False


TITLE = "Data Fabric End to End Data Pipeline"
STORAGE_SECRET = "ezmer@1r0cks"

# INSECURE REQUESTS ARE OK in Lab
requests.packages.urllib3.disable_warnings()
urllib_logger = logging.getLogger("urllib3.connectionpool")
urllib_logger.setLevel(logging.WARNING)

requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.WARNING)

faker_log = logging.getLogger("faker.factory")
faker_log.setLevel(logging.FATAL)


# Entry point for the module
def enter():
    ui.run(
        title=TITLE,
        dark=None,
        storage_secret=STORAGE_SECRET,
        reload=False,
    )


# For development and debugs
if __name__ in {"__main__", "__mp_main__"}:
    print("Running in DEV")

    ui.run(
        title=TITLE,
        dark=None,
        storage_secret=STORAGE_SECRET,
        reload=True,
    )


app.on_exception(gracefully_fail)
# app.on_disconnect()
# app.on_connect(start_services)
