import logging
import os
import sys
import importlib_resources

import requests
from nicegui import app, ui, binding

from elements import get_echart, metrics_chart
from functions import *


logging.basicConfig(level=logging.INFO,
                format="%(asctime)s:%(name)s:%(levelname)s:%(module)s (%(funcName)s): %(message)s",
                datefmt='%H:%M:%S')

logger = logging.getLogger()

# https://sam.hooke.me/note/2023/10/nicegui-binding-propagation-warning/
binding.MAX_PROPAGATION_TIME = 0.05


@ui.page("/")
async def home():
    # Keep app view between runs
    if "ui" not in app.storage.general:
        app.storage.general["ui"] = {}

    # and previous run state if it was hang
    app.storage.user["busy"] = False

    # and log window
    app.storage.user["showlog"] = False

    # and ui counters
    app.storage.user["ui"] = {}

    # Header
    with ui.header(elevated=True).style('background-color: #3874c8').classes('items-center justify-between uppercase'):
        ui.label(f"{APP_NAME}: {TITLE}")
        ui.space()

        # Env
        ui.label("Cluster:")
        ui.link(target=f"https://{os.environ['MAPR_USER']}:{os.environ['MAPR_PASS']}@{os.environ['MAPR_IP']}:8443/", new_tab=True).bind_text_from(os.environ, "MAPR_CLUSTER").classes("text-sky-300 hover:text-blue-600")
        ui.space()
        ui.label("Volume:")
        ui.label(DEMO["volume"])
        ui.space()
        ui.label("Stream:")
        ui.label(DEMO["stream"])
        ui.space()
        ui.label("Table:")
        ui.label(DEMO["table"])
        ui.space()

        ui.switch(on_change=toggle_debug).bind_value(app.storage.user, "debugging").tooltip("Debug")

    # Documentation / Intro
    with ui.expansion(
        TITLE,
        icon="info",
        caption="End to end pipeline processing using Ezmeral Data Fabric",
    ).classes("w-full").classes("text-bold") as info:
        ui.markdown(DEMO["description"]).classes("font-normal")
        ui.image(importlib_resources.files("main").joinpath(DEMO["diagram"])).classes(
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
    #     "Fraud Detection using Data Fabric",
    #     icon="credit_score",
    #     caption="React to every transaction in realtime ",
    # ).classes("w-full").classes("text-bold") as home:
    #     with ui.stepper().props("vertical header-nav").classes("w-full") as stepper:
    #         for step in DEMO["steps"]:
    #             with ui.step(name=step["name"].title()):
    #                 ui.markdown(step["description"]).classes("font-normal")
    #                 for code in step.get("codes", []):
    #                     t = code.split('.')
    #                     ui.code(inspect.getsource(getattr(__import__(t[0]), t[1]))).classes("w-full text-wrap")
    #                 if step["runner"] == "rest":
    #                     ui.label(
    #                         f"https://{os.environ['MAPR_IP']}:8443{step['runner_target']}"
    #                     )

    #                 elif step['runner'] in ['app', 'noop']:
    #                     if step['runner'] != "noop":
    #                         # Display function params if 'count' is provided
    #                         if 'count' in step.keys():
    #                             ui.label().bind_text_from(
    #                                 app.storage.user,
    #                                 f"count__{DEMO['name'].replace(' ', '_')}__{step['id']}",
    #                                 # format message to show function(param, count) format
    #                                 backward=lambda x, y=step.get(
    #                                     "runner_parameters", None
    #                                 ), p=step["runner_target"]: f"Will be running: {p}({y}, {x})"
    #                             )
    #                         else:
    #                             ui.label(f"Will be running: {step['runner_target']} ({step.get('runner_parameters', None)} )")

    #                     # Insert count slider if step wants one
    #                     if step.get("count", None):
    #                         with ui.row().classes("w-full"):
    #                             slider = (
    #                                 ui.slider(
    #                                     min=0,
    #                                     max=3 * step["count"],
    #                                     step=1,
    #                                     value=step["count"],
    #                                     # saving selection in user storage, with demo name (spaces substituted with _), step id and parameter
    #                                 )
    #                                 .bind_value_to(
    #                                     app.storage.user,
    #                                     f"count__{DEMO['name'].replace(' ', '_')}__{step['id']}",
    #                                 )
    #                                 .classes("w-5/6 self-center")
    #                             )
    #                             ui.label().bind_text_from(
    #                                 slider, "value"
    #                             ).classes("self-center")

    #                     # Get input if step wants one
    #                     if step.get("input", None):
    #                         ui.input(
    #                             step["input"],
    #                             placeholder="We need this input to proceed",
    #                         ).bind_value_to(
    #                             app.storage.user,
    #                             # saving selection in user storage, with demo name (spaces substituted with _), step id and parameter
    #                             f"input__{DEMO['name'].replace(' ', '_')}__{step['id']}",
    #                         ).classes(
    #                             "w-full"
    #                         )


    #                 with ui.stepper_navigation():
    #                     ui.button(
    #                         "Run",
    #                         icon="play_arrow",
    #                         on_click=lambda step=step, pager=stepper: run_step(
    #                                 step, pager
    #                             ),
    #                     ).bind_enabled_from(
    #                         app.storage.user,
    #                         "busy",
    #                         backward=lambda x: not x,
    #                     ).bind_visibility_from(
    #                         step, "runner", backward=lambda x: x != "noop"
    #                     )
    #                     ui.button(
    #                         "Next",
    #                         icon="fast_forward",
    #                         on_click=stepper.next,
    #                         color="secondary"
    #                     ).props("flat")
    #                     ui.button(
    #                         "Back",
    #                         icon="fast_rewind",
    #                         on_click=stepper.previous,
    #                     ).props("flat").bind_visibility_from(
    #                         step, "id", backward=lambda x: x != 1
    #                     )  # don't show for the first step

    # home.bind_value(app.storage.general.get("ui", {}), "demo")

    # Services
    ui.button("Start transaction feed", on_click=transaction_feed_service)
    ui.switch().bind_value(app.storage.general, "txn_feed_svc").bind_text_from(app.storage.general, "txn_feed_svc", backward=lambda x: "Running" if x else "Stopped")


    with ui.row().classes("w-full"):
        for svc in DEMO['services']:
            svc_chart = get_echart()
            metrics_chart(svc, svc_chart)

    with ui.footer() as footer:
        with ui.row().classes("w-full items-center"):
            ui.button(icon="menu", on_click=toggle_log).props("flat text-color=white")
            ui.label("Log")
            ui.space()
            ui.spinner("ios", size="2em", color="red").bind_visibility_from(
                app.storage.user, "busy"
            ).tooltip("Busy")
            ui.icon("check_circle", size="2em", color="green").bind_visibility_from(
                app.storage.user, "busy", lambda x: not x
            ).tooltip("Ready")
        log = (
            ui.log()
            .classes("w-full h-48 bg-neutral-300/30")
            .style("white-space: pre-wrap")
            .bind_visibility(app.storage.user, "showlog")
        )
        logger.addHandler(LogElementHandler(log, level=logging.INFO))


TITLE = "Data Fabric End to End Data Pipeline"
STORAGE_SECRET = "ezmer@1r0cks"

# enable pyspark from /opt/mapr
sys.path.insert(1, "/opt/mapr/spark/spark-3.3.3/python/lib/pyspark.zip")
sys.path.insert(1, "/opt/mapr/spark/spark-3.3.3/python/lib/py4j-0.10.9.5-src.zip")

# INSECURE REQUESTS ARE OK in Lab
requests.packages.urllib3.disable_warnings()
urllib_logger = logging.getLogger("urllib3.connectionpool")
urllib_logger.setLevel(logging.WARNING)

requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.WARNING)

watcher_logger = logging.getLogger("watchfiles.main")
watcher_logger.setLevel(logging.FATAL)

faker_log = logging.getLogger("faker.factory")
faker_log.setLevel(logging.FATAL)

if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title=TITLE,
        dark=None,
        storage_secret=STORAGE_SECRET,
        reload=True,
        port=3000,
    )

app.on_exception(gracefully_fail)
