import inspect
import logging
from urllib.parse import urlparse
from nicegui import app, run, ui

from helpers import *
import restrunner, runners

logger = logging.getLogger()


async def run_step(step, pager: ui.stepper):
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
                logger.debug("URL %s", url)

            elif op == "prepend":
                url = app.storage.user[f"input__{DEMO['name'].replace(' ', '_')}"] + url

            else:
                logger.warning("Unknown input operation %s", op)

        response = await run.io_bound(restrunner.post, url)

        if response is None:
            ui.notify("No response", type="negative")

        elif response.ok:
            resjson = response.json()
            logger.debug("DEBUG RESTRUNNER RETURN %s", resjson)
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
        func = getattr(runners, step["runner_target"])

        if "count" in step.keys():
            app.storage.general["ui"]["counting"] = 0
            # keep user selected count in sync
            count = app.storage.user[
                f"count__{DEMO['name'].replace(' ', '_')}__{step['id']}"
            ]

            # add progressbar if 'count'ing
            ui.linear_progress().bind_value_from(
                app.storage.general["ui"],
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


def toggle_log():
    app.storage.user["showlog"] = not app.storage.user["showlog"]


def toggle_debug(val: bool):
    if val:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)


# Handle exceptions without UI failure
def gracefully_fail(exc: Exception):
    print("gracefully failing...")
    logger.exception(exc)
    app.storage.user["busy"] = False


async def stream_consumer(stream: str, topic: str):
    app.storage.user["busy"] = True

    topic_path = f"{stream}:{topic}"

    result = []

    try:
        # Create consumer instance
        response = await run.io_bound(
            restrunner.kafkapost,
            host=app.storage.general["hq"],
            path=f"/consumers/{topic}_cg",
            data={
                "name": f"{topic}_ci",
                "format": "json",
                "auto.offset.reset": "earliest",
                # "fetch.max.wait.ms": 1000,
                # "consumer.request.timeout.ms": "500",
            },
        )

        if response is None:
            return result

        ci = response.json()
        ci_path = urlparse(ci["base_uri"]).path

        # subscribe to consumer
        await run.io_bound(
            restrunner.kafkapost,
            host=app.storage.general["hq"],
            path=f"{ci_path}/subscription",
            data={"topics": [topic_path]},
        )
        # No content in response

        # get records
        records = await run.io_bound(
            restrunner.kafkaget,
            host=app.storage.general["hq"],
            path=f"{ci_path}/records",
        )
        if records and records.ok:
            for message in records.json():
                # logger.debug("CONSUMER GOT MESSAGE: %s", message)
                result.append(message["value"])

    except Exception as error:
        logger.warning("STREAM CONSUMER ERROR %s", error)

    finally:
        # Unsubscribe from consumer instance
        await run.io_bound(
            restrunner.kafkadelete,
            host=app.storage.general["hq"],
            path=f"/consumers/{topic}_cg/instances/{topic}_ci",
        )

        app.storage.user["busy"] = False

        return result
