import datetime
import json
import socket
import timeit
import httpx
from nicegui import ui, app
import pandas as pd
import sqlalchemy

from common import *
import iceberger
import streams
import tables

logger = logging.getLogger("monitoring")

MONITORING_CHARTS = None
MONITORING_TIMERS = []

def new_echart(title: str):
    chart = ui.echart(
        {
            "tooltip": {
                "trigger": "axis",
                # 'position': ["20%","80%"],
                "axisPointer": {
                    "type": "shadow",
                },
            },
            "title": {
                "left": 4,
                "text": title,
                "textStyle": {
                    "fontSize": 12
                }
            },
            # "legend": {"right": "center"},
            "xAxis": {
                "type": "category",
                "boundaryGap": False,
                "axisLine": {"onZero": True},
                "data": [],
            },
            "yAxis": [
                {
                    "type": "value",
                    "name": "Count",
                    "boundaryGap": [0, "100%"],
                    "splitLine": { "show": False }
                },
                {
                    "type": "value",
                    "name": "Seconds",
                    "axisLabel": {
                        "formatter": "{value} s",
                    },
                    "boundaryGap": [0, "100%"],
                    "splitLine": { "show": False }
                },
            ],
            "series": [],
        },
    )

    chart.run_chart_method(':showLoading', r'{text: "Waiting..."}')

    return chart


async def update_chart(chart: ui.echart, metric_caller, *args):
    """
    Update the chart by running the caller function

    :param chart ui.Chart: chart to add metrics to, it must be initialised
    :param metric_caller function: coroutine that will get the metrics
    :param args any: passed to caller function

    :returns None
    """

    metric = await metric_caller(*args)

    if metric:
        # logger.debug("Got metric from %s: %s", metric_caller.__name__, metric)

        chart.options["xAxis"]["data"].append(metric["time"])
        # chart.options["title"]["text"] = metric["name"].title()

        for idx, serie in enumerate(metric["values"]):
            # add missing series
            if idx not in chart.options["series"]:
                chart.options["series"].append(new_series())

            chart_series = chart.options["series"][idx]
            for key in serie.keys():
                if not chart_series.get("name", None):
                    chart_series["name"] = f"{metric['name'].title()}-{key}"
                # if name ends with (s), place it onto second yAxis
                if "(s)" in key:
                    chart_series["yAxisIndex"] = 1

                chart_series["data"].append(int(serie[key]))

        chart.run_chart_method('hideLoading')
        chart.update()


def new_series():
    return {
        "type": "line",
        "showSymbol": False,
        "smooth": True,
        "data": [],
        "emphasis": {
            "focus": 'series'
        },
    }


def mapr_monitoring():
    stream_path = "/var/mapr/mapr.monitoring/metricstreams/0"

    metric_host_fqdn = socket.getfqdn(app.storage.user['MAPR_HOST'])

    for record in streams.consume(stream=stream_path, topic=metric_host_fqdn, consumer_group="monitoring"):
        metric = json.loads(record)

        series = []
        if metric[0]["metric"] in ["mapr.streams.produce_msgs", "mapr.streams.listen_msgs", "mapr.db.table.write_rows", "mapr.db.table.read_rows"]:
            logger.info("Found metric %s", metric[0])

            series.append(
                { metric[0]["metric"]: metric[0]["value"] }
            )
        yield {
            "name": metric[0]["tags"]["clustername"],
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "values": series,
        }


async def incoming_topic_stats():
    stream_path = f"{BASEDIR}/{STREAM_INCOMING}"
    topic = TOPIC_TRANSACTIONS

    if app.storage.user.get("MAPR_HOST", None) is None:
        logger.warning("Cluster not configured, skipping.")
        return

    try:
        URL = f"https://{app.storage.user['MAPR_HOST']}:8443/rest/stream/topic/info?path={stream_path}&topic={topic}"
        auth = (app.storage.user["MAPR_USER"], app.storage.user["MAPR_PASS"])

        async with httpx.AsyncClient(verify=False) as client:  # using async httpx instead of sync requests to avoid blocking the event loop
            response = await client.get(URL, auth=auth, timeout=2.0)

            if response is None or response.status_code != 200:
                # possibly not connected or topic not populated yet, just ignore
                logger.warning(f"Failed to get topic stats for {topic}")

            else:
                metrics = response.json()
                if not metrics["status"] == "ERROR":
                    # logger.debug(metrics)

                    series = []
                    for m in metrics["data"]:
                        series.append(
                            {"publishedMsgs": m["maxoffset"] + 1}
                        )  # interestingly, maxoffset starts from -1
                        series.append(
                            {"consumedMsgs": m["minoffsetacrossconsumers"]}
                        )  # this metric starts at 0
                        # series.append(
                        #     {
                        #         "latestAgo(s)": (
                        #             datetime.datetime.now().astimezone()
                        #             - dt_from_iso(m["maxtimestamp"])
                        #         ).total_seconds()
                        #     }
                        # )
                        # series.append(
                        #     {
                        #         "consumerLag(s)": (
                        #             dt_from_iso(m["maxtimestamp"])
                        #             - dt_from_iso(m["mintimestampacrossconsumers"])
                        #         ).total_seconds()
                        #     }
                        # )
                    # logger.info("Metrics %s", series)
                    # update counter
                    app.storage.user["transactions_ingested"] = m["maxoffset"] + 1
                    app.storage.user["transactions_processed"] = m["minoffsetacrossconsumers"]

                    return {
                        "name": "Incoming",
                        "time": datetime.datetime.fromtimestamp(
                            metrics["timestamp"] / (10**3)
                        ).strftime("%H:%M:%S"),
                        "values": series,
                    }
                else:
                    # possibly topic is not created yet
                    logger.warning("Topic stat query error %s", metrics["errors"])

    except Exception as error:
        logger.warning("Topic stat request error %s", error)
        # delayed query if failed - possibly cluster is not accessible
        await asyncio.sleep(30)


async def txn_consumer_stats():
    stream_path = f"{BASEDIR}/{STREAM_INCOMING}"
    topic = TOPIC_TRANSACTIONS

    if app.storage.user.get("MAPR_HOST", None) is None:
        logger.warning("Cluster not configured, skipping.")
        return

    try:
        URL = f"https://{app.storage.user['MAPR_HOST']}:8443/rest/stream/cursor/list?path={stream_path}&topic={topic}"
        auth = (app.storage.user["MAPR_USER"], app.storage.user["MAPR_PASS"])
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(URL, auth=auth, timeout=2.0)

            if response is None or response.status_code != 200:
                # possibly not connected or topic not populated yet, just ignore
                logger.warning(f"Failed to get consumer stats for {topic}")

            else:
                metrics = response.json()
                # logger.debug(metrics)

                if not metrics["status"] == "ERROR":
                    series = []
                    for m in metrics["data"]:
                        series.append(
                            {
                                f"{m['consumergroup']}_{m['partitionid']}_lag(s)": float(
                                    m["consumerlagmillis"]
                                )
                                / 1000 # convert millis to seconds
                            }
                        )
                        series.append(
                            {
                                f"{m['consumergroup']}_{m['partitionid']}_offsetBehind": int(
                                    m["produceroffset"]
                                ) + 1 # starting from -1 !!!
                                - int(m["committedoffset"])
                            }
                        )
                    # logger.info("Metrics %s", series)
                    return {
                        "name": "Consumers",
                        "time": datetime.datetime.fromtimestamp(
                            metrics["timestamp"] / (10**3)
                        ).strftime("%H:%M:%S"),
                        "values": series,
                    }
                else:
                    # possibly topic is not created yet
                    logger.warning("Consumer stat query error %s", metrics["errors"])

    except Exception as error:
        # possibly not connected or topic not populated yet, just ignore it
        logger.warning("Consumer stat request error %s", error)
        # delay for a while before retry
        await asyncio.sleep(30)


async def bronze_stats():
    if app.storage.user.get("MAPR_HOST", None) is None:
        logger.warning("Cluster not configured, skipping.")
        return

    series = []

    ttable = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}"
    binarytable = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}-binary"
    ctable = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_CUSTOMERS}"

    try:
        tick = timeit.default_timer()

        if os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{ttable}"):
            num_transactions = len(await tables.get_documents(ttable, limit=None))
            series.append({ "transactions": num_transactions })
            app.storage.user["bronze_transactions"] = num_transactions
        else:
            app.storage.user["bronze_transactions"] = 0

        # FIX: binary table counters are not usable, might need to query the table for total distinct records
        # if os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{binarytable}"):
        #     URL = f"https://{app.storage.user['MAPR_HOST']}:8443/rest/table/info?path={binarytable}"
        #     auth = (app.storage.user["MAPR_USER"], app.storage.user["MAPR_PASS"])
        #     async with httpx.AsyncClient(verify=False) as client:
        #         response = await client.get(URL, auth=auth, timeout=2.0)

        #         if response is None or response.status_code != 200:
        #             # possibly not connected or topic not populated yet, just ignore
        #             logger.debug(f"Failed to get consumer stats for {binarytable}")

        #         else:
        #             metrics = response.json()
        #             # logger.debug(f"metrics for {binarytable}: {metrics}")
        #             if not metrics["status"] == "ERROR":
        #                 # logger.debug(metrics)
        #                 for m in metrics["data"]:
        #                     app.storage.user["bronze_transactions"] += m["totalrows"]

        if os.path.isdir(f"{MOUNT_PATH}/{get_cluster_name()}{ctable}"): # isdir for iceberg tables
            num_customers = len(iceberger.find_all(VOLUME_BRONZE, TABLE_CUSTOMERS))
            series.append({ "customers": num_customers })
            app.storage.user["bronze_customers"] = num_customers

        logger.debug("Bronze stat time: %f", timeit.default_timer() - tick)

        # Don't update metrics for empty results
        if len(series) == 0: return

    except Exception as error:
        logger.warning("STAT get error %s", error)
        return

    return {
        "name": VOLUME_BRONZE,
        "time": datetime.datetime.now().strftime("%H:%M:%S"),
        "values": series,
    }


async def silver_stats():
    if app.storage.user.get("MAPR_HOST", None) is None:
        logger.warning("Cluster not configured, skipping.")
        return

    series = []

    ptable = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}"
    ttable = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}"
    ctable = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}"

    try:
        # logger.info("Searching table %s", f"{MOUNT_PATH}/{get_cluster_name()}{ptable}")

        tick = timeit.default_timer()

        if os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{ptable}"):
            # logger.info("Found table %s", ptable)
            num_profiles = len(await tables.get_documents(ptable, limit=None))
            # logger.debug("Got metrics for silver profiles %d", num_profiles)
            series.append({ "profiles": num_profiles })
            app.storage.user["silver_profiles"] = num_profiles
        else:
            logger.warning("Cannot get metric for silver profiles")

        if os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{ttable}"):
            num_transactions = len(await tables.get_documents(ttable, limit=None))
            series.append({ "transactions": num_transactions })
            app.storage.user["silver_transactions"] = num_transactions
        if os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{ctable}"):
            num_customers = len(await tables.get_documents(ctable, limit=None))
            series.append({ "customers": num_customers })
            app.storage.user["silver_customers"] = num_customers

        logger.debug("Silver stat time: %f", timeit.default_timer() - tick)
        # Don't update metrics for empty results
        if len(series) == 0: return

    except Exception as error:
        logger.warning("STAT get error %s", error)
        return

    return {
        "name": VOLUME_SILVER,
        "time": datetime.datetime.now().strftime("%H:%M:%S"),
        "values": series,
    }


async def gold_stats():
    if app.storage.user.get("MAPR_HOST", None) is None:
        logger.warning("Cluster not configured, skipping.")
        return

    series = []

    try:
        tick = timeit.default_timer()

        ctable = f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_CUSTOMERS}"
        ttable = f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}"

        customers_df = await tables.delta_table_get(ctable, None)

        series.append({ TABLE_CUSTOMERS: customers_df.shape[0] })
        app.storage.user["gold_customers"] = customers_df.shape[0]

        transactions_df = await tables.delta_table_get(ttable, None)
        series.append({ TABLE_TRANSACTIONS: transactions_df.shape[0] })
        app.storage.user["gold_transactions"] = transactions_df.shape[0]

        fraud_transactions_df = transactions_df[transactions_df['fraud'] == True] # or pd.DataFrame() # empty dataframe if none found

        series.append({ TABLE_FRAUD: fraud_transactions_df.shape[0] })
        app.storage.user["gold_fraud"] = fraud_transactions_df.shape[0]

        logger.debug("Gold stat time: %f", timeit.default_timer() - tick)

        if len(series) == 0: return

    except Exception as error:
        logger.warning("STAT got error: %s", error)
        return

    return {
        "name": VOLUME_GOLD,
        "time": datetime.datetime.now().strftime("%H:%M:%S"),
        "values": series,
    }


async def monitoring_metrics():
    """
    Capture metrics for monitoring/dashboard
    """

    incoming = await incoming_topic_stats()
    # logger.debug(incoming)

    if incoming is not None:
        metrics = incoming["values"]

        logger.info("incoming: %s", metrics)

        app.storage.user["transactions_ingested"] = next(
            iter([m["publishedMsgs"] for m in metrics if "publishedMsgs" in m]), None
        )

        app.storage.user["transactions_processed"] = next(
            iter([m["consumedMsgs"] for m in metrics if "consumedMsgs" in m]), None
        )

    bronze = await bronze_stats()
    # logger.debug(bronze)

    if bronze is not None:
        metrics = bronze["values"]
        app.storage.user["bronze_transactions"] = next(
            iter([m["transactions"] for m in metrics if "transactions" in m]), None
        )
        app.storage.user["bronze_customers"] = next(
            iter([m["customers"] for m in metrics if "customers" in m]), None
        )

    silver = await silver_stats()
    # logger.debug(silver)

    if silver is not None:
        metrics = silver["values"]
        app.storage.user["silver_profiles"] = next(
            iter([m["profiles"] for m in metrics if "profiles" in m]), None
        )
        app.storage.user["silver_transactions"] = next(
            iter([m["transactions"] for m in metrics if "transactions" in m]), None
        )
        app.storage.user["silver_customers"] = next(
            iter([m["customers"] for m in metrics if "customers" in m]), None
        )

    gold = await gold_stats()
    # logger.debug(gold)

    if gold is not None:
        metrics = gold["values"]
        app.storage.user["gold_fraud"] = next(
            iter([m[TABLE_FRAUD] for m in metrics if TABLE_FRAUD in m]), None
        )
        app.storage.user["gold_transactions"] = next(
            iter([m[TABLE_TRANSACTIONS] for m in metrics if TABLE_TRANSACTIONS in m]),
            None,
        )
        app.storage.user["gold_customers"] = next(
            iter([m[TABLE_CUSTOMERS] for m in metrics if TABLE_CUSTOMERS in m]), None
        )


def toggle_monitoring(value: bool):
    for timer in monitoring_timers():
        if value: timer.activate()
        else:
            timer.deactivate()


def monitoring_timers():

    """Set timers to refresh each chart"""

    global MONITORING_TIMERS

    # Singleton
    if len(MONITORING_TIMERS) == 0:

        MONITORING_TIMERS.append(ui.timer(MON_REFRESH_INTERVAL3, txn_consumer_stats, active=False))

        MONITORING_TIMERS.append(ui.timer(MON_REFRESH_INTERVAL3 + 1, incoming_topic_stats, active=False))

        MONITORING_TIMERS.append(ui.timer(MON_REFRESH_INTERVAL3 + 2, bronze_stats, active=False))

        MONITORING_TIMERS.append(ui.timer(MON_REFRESH_INTERVAL3 + 3, silver_stats, active=False))

        MONITORING_TIMERS.append(ui.timer(MON_REFRESH_INTERVAL3 + 4, gold_stats, active=False))

    return MONITORING_TIMERS


def monitoring_charts():
    """Return charts for realtime monitoring"""

    charts = {}

    # global MONITORING_TIMERS

    with ui.card().classes("w-full flex-grow shrink no-wrap bottom-0"):
        # with ui.row().classes("w-full place-content-stretch no-wrap"):
        with ui.grid(columns=2).classes("w-full place-content-stretch no-wrap"):
            # # monitor using /var/mapr/mapr.monitoring/metricstreams/0
            # streams_chart = get_echart()
            # streams_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
            # ui.timer(MON_REFRESH_INTERVAL, lambda c=streams_chart, s=mapr_monitoring: chart_listener(c, s), once=True)

            # TODO: embed grafana dashboard
            # https://10.1.1.31:3000/d/pUfMqVUIz/demo-monitoring?orgId=1

            charts["consumer"] = new_echart(title="Consumers")
            # MONITORING_TIMERS.append(ui.timer(MON_REFRESH_INTERVAL3, lambda c=charts["consumer"]: update_chart(c, txn_consumer_stats), active=True))

            charts["incoming"] = new_echart(title="Incoming")
            # MONITORING_TIMERS.append(ui.timer(MON_REFRESH_INTERVAL3 + 1, lambda c=charts["incoming"]: update_chart(c, incoming_topic_stats), active=True))

            charts["bronze"] = new_echart(title="Bronze tier")
            # MONITORING_TIMERS.append(ui.timer(MON_REFRESH_INTERVAL3 + 2, lambda c=charts["bronze"]: update_chart(c, bronze_stats), active=True))

            charts["silver"] = new_echart(title="Silver tier")
            # MONITORING_TIMERS.append(ui.timer(MON_REFRESH_INTERVAL3 + 3, lambda c=charts["silver"]: update_chart(c, silver_stats), active=True))

            charts["gold"] = new_echart(title="Gold tier")
            # MONITORING_TIMERS.append(ui.timer(MON_REFRESH_INTERVAL3 + 4, lambda c=charts["gold"]: update_chart(c, gold_stats), active=True))

    return charts


# NOT USED
# async def update_metrics(chart: ui.chart):

#     # # monitor using /var/mapr/mapr.monitoring/metricstreams/0
#     # streams_chart = get_echart()
#     # streams_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
#     # ui.timer(MON_REFRESH_INTERVAL, lambda c=streams_chart, s=mapr_monitoring: chart_listener(c, s), once=True)

#     tick = timeit.default_timer()

#     values = []
#     # transform multiple series to single one replacing key with metric_key
#     metrics = await incoming_topic_stats()
#     if metrics is not None:
#         metric_time = metrics["time"]
#         # definitely not readable and confusing. TODO: extract to a extract and flatten function
#         values += [item for row in [ [ { metrics["name"] + "_" + k: v } for k,v in m.items() ] for m in metrics["values"] ] for item in row]

#     metrics = await bronze_stats()
#     if metrics is not None:
#         metric_time = metrics["time"]
#         values += [item for row in [ [ { metrics["name"] + "_" + k: v } for k,v in m.items() ] for m in metrics["values"] ] for item in row]

#     metrics = await silver_stats()
#     if metrics is not None:
#         metric_time = metrics["time"]
#         values += [item for row in [ [ { metrics["name"] + "_" + k: v } for k,v in m.items() ] for m in metrics["values"] ] for item in row]

#     metrics = await gold_stats()
#     if metrics is not None:
#         metric_time = metrics["time"]
#         values += [item for row in [ [ { metrics["name"] + "_" + k: v } for k,v in m.items() ] for m in metrics["values"] ] for item in row]

#     logger.debug(values)

#     if len(values) > 0:
#         chart.options["xAxis"]["data"].append(metric_time)  # taking the time from the last (gold) metric
#         for idx, serie in enumerate(values):
#             # add missing series
#             if idx not in chart.options["series"]:
#                 chart.options["series"].append(new_series())

#             chart_series = chart.options["series"][idx]
#             for key in serie.keys():
#                 if not chart_series.get("name", None):
#                     chart_series["name"] = key

#                 chart_series["data"].append(int(serie[key]))

#         chart.update()
#         chart.run_chart_method('hideLoading')

#     logger.info("Finished in: %ss", timeit.default_timer() - tick)


def monitoring_card():
    with ui.card().bind_visibility_from(app.storage.user, 'demo_mode').props("flat") as monitoring_card:
        ui.label("Realtime Visibility").classes("uppercase")
        with ui.grid(columns=1).classes("w-full"):
            for metric in MONITORING_METRICS:
                with ui.row().classes("w-full place-content-between"):
                    ui.label(metric).classes("text-xs m-0 p-0")
                    ui.badge().bind_text_from(app.storage.user, metric
                    ).props("color=red align=middle").classes(
                        "size-xs self-end"
                    )

    return monitoring_card


def monitoring_ticker():
    with ui.row().bind_visibility_from(app.storage.user, 'demo_mode').props("flat") as monitoring_ticker:
        for metric in MONITORING_METRICS:
                ui.label(metric).classes("text-xs m-0 p-0")
                ui.badge().bind_text_from(app.storage.user, metric
                ).props("color=red align=middle").classes(
                    "size-xs self-end"
                )

    return monitoring_ticker


def logging_card():
    # Realtime logging
    with ui.card().bind_visibility_from(app.storage.user, 'demo_mode').props("flat") as logging_card:
        # ui.label("App log").classes("uppercase")
        log = ui.log().classes("h-24")
        handler = LogElementHandler(log, logging.INFO)
        rootLogger = logging.getLogger()
        rootLogger.addHandler(handler)
        ui.context.client.on_disconnect(lambda: rootLogger.removeHandler(handler))
        rootLogger.info("Logging started")

    return logging_card
