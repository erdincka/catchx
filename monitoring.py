import datetime
import json
import socket
import httpx
from nicegui import ui, app
import sqlalchemy

from common import *
import iceberger
import streams
import tables

logger = logging.getLogger("monitoring")


def get_echart():
    return ui.echart(
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
                "text": "", 
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


# async def chart_listener(chart: ui.echart, metric_generator, *args):
#     for metric in metric_generator(*args):
#         if metric:

#             chart.options["xAxis"]["data"].append(metric["time"])
#             chart.options["title"]["text"] = metric["name"].title()

#             for idx, serie in enumerate(metric["values"]):
#                 # add missing series
#                 if idx not in chart.options["series"]:
#                     chart.options["series"].append(new_series())

#                 chart_series = chart.options["series"][idx]

#                 for key in serie.keys():
#                     if not chart_series.get("name", None):
#                         chart_series["name"] = key
#                     # if name ends with (s), place it onto second yAxis
#                     if "(s)" in key:
#                         chart_series["yAxisIndex"] = 1

#                     chart_series["data"].append(int(serie[key]))

#             chart.run_chart_method('hideLoading')
#             chart.update()


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

    metric_host_fqdn = socket.getfqdn(app.storage.general['cluster'])

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

    if app.storage.general.get("cluster", None) is None:
        logger.warning("Cluster not configured, skipping.")
        return

    try:
        URL = f"https://{app.storage.general['cluster']}:8443/rest/stream/topic/info?path={stream_path}&topic={topic}"
        auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])

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
                    app.storage.general["in_txn_pushed"] = m["maxoffset"] + 1
                    app.storage.general["in_txn_pulled"] = m["minoffsetacrossconsumers"]

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

    if app.storage.general.get("cluster", None) is None:
        logger.warning("Cluster not configured, skipping.")
        return

    try:
        URL = f"https://{app.storage.general['cluster']}:8443/rest/stream/cursor/list?path={stream_path}&topic={topic}"
        auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])
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
    if app.storage.general.get("cluster", None) is None:
        logger.warning("Cluster not configured, skipping.")
        return

    series = []

    ttable = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}"
    binarytable = f"{BASEDIR}/{VOLUME_BRONZE}/b{TABLE_TRANSACTIONS}"
    ctable = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_CUSTOMERS}"

    try:
        tick = timeit.default_timer()

        if os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{ttable}"):
            num_transactions = len(tables.get_documents(ttable, limit=None))
            series.append({ "transactions": num_transactions })
            app.storage.general["brnz_transactions"] = num_transactions
        else:
            app.storage.general["brnz_transactions"] = 0

        # FIX: binary table counters are not usable, might need to query the table for total distinct records
        # if os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{binarytable}"):
        #     URL = f"https://{app.storage.general['cluster']}:8443/rest/table/info?path={binarytable}"
        #     auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])
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
        #                     app.storage.general["brnz_transactions"] += m["totalrows"]

        if os.path.isdir(f"{MOUNT_PATH}/{get_cluster_name()}{ctable}"): # isdir for iceberg tables
            num_customers = len(iceberger.find_all(VOLUME_BRONZE, TABLE_CUSTOMERS))
            series.append({ "customers": num_customers })
            app.storage.general["brnz_customers"] = num_customers

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
    if app.storage.general.get("cluster", None) is None:
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
            num_profiles = len(tables.get_documents(ptable, limit=None))
            # logger.debug("Got metrics for silver profiles %d", num_profiles)
            series.append({ "profiles": num_profiles })
            app.storage.general["slvr_profiles"] = num_profiles
        else:
            logger.warning("Cannot get metric for silver profiles")

        if os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{ttable}"):
            num_transactions = len(tables.get_documents(ttable, limit=None))
            series.append({ "transactions": num_transactions })
            app.storage.general["slvr_transactions"] = num_transactions
        if os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{ctable}"):
            num_customers = len(tables.get_documents(ctable, limit=None))
            series.append({ "customers": num_customers })
            app.storage.general["slvr_customers"] = num_customers

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
    if app.storage.general.get("cluster", None) is None:
        logger.warning("Cluster not configured, skipping.")
        return

    series = []

    try:
        mydb = f"mysql+pymysql://{app.storage.general['MYSQL_USER']}:{app.storage.general['MYSQL_PASS']}@{app.storage.general['cluster']}/{DATA_PRODUCT}"

        # return if table is missing
        engine = create_engine(mydb)
        with engine.connect() as conn:
            # logger.debug("mysql connected")
            tick = timeit.default_timer()

            if sqlalchemy.inspect(engine).has_table(TABLE_TRANSACTIONS):
                num_transactions = conn.execute(text(f"SELECT COUNT('_id') FROM {TABLE_TRANSACTIONS}")).scalar()
                series.append({ TABLE_TRANSACTIONS: num_transactions })
                app.storage.general["gold_transactions"] = num_transactions

            if sqlalchemy.inspect(engine).has_table(TABLE_CUSTOMERS):
                num_customers = conn.execute(text(f"SELECT COUNT('_id') FROM {TABLE_CUSTOMERS}")).scalar()
                series.append({ TABLE_CUSTOMERS: num_customers })
                app.storage.general["gold_customers"] = num_customers

            if sqlalchemy.inspect(engine).has_table(TABLE_FRAUD):
                num_fraud = conn.execute(text(f"SELECT COUNT('_id') FROM {TABLE_FRAUD}")).scalar()
                series.append({ TABLE_FRAUD: num_fraud })
                app.storage.general["gold_fraud"] = num_fraud

            logger.debug("Gold stat time: %f", timeit.default_timer() - tick)

            # Don't update metrics for empty results
            if len(series) == 0: return

    except Exception as error:
        logger.warning("STAT got error %s", error)
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

        app.storage.general["in_txn_pushed"] = next(
            iter([m["publishedMsgs"] for m in metrics if "publishedMsgs" in m]), None
        )

        app.storage.general["in_txn_pulled"] = next(
            iter([m["consumedMsgs"] for m in metrics if "consumedMsgs" in m]), None
        )

    bronze = await bronze_stats()
    # logger.debug(bronze)

    if bronze is not None:
        metrics = bronze["values"]
        app.storage.general["brnz_transactions"] = next(
            iter([m["transactions"] for m in metrics if "transactions" in m]), None
        )
        app.storage.general["brnz_customers"] = next(
            iter([m["customers"] for m in metrics if "customers" in m]), None
        )

    silver = await silver_stats()
    # logger.debug(silver)

    if silver is not None:
        metrics = silver["values"]
        app.storage.general["slvr_profiles"] = next(
            iter([m["profiles"] for m in metrics if "profiles" in m]), None
        )
        app.storage.general["slvr_transactions"] = next(
            iter([m["transactions"] for m in metrics if "transactions" in m]), None
        )
        app.storage.general["slvr_customers"] = next(
            iter([m["customers"] for m in metrics if "customers" in m]), None
        )

    gold = await gold_stats()
    # logger.debug(gold)

    if gold is not None:
        metrics = gold["values"]
        app.storage.general["gold_fraud"] = next(
            iter([m[TABLE_FRAUD] for m in metrics if TABLE_FRAUD in m]), None
        )
        app.storage.general["gold_transactions"] = next(
            iter([m[TABLE_TRANSACTIONS] for m in metrics if TABLE_TRANSACTIONS in m]),
            None,
        )
        app.storage.general["gold_customers"] = next(
            iter([m[TABLE_CUSTOMERS] for m in metrics if TABLE_CUSTOMERS in m]), None
        )


def switch_monitoring(value, timers):
    for timer in timers:
        if value: timer.activate()
        else: timer.deactivate()


async def monitoring_charts():

    # Monitoring charts
    timers = []

    with ui.card().classes("w-full flex-grow shrink no-wrap bottom-0"):

        with ui.row().classes("w-full"):
            ui.label("Realtime Charts").classes("uppercase")

        with ui.row().classes("w-full place-content-stretch no-wrap"):
            # # monitor using /var/mapr/mapr.monitoring/metricstreams/0
            # streams_chart = get_echart()
            # streams_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
            # ui.timer(MON_REFRESH_INTERVAL, lambda c=streams_chart, s=mapr_monitoring: chart_listener(c, s), once=True)

            # TODO: embed grafana dashboard
            # https://10.1.1.31:3000/d/pUfMqVUIz/demo-monitoring?orgId=1

            consumer_chart = get_echart()
            consumer_chart.options["title"]["text"] = "Consumers"
            consumer_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}')

            incoming_chart = get_echart().classes("")
            incoming_chart.options["title"]["text"] = "Incoming"
            incoming_chart.run_chart_method(":showLoading", r'{text: "Waiting..."}')

            bronze_chart = get_echart().classes("")
            bronze_chart.options["title"]["text"] = "Bronze tier"
            bronze_chart.run_chart_method(":showLoading", r'{text: "Waiting..."}')

            silver_chart = get_echart().classes("")
            silver_chart.options["title"]["text"] = "Silver tier"
            silver_chart.run_chart_method(":showLoading", r'{text: "Waiting..."}')

            gold_chart = get_echart().classes("")
            gold_chart.options["title"]["text"] = "Gold tier"
            gold_chart.run_chart_method(":showLoading", r'{text: "Waiting..."}')

            timers.append(ui.timer(MON_REFRESH_INTERVAL5 + 3, lambda c=consumer_chart: update_chart(c, txn_consumer_stats), active=False))

            timers.append(ui.timer(MON_REFRESH_INTERVAL3, lambda c=incoming_chart: update_chart(c, incoming_topic_stats), active=False))

            timers.append(ui.timer(MON_REFRESH_INTERVAL5, lambda c=bronze_chart: update_chart(c, bronze_stats), active=False))

            timers.append(ui.timer(MON_REFRESH_INTERVAL5 + 1, lambda c=silver_chart: update_chart(c, silver_stats), active=False))

            timers.append(ui.timer(MON_REFRESH_INTERVAL5 + 2, lambda c=gold_chart: update_chart(c, gold_stats), active=False))

    return timers

async def update_metrics(chart: ui.chart):

    # # monitor using /var/mapr/mapr.monitoring/metricstreams/0
    # streams_chart = get_echart()
    # streams_chart.run_chart_method(':showLoading', r'{text: "Waiting..."}',)
    # ui.timer(MON_REFRESH_INTERVAL, lambda c=streams_chart, s=mapr_monitoring: chart_listener(c, s), once=True)

    tick = timeit.default_timer()

    values = []
    # transform multiple series to single one replacing key with metric_key
    metrics = await incoming_topic_stats()
    if metrics is not None:
        metric_time = metrics["time"]
        # definitely not readable and confusing. TODO: extract to a extract and flatten function
        values += [item for row in [ [ { metrics["name"] + "_" + k: v } for k,v in m.items() ] for m in metrics["values"] ] for item in row]

    metrics = await bronze_stats()
    if metrics is not None:
        metric_time = metrics["time"]
        values += [item for row in [ [ { metrics["name"] + "_" + k: v } for k,v in m.items() ] for m in metrics["values"] ] for item in row]

    metrics = await silver_stats()
    if metrics is not None:
        metric_time = metrics["time"]
        values += [item for row in [ [ { metrics["name"] + "_" + k: v } for k,v in m.items() ] for m in metrics["values"] ] for item in row]

    metrics = await gold_stats()
    if metrics is not None:
        metric_time = metrics["time"]
        values += [item for row in [ [ { metrics["name"] + "_" + k: v } for k,v in m.items() ] for m in metrics["values"] ] for item in row]

    logger.debug(values)

    if len(values) > 0:
        chart.options["xAxis"]["data"].append(metric_time)  # taking the time from the last (gold) metric
        for idx, serie in enumerate(values):
            # add missing series
            if idx not in chart.options["series"]:
                chart.options["series"].append(new_series())

            chart_series = chart.options["series"][idx]
            for key in serie.keys():
                if not chart_series.get("name", None):
                    chart_series["name"] = key

                chart_series["data"].append(int(serie[key]))

        chart.update()
        chart.run_chart_method('hideLoading')

    logger.info("Finished in: %ss", timeit.default_timer() - tick)


def monitoring_card():
    # Realtime monitoring information
    with ui.card().classes(
        "flex-grow shrink absolute top-10 right-0 w-1/4 h-fit opacity-50 hover:opacity-100"
    ).bind_visibility_from(app.storage.general, 'demo_mode').props("flat") as monitoring_card:
        ui.label("Realtime Visibility").classes("uppercase")
        with ui.grid(columns=2).classes("w-full"):
            for metric in [
                "in_txn_pushed",
                "in_txn_pulled",
                "brnz_customers",
                "brnz_transactions",
                "slvr_profiles",
                "slvr_transactions",
                "slvr_customers",
                "gold_transactions",
                "gold_customers",
                "gold_fraud",
            ]:
                with ui.row().classes("w-full place-content-between"):
                    ui.label(metric).classes("text-xs m-0 p-0")
                    ui.badge().bind_text_from(
                        app.storage.general, metric
                    ).props("color=red align=middle").classes(
                        "size-xs self-end"
                    )

    return monitoring_card


def logging_card():
    # Realtime logging
    with ui.card().classes(
        "flex-grow shrink absolute top-64 right-0 w-1/4 opacity-50 hover:opacity-100"
    ).bind_visibility_from(app.storage.general, 'demo_mode') as logging_card:
        ui.label("App log").classes("uppercase")
        log = ui.log().classes("h-40")
        handler = LogElementHandler(log, logging.INFO)
        rootLogger = logging.getLogger()
        rootLogger.addHandler(handler)
        ui.context.client.on_disconnect(lambda: rootLogger.removeHandler(handler))
