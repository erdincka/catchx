import datetime
import json
import socket
import httpx
from nicegui import ui, app

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
            },
            "title": {"left": 10, "text": ""},
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


async def chart_listener(chart: ui.echart, metric_generator, *args):
    for metric in metric_generator(*args):
        if metric:

            chart.options["xAxis"]["data"].append(metric["time"])
            chart.options["title"]["text"] = metric["name"].title()

            for idx, serie in enumerate(metric["values"]):
                # add missing series
                if idx not in chart.options["series"]:
                    chart.options["series"].append(new_series())

                chart_series = chart.options["series"][idx]

                for key in serie.keys():
                    if not chart_series.get("name", None):
                        chart_series["name"] = key
                    # if name ends with (s), place it onto second yAxis
                    if "(s)" in key:
                        chart_series["yAxisIndex"] = 1

                    chart_series["data"].append(int(serie[key]))

            chart.run_chart_method('hideLoading')
            chart.update()


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
        logger.debug("Got metric from %s: %s", metric_caller.__name__, metric)

        chart.options["xAxis"]["data"].append(metric["time"])
        chart.options["title"]["text"] = metric["name"].title()

        for idx, serie in enumerate(metric["values"]):
            # add missing series
            if idx not in chart.options["series"]:
                chart.options["series"].append(new_series())

            chart_series = chart.options["series"][idx]
            for key in serie.keys():
                if not chart_series.get("name", None):
                    chart_series["name"] = key
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
            logger.debug("Found metric %s", metric[0])

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
        logger.debug("Cluster not configured, skipping.")
        return
    
    try:
        URL = f"https://{app.storage.general['cluster']}:8443/rest/stream/topic/info?path={stream_path}&topic={topic}"
        auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])
        async with httpx.AsyncClient(verify=False) as client:  # using async httpx instead of sync requests to avoid blocking the event loop 
            response = await client.get(URL, auth=auth, timeout=2.0)

            if response is None or response.status_code != 200:
                # possibly not connected or topic not populated yet, just ignore
                logger.debug(f"Failed to get topic stats for {topic}")

            else:
                metrics = response.json()
                if not metrics["status"] == "ERROR":
                    logger.debug(metrics)

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
                    return {
                        "name": "Incoming",
                        "time": datetime.datetime.fromtimestamp(
                            metrics["timestamp"] / (10**3)
                        ).strftime("%H:%M:%S"),
                        "values": series,
                    }
                else:
                    # possibly topic is not created yet
                    logger.debug("Topic stat query error %s", metrics["errors"])

    except Exception as error:
        logger.warning("Topic stat request error %s", error)
        await asyncio.sleep(30)


async def txn_consumer_stats():
    stream_path = f"{BASEDIR}/{STREAM_INCOMING}"
    topic = TOPIC_TRANSACTIONS

    if app.storage.general.get("cluster", None) is None:
        logger.debug("Cluster not configured, skipping.")
        return
    
    try:
        URL = f"https://{app.storage.general['cluster']}:8443/rest/stream/cursor/list?path={stream_path}&topic={topic}"
        auth = (app.storage.general["MAPR_USER"], app.storage.general["MAPR_PASS"])
        async with httpx.AsyncClient(verify=False) as client:
            response = await client.get(URL, auth=auth, timeout=2.0)

            if response is None or response.status_code != 200:
                # possibly not connected or topic not populated yet, just ignore
                logger.debug(f"Failed to get consumer stats for {topic}")

            else:
                metrics = response.json()
                logger.debug(metrics)

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
                    logger.debug("Consumer stat query error %s", metrics["errors"])

    except Exception as error:
        # possibly not connected or topic not populated yet, just ignore it
        logger.debug("Consumer stat request error %s", error)
        # delay for a while before retry
        await asyncio.sleep(30)


async def bronze_stats():
    if app.storage.general.get("cluster", None) is None:
        logger.debug("Cluster not configured, skipping.")
        return

    series = []
    
    ttable = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}"
    ctable = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_CUSTOMERS}"

    try:
        if os.path.lexists(f"{MOUNT_PATH}{get_cluster_name()}{ttable}"):
            series.append({ "transactions": len(tables.get_documents(ttable, limit=None)) })
        if os.path.isdir(f"{MOUNT_PATH}{get_cluster_name()}{ctable}"): # isdir for iceberg tables
            series.append({ "customers": len(iceberger.find_all(VOLUME_BRONZE, TABLE_CUSTOMERS)) })

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
        logger.debug("Cluster not configured, skipping.")
        return

    series = []

    ptable = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_PROFILES}"
    ttable = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_TRANSACTIONS}"
    ctable = f"{BASEDIR}/{VOLUME_SILVER}/{TABLE_CUSTOMERS}"

    try:
        if os.path.lexists(f"{MOUNT_PATH}{get_cluster_name()}{ptable}"):
            series.append({ "profiles": len(tables.get_documents(ptable, limit=None)) })
        if os.path.lexists(f"{MOUNT_PATH}{get_cluster_name()}{ttable}"):
            series.append({ "transactions": len(tables.get_documents(ttable, limit=None)) })
        if os.path.lexists(f"{MOUNT_PATH}{get_cluster_name()}{ctable}"):
            series.append({ "customers": len(tables.get_documents(ctable, limit=None)) })

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
        logger.debug("Cluster not configured, skipping.")
        return

    series = []
    
    try:
        mydb = f"mysql+pymysql://{app.storage.general['MYSQL_USER']}:{app.storage.general['MYSQL_PASS']}@{app.storage.general['cluster']}/{DATA_PRODUCT}"

        # return if table is missing
        engine = create_engine(mydb)
        with engine.connect() as conn:
            # return if table is not created
            if TABLE_FRAUD not in [ t for t in conn.execute(text(f"SHOW TABLES LIKE '{TABLE_FRAUD}';")) ]: return
                
        # TODO: find a better/more efficient way to count records
        series.append({ TABLE_FRAUD: pd.read_sql(f"SELECT COUNT('_id') FROM {TABLE_FRAUD}", con=mydb).values[:1].flat[0] })
        series.append({ TABLE_TRANSACTIONS: pd.read_sql(f"SELECT COUNT('_id') FROM {TABLE_TRANSACTIONS}", con=mydb).values[:1].flat[0] })
        series.append({ TABLE_CUSTOMERS: pd.read_sql(f"SELECT COUNT('_id') FROM {TABLE_CUSTOMERS}", con=mydb).values[:1].flat[0] })

        # Don't update metrics for empty results
        if len(series) == 0: return

    except Exception as error:
        logger.warning("STAT get error %s", error)
        return
    
    return {
        "name": VOLUME_GOLD,
        "time": datetime.datetime.now().strftime("%H:%M:%S"),
        "values": series,
    }


