import datetime
import socket
import httpx
from nicegui import ui, app

from helpers import *
import iceberger
import streams

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
    for metric in await metric_generator(*args):
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
    metric = await metric_caller(*args)
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


async def stream_stats():
    stream_path = "/var/mapr/mapr.monitoring/metricstreams/0"

    for record in await streams.consume(stream=stream_path, topic=socket.getfqdn(app.storage.general['cluster'])):
        metric = json.loads(record)
        series = []
        if metric[0]["metric"] in ["mapr.streams.produce_msgs", "mapr.streams.listen_msgs"]:
            # mapr.db.table.write_rows and mapr.db.table.read_rows for table
            series.append(
                { metric[0]["metric"]: metric[0]["value"] }
            )
        yield {
            "name": metric[0]["tags"]["clustername"],
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "values": series,
        }


async def topic_stats():
    stream_path = f"{DEMO['basedir']}/{DEMO['stream']}"
    topic = DEMO["topic"]

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
                    logger.debug("TOPIC STAT %s", metrics)

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
                        series.append(
                            {
                                "consumerLag(s)": (
                                    dt_from_iso(m["maxtimestamp"])
                                    - dt_from_iso(m["mintimestampacrossconsumers"])
                                ).total_seconds()
                            }
                        )
                    # logger.info("Metrics %s", series)
                    return {
                        "name": topic,
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


async def consumer_stats():
    stream_path = f"{DEMO['basedir']}/{DEMO['stream']}"
    topic = DEMO["topic"]

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
                    logger.debug("CONSUMER STAT %s", metrics)
                    series = []
                    for m in metrics["data"]:
                        series.append(
                            {
                                f"{m['consumergroup']}_{m['partitionid']}_lag(s)": float(
                                    m["consumerlagmillis"]
                                )
                                / 1000
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


async def table_stats(tier: str):
    if app.storage.general.get("cluster", None) is None:
        logger.debug("Cluster not configured, skipping.")
        return
    
    try:
        ### get # of records in iceberg table "customers"
        ### get # of records in iceberg table "transactions"
        ### get # of records in json table "profiles"
        metrics = iceberger.stats(tier)
        print(metrics)
        logger.debug("BRONZE STAT %s", metrics)

        series = []
        series.append(metrics)
        # for m in metrics:
        #     series.append(
        #         {
        #             f"{m['name']}": float(m['value'])
        #         }
        #     )

        return {
            "name": tier.capitalize(),
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "values": series,
        }

    except Exception as error:
        logger.debug("%s stat get error %s", tier, error)

