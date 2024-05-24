import asyncio
from nicegui import app, ui

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
            "series": [  # manually set max series to display (TODO: find a pythonic way)
                {
                    "type": "line",
                    "smooth": True,
                    "data": [],
                },
                {
                    "type": "line",
                    "smooth": True,
                    "data": [],
                },
                {
                    "type": "line",
                    "smooth": True,
                    "data": [],
                },
                {
                    "type": "line",
                    "smooth": True,
                    "data": [],
                },
            ],
        },
    )


def add_measurement(metric, chart):
    if metric:
        chart.options["xAxis"]["data"].append(metric["time"])
        chart.options["title"]["text"] = metric["name"].title()

        for idx, serie in enumerate(metric["values"]):
            chart_series = chart.options["series"][idx]
            for key in serie.keys():
                if not chart_series.get("name", None):
                    chart_series["name"] = key
                # if name ends with (s), place it onto second yAxis
                if "(s)" in key:
                    chart_series["yAxisIndex"] = 1
                chart_series["data"].append(int(serie[key]))
        chart.update()

