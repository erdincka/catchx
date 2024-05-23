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
                    "symbol": "triangle",
                    "smooth": True,
                    "data": [],
                },
                {
                    "type": "line",
                    "symbol": "roundRect",
                    "smooth": True,
                    "data": [],
                },
                {
                    "type": "line",
                    "symbol": "pin",
                    "smooth": True,
                    "data": [],
                },
            ],
        },
    )


def update_metrics(metric_name, metric_chart):
    timer = ui.timer(
        interval=3.0,
        # Using lambda below so we capture the function name for individual steps
        callback=lambda service=metric_name, chart=metric_chart: add_metric(
            service, chart
        ),
        active=True,
    )

    # ui.switch(
    #     " -> ".join(metric_name.split(".")[1:]).title().replace("_", " ")
    # ).bind_value(timer, "active")

    def add_metric(service_name, chart_name):
        t = service_name.split(".")
        func = getattr(__import__(t[0]), t[1])

        # collect the metrics
        metric = func(t[2])
        if metric:
            chart_name.options["xAxis"]["data"].append(metric["time"])
            chart_name.options["title"]["text"] = metric["name"].title()

            for idx, serie in enumerate(metric["values"]):
                chart_series = chart_name.options["series"][idx]
                for key in serie.keys():
                    if not chart_series.get("name", None):
                        chart_series["name"] = key
                    # if name ends with (s), place it onto second yAxis
                    if "(s)" in key:
                        chart_series["yAxisIndex"] = 1
                    chart_series["data"].append(int(serie[key]))
            chart_name.update()
