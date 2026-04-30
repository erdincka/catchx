import json
import logging

import httpx
from nicegui import ui, app

from config import BACKEND_URL, MONITORING_METRICS, HPE_COLORS

logger = logging.getLogger("monitoring")

_monitoring_timer: ui.timer | None = None


def set_monitoring_active(active: bool):
    global _monitoring_timer
    if _monitoring_timer is not None:
        if active:
            _monitoring_timer.activate()
        else:
            _monitoring_timer.deactivate()


def _get_creds() -> tuple[str, str, str]:
    return (
        app.storage.user.get("MAPR_HOST", ""),
        app.storage.user.get("MAPR_USER", ""),
        app.storage.user.get("MAPR_PASS", ""),
    )


async def _fetch_metrics():
    host, user, password = _get_creds()
    if not host:
        return
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{BACKEND_URL}/api/monitoring/metrics",
                params={"mapr_host": host, "mapr_user": user, "mapr_pass": password},
                timeout=10.0,
            )
        data = response.json()
        for key in MONITORING_METRICS:
            if key in data:
                app.storage.user[key] = data[key]

        # Update chart series if charts are available
        if "series" in data and app.storage.user.get("_charts"):
            _update_charts(data["series"])

    except Exception as error:
        logger.debug("Metrics fetch error: %s", error)


def _update_charts(series: dict):
    charts = app.storage.user.get("_charts", {})
    for key, chart in charts.items():
        if key in series:
            s = series[key]
            chart.options["xAxis"]["data"].append(s["time"])
            for idx, val in enumerate(s["values"]):
                if idx >= len(chart.options["series"]):
                    chart.options["series"].append(_new_series())
                serie = chart.options["series"][idx]
                for k, v in val.items():
                    if not serie.get("name"):
                        serie["name"] = k
                    serie["data"].append(int(v) if not isinstance(v, float) else v)
            chart.run_chart_method("hideLoading")
            chart.update()


def _new_series() -> dict:
    return {
        "type": "line",
        "showSymbol": False,
        "smooth": True,
        "data": [],
        "emphasis": {"focus": "series"},
    }


def monitoring_card():
    """Compact metric badges column — binds to app.storage.user[metric]"""
    with ui.card().bind_visibility_from(app.storage.user, "demo_mode").classes(
        "p-3"
    ).style("border-radius:8px; min-width:200px") as card:
        ui.label("Real-time Metrics").classes(
            "text-xs font-semibold uppercase tracking-widest text-gray-400 mb-2"
        )
        with ui.column().classes("gap-1 w-full"):
            for metric in MONITORING_METRICS:
                label = metric.replace("_", " ").title()
                with ui.row().classes("w-full justify-between items-center"):
                    ui.label(label).classes("text-xs text-gray-500")
                    ui.badge("0").bind_text_from(app.storage.user, metric).props(
                        f"color=negative rounded"
                    ).classes("text-xs font-mono")
    return card


def monitoring_ticker():
    """Horizontal compact metric row for the domain page header"""
    with ui.row().bind_visibility_from(app.storage.user, "demo_mode").classes(
        "items-center gap-3 flex-wrap px-2"
    ) as ticker:
        for metric in MONITORING_METRICS:
            label = metric.replace("_", " ").replace("transactions ", "txn ").title()
            with ui.card().classes("p-1 px-2").style("border-radius:6px"):
                with ui.row().classes("items-center gap-1"):
                    ui.label(label).classes("text-xs text-gray-500")
                    ui.badge("0").bind_text_from(app.storage.user, metric).props(
                        "color=negative rounded"
                    ).classes("text-xs font-mono")
    return ticker


def monitoring_charts():
    """ECharts panels for the monitoring section — returns chart dict"""
    charts = {}
    with ui.card().classes("w-full p-2").style("border-radius:8px"):
        with ui.grid(columns=2).classes("w-full gap-2"):
            for key, title in [
                ("consumer", "Consumer Lag"),
                ("incoming", "Incoming Stream"),
                ("bronze", "Bronze Tier"),
                ("silver", "Silver Tier"),
                ("gold", "Gold Tier"),
            ]:
                charts[key] = _new_echart(title)

    # Store reference so _update_charts can find them
    app.storage.user["_charts"] = charts
    return charts


def _new_echart(title: str) -> ui.echart:
    chart = ui.echart({
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "title": {"left": 4, "text": title, "textStyle": {"fontSize": 11, "color": "#666"}},
        "xAxis": {"type": "category", "boundaryGap": False, "axisLine": {"onZero": True}, "data": []},
        "yAxis": [
            {"type": "value", "name": "Count", "boundaryGap": [0, "100%"], "splitLine": {"show": False}},
            {"type": "value", "name": "Seconds", "axisLabel": {"formatter": "{value}s"}, "boundaryGap": [0, "100%"], "splitLine": {"show": False}},
        ],
        "series": [],
    })
    chart.run_chart_method(":showLoading", r'{text: "Waiting..."}')
    return chart


def logging_card():
    """Real-time log output card"""
    with ui.card().bind_visibility_from(app.storage.user, "demo_mode").classes(
        "w-full p-2 opacity-60 hover:opacity-100 transition-opacity"
    ).style("border-radius:8px") as card:
        log = ui.log().classes("h-20 w-full text-xs")
        import logging as _logging
        handler = _LogHandler(log, _logging.INFO)
        root = _logging.getLogger()
        root.addHandler(handler)
        ui.context.client.on_disconnect(lambda: root.removeHandler(handler))
    return card


def init_monitoring_timer():
    """Call once per page to create the polling timer (inactive by default)."""
    global _monitoring_timer
    _monitoring_timer = ui.timer(3.0, _fetch_metrics, active=False)
    return _monitoring_timer


class _LogHandler(logging.Handler):
    def __init__(self, element: ui.log, level: int = logging.DEBUG):
        self.element = element
        super().__init__(level)

    def emit(self, record: logging.LogRecord):
        try:
            self.element.push(self.format(record))
        except Exception:
            self.handleError(record)
