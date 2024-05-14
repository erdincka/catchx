import datetime
import json
import logging
import re

import importlib_resources
from nicegui import ui

APP_NAME = "catchX"

DEMO = json.loads(importlib_resources.files().joinpath("banking.json").read_text())

MAX_POLL_TIME = 5

class LogElementHandler(logging.Handler):
    """A logging handler that emits messages to a log element."""

    def __init__(self, element: ui.log, level: int = logging.NOTSET) -> None:
        self.element = element
        super().__init__(level)

    def emit(self, record: logging.LogRecord) -> None:
        # change log format for UI
        self.setFormatter(
            logging.Formatter(
                # "%(asctime)s %(levelname)s: %(message)s",
                # datefmt="%H:%M:%S",
                "%(message)s",
            )
        )
        try:
            # remove color formatting for ezfabricctl output
            ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
            msg = self.format(record)
            self.element.push(re.sub(ANSI_RE, "", msg))
        except Exception:
            self.handleError(record)


def dt_from_iso(timestring):
    """
    Convert ISO formatted timestamp to standard one
    """
    # Workaround since received timestring with " AM"/" PM" suffix is not parsed properly
    isPM = " PM" in timestring
    dt = datetime.datetime.strptime(timestring.replace(" AM", "").replace(" PM", ""), "%Y-%m-%dT%H:%M:%S.%f%z")
    return dt + datetime.timedelta(hours=12) if isPM else dt
