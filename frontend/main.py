import logging
import os

from nicegui import app, ui, binding

from config import TITLE, STORAGE_SECRET

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("watchfiles").setLevel(logging.FATAL)
binding.MAX_PROPAGATION_TIME = 0.05

# Register page routes (import order = declaration order)
from pages import mesh, domain, old  # noqa: F401, E402

app.add_static_files("/images", local_directory="./images")

app.on_exception(lambda e: logging.getLogger("main").exception(e))


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title=TITLE,
        storage_secret=STORAGE_SECRET,
        reload=os.environ.get("NICEGUI_RELOAD", "false").lower() == "true",
        port=3000,
    )
