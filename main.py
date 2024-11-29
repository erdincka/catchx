from nicegui import app, ui

from monitoring import *
from functions import *
from page import *

def app_init():
    os.environ["LD_LIBRARY_PATH"] = "/opt/mapr/lib"

# catch-all exceptions
app.on_exception(gracefully_fail)
app.on_disconnect(app_init)

# serve images
app.add_static_files("/images", local_directory="./images")

# configure the logging
configure_logging()

logger = logging.getLogger("main")

app_init()


if __name__ in {"__main__", "__mp_main__"}:
    ui.run(
        title=TITLE,
        # dark=None,
        storage_secret=STORAGE_SECRET,
        reload=True,
        port=3000,
    )
