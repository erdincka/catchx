from nicegui import app, ui

from monitoring import *
from functions import *
from page import *

def app_init():
    pass
    # ui.add_head_html('<link href="https://api.iconify.design/grommet-icons?icons=apps,home,connect,add,user-settings,download-option,upload-option,circle-information" rel="stylesheet" />')

    # Reset previous run state if it was hang
    # app.storage.user["busy"] = False

    # # reset the cluster info
    # if "clusters" not in app.storage.user:
    #     app.storage.user["clusters"] = {}

    # # If user is not set, get from environment
    # if "MAPR_USER" not in app.storage.user:
    #     app.storage.user["MAPR_USER"] = os.environ.get("MAPR_USER", "")
    #     app.storage.user["MAPR_PASS"] = os.environ.get("MAPR_PASS", "")


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
        dark=None,
        storage_secret=STORAGE_SECRET,
        reload=True,
        port=3000,
    )
