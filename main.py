from nicegui import app, ui

from monitoring import *
from functions import *
from page import *

def app_init():

    # Reset metrics
    for metric in [
                "in_txn_pushed",
                "in_txn_pulled",
                "brnz_customers",
                "brnx_txns",
                "slvr_profiles",
                "slvr_txns",
                "slvr_customers",
                "gold_txns",
                "gold_customers",
                "gold_fraud",
            ]:
        app.storage.general[metric] = 0

    # and previous run state if it was hang
    app.storage.general["busy"] = False

    # reset the cluster info
    if "clusters" not in app.storage.general:
        app.storage.general["clusters"] = {}

    # If user is not set, get from environment
    if "MAPR_USER" not in app.storage.general:
        app.storage.general["MAPR_USER"] = os.environ.get("MAPR_USER", "")
        app.storage.general["MAPR_PASS"] = os.environ.get("MAPR_PASS", "")


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
