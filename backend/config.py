import logging

BASEDIR = "/catchx-demo"
MOUNT_PATH = "/mapr"

# Volume MOUNT-PATH suffixes (used in file/table paths inside the volumes)
VOLUME_BRONZE = "bronze"
VOLUME_SILVER = "silver"
VOLUME_GOLD   = "gold"

# Volume NAMES on the cluster (qualified to avoid collisions with other deployments)
CATCHX_VOL_PARENT = "catchx-demo"    # mounted at /catchx-demo
CATCHX_VOL_BRONZE = "catchx-bronze"  # mounted at /catchx-demo/bronze
CATCHX_VOL_SILVER = "catchx-silver"  # mounted at /catchx-demo/silver
CATCHX_VOL_GOLD   = "catchx-gold"    # mounted at /catchx-demo/gold

STREAM_INCOMING = "incoming"
STREAM_CHANGELOG = "changelog"

TOPIC_TRANSACTIONS = "transactions"

TABLE_PROFILES = "profiles"
TABLE_TRANSACTIONS = "transactions"
TABLE_CUSTOMERS = "customers"
TABLE_FRAUD = "fraud_activity"

MON_REFRESH_INTERVAL = 3.0
FETCH_RECORD_NUM = 15

# Transactions scoring above this (0-100) are flagged as suspected fraud.
FRAUD_SCORE_THRESHOLD = 85

TRANSACTION_CATEGORIES = [
    "Entertainment", "Shopping", "Education", "Investment", "Bills",
    "Transport", "Income", "Home", "Transfers", "Dining", "Other",
]

MONITORING_METRICS = [
    "source_customers",
    "source_transactions",
    "transactions_ingested",
    "transactions_processed",
    "bronze_customers",
    "bronze_transactions",
    "silver_profiles",
    "silver_transactions",
    "silver_customers",
    "gold_transactions",
    "gold_customers",
    "gold_fraud",
]

def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s:%(levelname)s:%(name)s (%(funcName)s:%(lineno)d): %(message)s",
        datefmt="%H:%M:%S",
    )

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("faker").setLevel(logging.FATAL)
    logging.getLogger("pyiceberg.io").setLevel(logging.WARNING)
    logging.getLogger("mapr.ojai.storage.OJAIConnection").setLevel(logging.WARNING)
    logging.getLogger("mapr.ojai.storage.OJAIDocumentStore").setLevel(logging.WARNING)
    logging.getLogger("watchfiles").setLevel(logging.FATAL)
