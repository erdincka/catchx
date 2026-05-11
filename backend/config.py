import logging

APP_NAME = "Data Fabric"
TITLE = "Building a Hybrid Data Mesh"
DATA_PRODUCT = "fraud"

BASEDIR = "/nexmesh-demo"
MOUNT_PATH = "/mapr"

# Volume MOUNT-PATH suffixes (used in file/table paths inside the volumes)
VOLUME_BRONZE = "bronze"
VOLUME_SILVER = "silver"
VOLUME_GOLD   = "gold"

# Volume NAMES on the cluster (qualified to avoid collisions with other deployments)
NEXMESH_VOL_PARENT = "nexmesh-demo"    # mounted at /nexmesh-demo
NEXMESH_VOL_BRONZE = "nexmesh-bronze"  # mounted at /nexmesh-demo/bronze
NEXMESH_VOL_SILVER = "nexmesh-silver"  # mounted at /nexmesh-demo/silver
NEXMESH_VOL_GOLD   = "nexmesh-gold"    # mounted at /nexmesh-demo/gold

STREAM_INCOMING = "incoming"
STREAM_CHANGELOG = "changelog"

TOPIC_TRANSACTIONS = "transactions"

TABLE_PROFILES = "profiles"
TABLE_TRANSACTIONS = "transactions"
TABLE_CUSTOMERS = "customers"
TABLE_FRAUD = "fraud_activity"

MAX_POLL_TIME = 2.0
MON_REFRESH_INTERVAL = 3.0
FETCH_RECORD_NUM = 15

TRANSACTION_CATEGORIES = [
    "Entertainment", "Shopping", "Education", "Investment", "Bills",
    "Transport", "Income", "Home", "Transfers", "Dining", "Other",
]

MONITORING_METRICS = [
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

HPE_COLORS = {
    "green": "#01A982",
    "purple": "#7630EA",
    "teal": "#00E8CF",
    "blue": "#00739D",
    "red": "#C54E4B",
    "orange": "#FF8300",
    "yellow": "#FEC901",
    "darkgreen": "#008567",
    "darkpurple": "#6633BC",
    "darkteal": "#117B82",
    "darkblue": "#00567A",
    "darkred": "#A2423D",
    "darkorange": "#9B6310",
    "darkyellow": "#8D741C",
}

DATA_DOMAIN = {
    "description": "What, why and how?",
    "diagram": "/images/frauddomain.png",
    "link": "https://github.com/erdincka/nexmesh",
}

DOCUMENTATION = {
    "Overview": """
For this demo, we will be using an end to end data pipeline for a financial transaction workflow.

We are handling data ingestion from streaming and batch data sources, processing it through its lifecycle using a 'medallion architecture'.

In a medallion architecture, we are using the bronze tier as the landing page where all the raw data is stored as soon as it arrives.

Then we will apply our ETL processes to clean, enrich and filter data, so the next tier, silver tier, has the organisation's curated data with all the details and information ready to process.

The final tier, gold tier, gives us a consolidated data lake that is used for reporting, trend analysis etc, but doesn't include any individual transaction or customer detail.

Finally, we expose the data in our gold tier to reporting engines through standard connection/API endpoints to be consumed and shared.
    """,
    "Source Data Generation": """
Customer data is used for batch ingestion, that can be processed using an ETL process.

Transactions data is used for streaming ingestion.

You can view the source data generation code, sample it, and submit it to the pipeline. You can also generate a new set of customers or transactions for testing.
    """,
    "Data Ingestion and ETL": """
The first step in our medallion architecture is to ingest the data from the sources. We will be using two different methods to ingest the data.

- Batch ingestion: This method is used when we have a large amount of data that needs to be processed.

- Streaming ingestion: This method is used when we have a small amount of data that needs to be processed in real time.

We will be using both methods to ingest the data. Customers are ingested as batch data and transactions are ingested as streaming data.
    """,
    "Data Enrichment": """
The next step in our medallion architecture is to enrich the data with additional information that is not available in the source.

We simulate the enrichment process by adding and hiding information about customers and transactions.

  - Add country name from country_code to the customer data.
  - Find and add iso3166_2 code for customers.
  - Hide birthday and current_location of customers.
  - Add category name to the transaction data.
    """,
    "Data Consolidation": """
The final step in our medallion architecture is to consolidate the data from multiple sources into a single source.

With this, we will be able to create a "product" that we can share with the rest of the organisation.

Data consolidation is done by taking customer and transaction data and clean them from individual information and then providing a consolidated summary view for all the transactions and customer base.
    """,
}


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
