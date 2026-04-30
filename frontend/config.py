import os

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")

TITLE = "Building a Hybrid Data Mesh"
STORAGE_SECRET = "ezmer@1r0cks"
APP_NAME = "Data Fabric"
DIAGRAM = "/images/hubspoke.png"

DATA_PRODUCT = "fraud"

VOLUME_BRONZE = "bronze"
VOLUME_SILVER = "silver"
VOLUME_GOLD = "gold"

TABLE_CUSTOMERS = "customers"
TABLE_TRANSACTIONS = "transactions"
TABLE_PROFILES = "profiles"
TABLE_FRAUD = "fraud_activity"

BASEDIR = "/demovol"
MOUNT_PATH = "/mapr"

STREAM_INCOMING = "incoming"
TOPIC_TRANSACTIONS = "transactions"

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
    "link": "https://github.com/erdincka/catchx",
}

DOCUMENTATION = {
    "Overview": """
For this demo, we will be using an end to end data pipeline for a financial transaction workflow.

We are handling data ingestion from streaming and batch data sources, processing it through its lifecycle using a **medallion architecture**.

- **Bronze** tier: raw data as it arrives
- **Silver** tier: curated, enriched and cleaned data
- **Gold** tier: consolidated analytics-ready data lake for reporting and trend analysis

Finally, we expose the data in our gold tier to reporting engines through standard connection/API endpoints.
    """,
    "Source Data Generation": """
Customer data is used for batch ingestion. Transactions data is used for streaming ingestion.

You can view the source data generation code, sample it, and submit it to the pipeline. You can also generate a new set of customers or transactions for testing.
    """,
    "Data Ingestion and ETL": """
The first step in our medallion architecture is to ingest the data from the sources using two different methods:

- **Batch ingestion**: for large amounts of data (customers CSV → Iceberg)
- **Streaming ingestion**: for real-time small data (transactions → Kafka → DocumentDB)
    """,
    "Data Enrichment": """
The enrichment process adds and masks information about customers and transactions:

- Add country name from `country_code`
- Find and add `iso3166_2` county code
- Mask `birthdate` and `current_location`
- Add transaction `category`
    """,
    "Data Consolidation": """
Consolidation creates a shareable "data product" by:

- Merging customer and transaction data
- Removing PII (names, accounts, birthdates)
- Providing a consolidated summary view for analytics
    """,
}

cluster_configuration_steps = [
    {"name": "clusterinfo", "info": "Get cluster details", "status": "pending"},
    {"name": "reconfigure", "info": "Configure cluster", "status": "pending"},
    {"name": "createvolumes", "info": "Create volumes and streams", "status": "pending"},
    {"name": "mockdata", "info": "Create dummy data", "status": "pending"},
]
