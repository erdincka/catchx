import csv
import json
from nicegui import run
import pandas as pd

from common import *
from mock import dummy_fraud_score
import streams
import tables
import iceberger
from functions import upsert_profile

logger = logging.getLogger("ingestion")


async def ingest_transactions():

    # Input stream
    input_stream_path = f"{BASEDIR}/{STREAM_INCOMING}"
    input_topic = TOPIC_TRANSACTIONS

    # if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{input_stream_path}"): # stream not created yet
    #     ui.notify(f"Stream not found {input_stream_path}", type="warning")
    #     return

    app.storage.user['busy'] = True

    # Output table
    output_table_path = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}"

    transactions = []

    for record in await run.io_bound(streams.consume, stream=input_stream_path, topic=input_topic, consumer_group="ingestion"):
        txn = json.loads(record)

        logger.info("Ingesting transaction: %s", txn["_id"])

        transactions.append(txn)
        # update the profile
        await upsert_profile(txn)

    if len(transactions) > 0:
        # Write into DocumentDB for Bronze tier (raw data) - this will allow CDC for fraud detection process
        if await tables.upsert_documents(table_path=output_table_path, docs=transactions):
            ui.notify(f"Saved {len(transactions)} records in {output_table_path}", type="positive")
        else:
            ui.notify(f"Failed to update table: {TABLE_TRANSACTIONS} in {VOLUME_BRONZE}", type='negative')

    # release when done
    app.storage.user['busy'] = False


# SSE-TODO: read from stream, upsert profiles table, and write raw data into iceberg table
### AVAILABLE IN THE AIRFLOW DAG
async def ingest_transactions_spark():

    stream_path = f"{BASEDIR}/{STREAM_INCOMING}" # input
    table_path = f"{BASEDIR}/{VOLUME_BRONZE}/{TABLE_PROFILES}" # output

    ### psuedo code below
    # spark.read_stream(stream_path).upsert_maprdb_binarytable(table=table_path).write_iceberg(schemaname=DEMO['volumes']['bronze'], tablename=DEMO['tables']['transactions'])
    ###

    ### profiles binary table has the following object for each record/row
    # profile = {
    #     "_id": get_customer_id(message['receiver_account']),
    #     "score": await dummy_fraud_score()
    # }


# SSE-TODO: Airflow DAG to process csv file at MOUNT_PATHlondon/apps/catchx/customers.csv,
# and write records into iceberg table at MOUNT_PATHlondon/apps/catchx/bronze/customers/
async def ingest_customers_airflow():
    """
    Read CSV file and ingest into Iceberg table
    """
    csvpath = f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv"

    COUNT_OF_ROWS = 0

    ### psuedo code below
    # if spark.read_csv(csvpath).write_iceberg(schemaname=DEMO['volumes']['bronze'], tablename=DEMO['tables']['customers']):
    #     ui.notify(f"Stored {len(COUNT_OF_ROWS)} records in bronze volume with Iceberg", type='positive')


async def ingest_customers_iceberg():
    """
    Read customers.csv and ingest into iceberg table in "bronze" tier
    """

    tier = VOLUME_BRONZE
    tablename = TABLE_CUSTOMERS

    csvpath = f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv"

    try:
        with open(csvpath, "r", newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            logger.info("Reading %s", csvpath)
            ui.notify(f"Reading customers from {csvpath}")

            # Write into iceberg for Bronze tier (raw data)
            new_customers = [cust for cust in csv_reader]
            if iceberger.write(tier=tier, tablename=tablename, records=new_customers):
                ui.notify(f"Saved {tablename} into {tier} with Iceberg", type='positive')
                app.storage.user["ingest_customers"] = len(new_customers)
                return True
            else:
                ui.notify("Failed to write into Iceberg table")
                return False

    except Exception as error:
        logger.warning("Failed to read customers.csv: %s", error)
        ui.notify(error, type='negative')
        return False

    finally:
        app.storage.user['busy'] = False


async def fraud_detection():
    """
    Simulate an AI inference query to all incoming transactions.
    Reading from bronze transactions.

    Writing to gold tier transactions
    """

    input_table = f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{VOLUME_BRONZE}/{TABLE_TRANSACTIONS}"
    output_table = f"{BASEDIR}/{VOLUME_GOLD}/{TABLE_TRANSACTIONS}"

    if not os.path.lexists(input_table):
        ui.notify(f"No transacations found in bronze tier {input_table}", type="warning")
        return

    fraud_count = 0
    non_fraud_count = 0

    for record in await tables.get_documents(table_path=input_table, limit=None):
        logger.info("Checking transaction for fraud: %s", record["_id"])

        # Where the actual scoring mechanism should work
        calculated_fraud_score = await dummy_fraud_score()

        # randomly select a small subset as fraud
        if calculated_fraud_score > 85:
            # TODO: should provide a widget/table to see fraud txn details
            ui.notify(f"Possible fraud in transaction between accounts {record['sender_account']} and {record['receiver_account']}", type='negative')
            # Write to gold/reporting tier
            possible_fraud = pd.DataFrame.from_dict([record])
            possible_fraud["fraud"] = True
            # clean up transaction details before writing into gold table
            possible_fraud.drop(["sender_account", "receiver_account"], axis=1, inplace=True)

            # TODO: possibly more performant to collect all fraud transactions and update them all at once
            if await tables.delta_table_upsert(output_table, possible_fraud):
                fraud_count += 1

            # and update score for the profiles table - not implemented

        else:
            non_fraud_count += 1
            logger.info("Non fraudulant transaction %s", record["_id"])

    ui.notify(f"Reported {fraud_count} fraud and {non_fraud_count} valid transactions", type='warning')
