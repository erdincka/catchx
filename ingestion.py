import csv
import json
from nicegui import run
import pandas as pd

from common import *
import streams
import tables
import iceberger
from functions import upsert_profile

logger = logging.getLogger("ingestion")


async def ingest_transactions():

    # Input stream
    input_stream_path = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['streams']['incoming']}"
    input_topic = DATA_DOMAIN['topics']['transactions']
    
    if not os.path.lexists(f"/edfs/{get_cluster_name()}{input_stream_path}"): # stream not created yet
        ui.notify(f"Stream not found {input_stream_path}", type="warning")
        return

    app.storage.user['busy'] = True

    # Output table
    output_table_path = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['bronze']}/{DATA_DOMAIN['tables']['transactions']}"
    
    transactions = []

    for record in await run.io_bound(streams.consume, stream=input_stream_path, topic=input_topic, consumer_group="ingestion"):
        txn = json.loads(record)

        logger.debug("Ingesting transaction: %s", txn["_id"])

        transactions.append(txn)
        # update the profile
        await upsert_profile(txn)

    if len(transactions) > 0:
        # Write into DocumentDB for Bronze tier (raw data) - this will allow CDC for fraud detection process
        if tables.upsert_documents(table_path=output_table_path, docs=transactions):
            ui.notify(f"Saved {len(transactions)} transactions in {output_table_path}", type="positive")
        else:
            ui.notify(f"Failed to update table: {DATA_DOMAIN['tables']['transactions']} in {DATA_DOMAIN['volumes']['bronze']}", type='negative')
        # # Write into iceberg for Bronze tier (raw data)
        # logger.info("Writing %d transactions with iceberg", len(transactions))
        # if iceberger.write(DATA_DOMAIN['volumes']['bronze'], DATA_DOMAIN['tables']['transactions'], records=transactions):
        #     ui.notify(f"Saved {len(transactions)} transactions in {DATA_DOMAIN['volumes']['bronze']} volume with Iceberg", type="positive")
        # else:
        #     ui.notify(f"Failed to save table: {DATA_DOMAIN['tables']['transactions']} in {DATA_DOMAIN['volumes']['bronze']}", type='negative')
    # release when done
    app.storage.user['busy'] = False


# SSE-TODO: read from stream, upsert profiles table, and write raw data into iceberg table
async def ingest_transactions_spark():

    stream_path = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['streams']['incoming']}" # input
    table_path = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['bronze']}/{DATA_DOMAIN['tables']['profiles']}" # output
    
    ### psuedo code below
    # spark.read_stream(stream_path).upsert_maprdb_binarytable(table=table_path).write_iceberg(schemaname=DEMO['volumes']['bronze'], tablename=DEMO['tables']['transactions'])
    ###
 
    ### profiles binary table has the following object for each record/row
    # profile = {
    #     "_id": get_customer_id(message['receiver_account']),
    #     "score": await dummy_fraud_score()
    # }


# SSE-TODO: Airflow DAG to process csv file at /edfs/london/apps/catchx/customers.csv, 
# and write records into iceberg table at /edfs/london/apps/catchx/bronze/customers/
async def ingest_customers_airflow():
    """
    Read CSV file and ingest into Iceberg table
    """
    csvpath = f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['tables']['customers']}.csv"

    COUNT_OF_ROWS = 0

    ### psuedo code below
    # if spark.read_csv(csvpath).write_iceberg(schemaname=DEMO['volumes']['bronze'], tablename=DEMO['tables']['customers']):
    #     ui.notify(f"Stored {len(COUNT_OF_ROWS)} records in bronze volume with Iceberg", type='positive')


async def ingest_customers_iceberg():
    """
    Read customers.csv and ingest into iceberg table in "bronze" tier
    """

    tier = DATA_DOMAIN['volumes']['bronze']
    tablename = DATA_DOMAIN['tables']['customers']

    csvpath = f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['tables']['customers']}.csv"
    app.storage.user['busy'] = True

    try:
        with open(csvpath, "r", newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            logger.info("Reading %s", csvpath)

            # Write into iceberg for Bronze tier (raw data)
            if iceberger.write(tier=tier, tablename=tablename, records=[cust for cust in csv_reader]):
                ui.notify(f"Saved {tablename} into {tier} with Iceberg", type='positive')
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
    Reading from "incoming" data stream.

    The query result will be an update to the "silver" "profile" database.
    """

    input_topic = DATA_DOMAIN['topics']['transactions']
    input_stream = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['streams']['incoming']}"
    output_table = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['volumes']['silver']}/{DATA_DOMAIN['tables']['profiles']}"

    if not os.path.lexists(f"/edfs/{get_cluster_name()}{input_stream}"): # stream not created yet
        ui.notify(f"Stream not found {input_stream}", type="warning")
        return

    # Gold table to update if fraud found
    mydb = f"mysql+pymysql://{app.storage.general['MYSQL_USER']}:{app.storage.general['MYSQL_PASS']}@{app.storage.general['cluster']}/{DATA_DOMAIN['name']}"

    fraud_count = 0
    non_fraud_count = 0

    for record in await run.io_bound(streams.consume, stream=input_stream, topic=input_topic, consumer_group="fraud"):
        txn = json.loads(record)

        logger.debug("Checking transaction for fraud: %s", txn["_id"])

        # Where the actual scoring mechanism should work
        calculated_fraud_score = await dummy_fraud_score()

        if int(calculated_fraud_score) > 85:
            ui.notify(f"Possible fraud in transaction between accounts {txn['sender_account']} and {txn['receiver_account']}", type='negative')
            # Write to gold/reporting tier
            possible_fraud = pd.DataFrame.from_dict([txn])
            possible_fraud["score"] = calculated_fraud_score
            fraud_count += possible_fraud.to_sql(name="fraud_activity", con=mydb, if_exists='append')

            # and update score for the profiles - not implemented                
 
        else:
            non_fraud_count += 1
            logger.debug("Non fraudulant transaction %s", txn["_id"])

    ui.notify(f"Reported {fraud_count} fraud and {non_fraud_count} valid transactions", type='warning')

