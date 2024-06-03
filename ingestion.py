import csv
from nicegui import run

from helpers import *
import streams
import iceberger
from functions import upsert_profile
import sparking

logger = logging.getLogger("ingestion")


async def ingest_transactions():

    stream_path = f"{DEMO['basedir']}/{DEMO['stream']}"
    
    app.storage.user['busy'] = True

    transactions = []

    for record in await run.io_bound(streams.consume, stream=stream_path, topic=DEMO['topic']):
        txn = json.loads(record)
        transactions.append(txn)
        # update the profile
        await upsert_profile(txn)

    if len(transactions) > 0:
        # Write into iceberg for Bronze tier (raw data)
        if iceberger.write(DEMO['volumes']['bronze'], DEMO['tables']['transactions'], records=transactions):
            ui.notify(f"Saved {len(transactions)} transactions in bronze volume with Iceberg", type="positive")

    # release when done
    app.storage.user['busy'] = False


# SSE-TODO: read from stream, upsert profiles table, and write raw data into iceberg table
async def ingest_transactions_spark():

    stream_path = f"{DEMO['basedir']}/{DEMO['stream']}" # input
    table_path = f"{DEMO['basedir']}/{DEMO['volumes']['bronze']}/{DEMO['tables']['profiles']}" # output
    
    ### psuedo code below
    # spark.read_stream(stream_path).upsert_maprdb_binarytable(table=table_path).write_iceberg(schemaname=DEMO['volumes']['bronze'], tablename=DEMO['tables']['transactions'])
    ###
 
    ### profiles binary table has the following object for each record/row
    # profile = {
    #     "_id": get_customer_id(message['receiver_account']),
    #     "score": await dummy_fraud_score()
    # }


# SSE-TODO: Airflow DAG to process csv file at /mapr/london/apps/catchx/customers.csv, 
# and write records into iceberg table at /mapr/london/apps/catchx/bronze/customers/
async def ingest_customers_airflow():
    """
    Read CSV file and ingest into Iceberg table
    """
    csvpath = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{DEMO['tables']['customers']}.csv"

    COUNT_OF_ROWS = 0

    ### psuedo code below
    # if spark.read_csv(csvpath).write_iceberg(schemaname=DEMO['volumes']['bronze'], tablename=DEMO['tables']['customers']):
    #     ui.notify(f"Stored {len(COUNT_OF_ROWS)} records in bronze volume with Iceberg", type='positive')


async def ingest_customers_iceberg():
    """
    Read customers.csv and ingest into iceberg table in "bronze" tier
    """

    tier = DEMO['volumes']['bronze']
    tablename = DEMO['tables']['customers']

    csvpath = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{DEMO['tables']['customers']}.csv"

    try:
        with open(csvpath, "r", newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            logger.info("Reading %s", csvpath)

            # Write into iceberg for Bronze tier (raw data)
            if iceberger.write(tier=tier, tablename=tablename, records=[cust for cust in csv_reader]):
                ui.notify(f"Saved {tablename} into {tier} with Iceberg", type='positive')
            else:
                ui.notify("Failed to write into Iceberg table")

    except Exception as error:
        logger.debug("Failed to read customers.csv: %s", error)


def customer_data_history():

    tier = DEMO['volumes']['bronze']
    tablename = DEMO['tables']['customers']

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")
        result = ui.log().classes("w-full mt-6").style("white-space: pre-wrap")

        for history in iceberger.history(tier=tier, tablename=tablename):
            result.push(history)

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


def customer_data_tail():

    tier = DEMO['volumes']['bronze']
    tablename = DEMO['tables']['customers']

    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-2 top-2")

        df = iceberger.tail(tier=tier, tablename=tablename)
        ui.table.from_pandas(df).classes('w-full mt-6')

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()


