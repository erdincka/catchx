import csv
from nicegui import run

from helpers import *
import streams
import iceberger
from functions import upsert_profile

async def ingest_transactions():

    stream_path = f"{DEMO['basedir']}/{DEMO['stream']}"
    
    app.storage.user['busy'] = True

    transactions = []

    for record in await run.io_bound(streams.consume, stream=stream_path, topic=DEMO['topic']):
        txn = json.loads(record)
        transactions.append(txn)
        # update the profile
        await upsert_profile(txn)

    # Write into iceberg for Bronze tier (raw data)
    if iceberger.write(DEMO['volumes']['bronze'], DEMO['tables']['transactions'], records=transactions):
        ui.notify(f"Saved {len(transactions)} transactions in bronze volume with Iceberg", type="positive")

    # release when done
    app.storage.user['busy'] = False


async def ingest_customers():
    csvpath = f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{DEMO['tables']['customers']}.csv"
    try:
        with open(csvpath, "r", newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            logger.info("Reading %s", csvpath)

            # Write into iceberg for Bronze tier (raw data)
            if iceberger.write(schemaname=DEMO['volumes']['bronze'], tablename=DEMO['tables']['customers'], records=[cust for cust in csv_reader]):
                ui.notify(f"Stored {len(csv_reader)} records in bronze volume with Iceberg", type='positive')

    except Exception as error:
        logger.debug("Failed to read customers.csv: %s", error)

    finally:
        ui.notify("Customer data ingested into bronze tier", type='positive')


def customer_data_list():
    with ui.dialog().props("full-width") as dialog, ui.card().classes("grow relative"):
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-4 top-2")
        result = ui.log().classes("w-full").style("white-space: pre-wrap")

        for history in iceberger.history(tier=DEMO['volumes']['bronze'], tablename=DEMO['tables']['customers']):
            result.push(history)

    dialog.on("close", lambda d=dialog: d.delete())
    dialog.open()



