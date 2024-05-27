import asyncio
import csv
import random
from time import sleep
from faker import Faker
from nicegui import app, run

from helpers import *
from streams import consume, produce
from tables import search_documents, upsert_document


fake = Faker("en_US")


def cluster_configuration_dialog():
    with ui.dialog().props("position=right full-height") as dialog, ui.card().classes("relative bordered"):
        # with close button
        ui.button(icon="close", on_click=dialog.close).props("flat round dense").classes("absolute right-4 top-4")

        with ui.card_section().classes("mt-6"):
            ui.label("Client files").classes("text-lg")
            ui.label("config.tar and/or jwt_tokens.tar.gz").classes("text-subtitle2")
            ui.upload(label="Upload", on_upload=upload_client_files, multiple=True, auto_upload=True, max_files=2).props("accept='application/x-tar,application/x-gzip' hide-upload-btn").classes("w-full")

        ui.separator()
        with ui.card_section():
            ui.label("Select cluster").classes("text-lg")
            ui.toggle(app.storage.general["clusters"]).bind_value(app.storage.general, "cluster")

        ui.separator()
        with ui.card_section():
            ui.label("Local user credentials").classes("text-lg")
            ui.label("required for REST API and monitoring").classes("text-subtitle2")
            with ui.row().classes("w-full place-items-center"):
                ui.input("Username").bind_value(app.storage.general, "MAPR_USER")
                ui.input("Password", password=True, password_toggle_button=True).bind_value(app.storage.general, "MAPR_PASS")

        ui.separator()
        with ui.card_section():
            ui.label("Configure").classes("text-lg")
            ui.button("configure.sh -R", on_click=lambda: run_command("/opt/mapr/server/configure.sh -R"))

        ui.separator()
        with ui.card_section():
            ui.label("Login").classes("text-lg")
            ui.label("if not using JWT").classes("text-subtitle2")
            ui.button("maprlogin", on_click=lambda: run_command(f"echo {app.storage.general['MAPR_PASS']} | maprlogin password -user {app.storage.general['MAPR_USER']}"))

        ui.separator()
        with ui.card_section():
            ui.label("Create the volumes and the stream").classes("text-lg")
            ui.label("required constructs for the demo")
            ui.button("Create", on_click=create_volumes_and_stream)

        ui.separator()
        with ui.card_section():
            ui.label("Generate demo data").classes("text-lg")
            ui.label("create transactions and customers csv files")
            ui.button("Generate", on_click=generate_demo_data)

        ui.separator()
        with ui.card_section():
            ui.label("Show volumes").classes("text-lg")
            with ui.row().classes("w-full place-items-center"):
                for vol in DEMO['volumes']:
                    ui.button(f"List {vol}", on_click=lambda v=vol: run_command(f"ls -la /mapr/{get_cluster_name()}{DEMO['basedir']}/{DEMO['volumes'][v]}"))

        ui.separator()
        with ui.card_section():
            ui.label("Delete All!").classes("text-lg")
            ui.label("Will remove all volumes and the stream, all will be gone!").classes("text-subtitle2")
            ui.button("DELETE ALL!", on_click=delete_volumes_and_stream, color="negative")

    dialog.open()


def fake_customer():
    return {
        "id": get_uuid_key(),
        "account_number": fake.iban(),
        "country": random.choice(["GB", "DE", "US", "JP", "IN"]),
        "data": ""
    }


def fake_transaction(sender: str, receiver: str):
    return {
        "id": get_uuid_key(),
        "sender_account": sender,
        "receiver_account": receiver,
        "amount": round(fake.pyint(0, 10_000), 2),
        "currency": fake.currency_code(),
        "transaction_date": fake.past_datetime(start_date="-12M").timestamp(),
    }


def generate_demo_data():
    """
    Create customers and transactions CSV files with randomly generated data
    """

    number_of_customers = 1_000
    number_of_transactions = 10_000

    # customers
    customers = []
    for _ in range(number_of_customers):
        customers.append(fake_customer())

    with open(f"/mapr/{get_cluster_name()}{DEMO['basedir']}/customers.csv", "w", newline='') as csvfile:
        fieldnames = fake_customer().keys() # ["id", "account_number", "country", "data"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(customers)
        
    # transactions
    transactions = []
    for _ in range(number_of_transactions):
        # generate transaction with randomly selected sender and reciever accounts
        sender = customers[random.randrange(number_of_customers)]['account_number']
        receiver = customers[random.randrange(number_of_customers)]['account_number']
        transactions.append(fake_transaction(sender, receiver))

    with open(f"/mapr/{get_cluster_name()}{DEMO['basedir']}/transactions.csv", "w", newline='') as csvfile:
        fieldnames = fake_transaction("X", "Y").keys() # ["id", "sender_account", "receiver_account", "amount", "currency", "transaction_date"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)


def publish_transaction(txn: dict):
    stream_path = f"{DEMO['basedir']}/{DEMO['stream']}"

    if produce(stream_path, DEMO['topic'], json.dumps(txn)):
        logger.debug("Sent %s", txn["id"])
        # add delay
        sleep(0.2)
    else:
        logger.warning("Failed to send message to %s", DEMO['topic'])


async def transaction_feed_service():
    count = 0
    try:
        with open(f"/mapr/{get_cluster_name()}{DEMO['basedir']}/transactions.csv", "r", newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            for transaction in csv_reader:
                # only publish randomly selected transactions, limited to 100
                if count > 100: break
                if random.randrange(10) < 3: # ~30% to be selected randomly
                    await run.io_bound(publish_transaction, transaction)
                    count += 1

                else: logger.debug("Skipped transaction: %s", transaction["id"])
                # add delay
                # await asyncio.sleep(0.05)

    except Exception as error:
        logger.warning("Cannot read transactions: %s", error)


async def ingest_transactions():

    stream_path = f"{DEMO['basedir']}/{DEMO['stream']}"
    
    app.storage.user['busy'] = True

    for record in await run.io_bound(consume, stream=stream_path, topic=DEMO['topic']):
        message = json.loads(record)
        
        # Write into iceberg for Bronze tier (raw data)
        # iceberger.write(f"{DEMO['volumes']['bronze']}/{DEMO['tables']['transactions']}", message)
        logger.info("Store in bronze volume with Iceberg: %s", message['id'])
        # then update the profile
        await create_update_profile(message)

    # release when done
    app.storage.user['busy'] = False


async def create_update_profile(message: dict):
    """
    assign customer ID and fraud score for the receiver account
    """
    profile = {
        "_id": get_customer_id(message['receiver_account']),
        "score": await dummy_fraud_score()
    }

    table_path = f"{DEMO['basedir']}/{DEMO['volumes']['bronze']}/{DEMO['tables']['profiles']}"

    if upsert_document(host=app.storage.general["cluster"], table_path=table_path, json_dict=profile):
        logger.info("Updated profile: %s with score: %d", profile['_id'], profile['score'])


async def dummy_fraud_score():
    # introduce delay for calculation
    await asyncio.sleep(0.05)
    # respond with a probability
    return random.randint(0, 10)


def get_customer_id(from_account: str):
    """
    find the customerID from customers table using account #
    """
    return ""  # TODO: get customer ID from customers table


async def refine_transaction(message: dict):

    logger.info("Transaction cleanup")
    table_path = f"{DEMO['basedir']}/{DEMO['volumes']['bronze']}/{DEMO['tables']['profiles']}"
    # iceberger.read(table_path, message['id'])


async def customer_data_ingestion():
    await run.io_bound(print, "will run airflow")


async def customer_data_list():
    await run_command(f"ls -l /mapr/{get_cluster_name()}{DEMO['basedir']}/{DEMO['volumes']['bronze']}/{DEMO['tables']['customers']}")


def detect_fraud(params: list, count: int):
    # params is not used
    table_path = f"{DEMO['basedir']}/{DEMO['volumes']['bronze']}/{DEMO['tables']['profiles']}"

    # just a simulation of query to the profiles table, 
    # if any doc is found with the number as their CHECK DIGITS in IBAN, we consider it as fraud!
    whereClause = {
        "$or":[
            {"$like":{"sender":f"GB{count}%"}},
            {"$like":{"receiver":f"GB{count}%"}}
        ]
    }

    for doc in search_documents(app.storage.general.get('MAPR_IP', 'localhost'), table_path, whereClause):

        logger.debug("DB GET RESPONSE: %s", doc)

        app.storage.general["counting"] = app.storage.general.get("counting", 0) + 1
        yield f"Fraud txn from {doc['sender']} to {doc['receiver']}"
        
