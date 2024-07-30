import csv
import json
import random
import uuid
from faker import Faker

from nicegui import run
from common import *
import streams


logger = logging.getLogger("mock")

# fake = Faker(["en_GB", "en_US", "de_DE", "ja_JP", "en_IN"])
fake = Faker(["en_GB"])


def fake_customer():
    profile = fake.profile()
    # remove newline from address field
    profile['address'] = profile['address'].replace('\n', ' ')
    customer = {
        "_id": uuid.uuid4().hex,
        **profile,
        "account_number": fake.iban(),
        "county": fake.county(),
        "country_code": fake.current_country_code()
    }
    # clean up
    del customer["website"]
    del customer["residence"]
    customer["address"] = customer["address"].replace('\n', ' ').replace('\r', ' ')

    return customer


def fake_transaction(sender: str, receiver: str):
    return {
        "_id": uuid.uuid4().hex,
        "sender_account": sender,
        "receiver_account": receiver,
        "amount": round(fake.pyint(0, 10_000), 2),
        # "currency": fake.currency_code(),
        "transaction_date": fake.past_datetime(start_date="-12M").timestamp(),
    }


async def get_new_transactions(count: int = 100):
    transactions = []

    customers = []
    try:
        with open(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv", "r", newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            customers = [row for row in reader]

    except:
        ui.notify("Failed to read customers, create them first!", type='warning')
        return None

    try:
        # generate transaction with randomly selected sender and reciever accounts
        for _ in range(count):
            sender = customers[random.randrange(len(customers))]['account_number']
            receiver = customers[random.randrange(len(customers))]['account_number']
            transactions.append(fake_transaction(sender, receiver))

        logger.info(f"Returning {len(transactions)} transactions")

    except Exception as error:
        ui.notify(error, type='warning')

    finally:
        return transactions

async def create_transactions(count: int = 100):

    try:
        transactions = await get_new_transactions(count)

        if len(transactions) == 0: return # silently discard if transaction creation is failed

        filepath = f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv" if count < 100 else f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}-bulk.csv"

        with open(
            file=filepath,
            mode="w",
            newline="",
        ) as csvfile:
            fieldnames = fake_transaction("X", "Y").keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transactions)
            app.storage.general["raw_transactions"] = len(transactions)

    except Exception as error:
        ui.notify(error, type='warning')

    logger.info("%d transactions created", count)
    ui.notify(f"{count} transactions written to file {filepath}", type='positive')


async def create_customers(count: int = 200):
    try:
        # customers
        customers = []
        csvfile = f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv"

        if os.path.isfile(csvfile):
            logger.info("Existing customers found, appending")
            with open(csvfile, "r", newline='') as existingfile:
                reader = csv.DictReader(existingfile)
                customers += [row for row in reader]
            logger.info("%d customers imported", len(customers))

        for _ in range(count):
            customers.append(fake_customer())

        with open(csvfile, "w", newline='') as csvfile:
            fieldnames = fake_customer().keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(customers)
            app.storage.general["raw_customers"] = len(customers)

    except Exception as error:
        logger.warning(error)
        return False

    logger.info("%d customers created", count)
    ui.notify(f"{count} customers created")

# NOT USED
def create_csv_files():
    """
    Create customers and transactions CSV files with randomly generated data
    """

    number_of_customers = 200
    number_of_transactions = 1_000

    # return if files already exist
    if os.path.isfile(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv") and os.path.isfile(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv"):
        ui.notify("Files exist, skipping...")
        app.storage.general["raw_transactions"] = number_of_transactions
        app.storage.general["raw_customers"] = number_of_customers
        return

    try:
        # customers
        customers = []
        for _ in range(number_of_customers):
            customers.append(fake_customer())

        with open(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv", "w", newline='') as csvfile:
            fieldnames = fake_customer().keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(customers)
            app.storage.general["raw_customers"] = len(customers)

        # transactions
        transactions = []
        for _ in range(number_of_transactions):
            # generate transaction with randomly selected sender and reciever accounts
            sender = customers[random.randrange(number_of_customers)]['account_number']
            receiver = customers[random.randrange(number_of_customers)]['account_number']
            transactions.append(fake_transaction(sender, receiver))

        with open(f"{MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv", "w", newline='') as csvfile:
            fieldnames = fake_transaction("X", "Y").keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transactions)
            app.storage.general["raw_transactions"] = len(transactions)

        ui.notify(f"Created files with {len(customers)} customers and {len(transactions)} transactions", type='positive')
        return True

    except Exception as error:
        logger.warning(error)
        return False


async def peek_mocked_customers():
    await run_command_with_dialog(f"head {MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv")


async def peek_mocked_transactions():
    await run_command_with_dialog(f"head {MOUNT_PATH}/{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.json")


async def sample_transactions():
    txlist = await get_new_transactions(10)
    if txlist is None:
        ui.notify("No transaction returned.", type='warning')

    with ui.dialog().props("full-width") as dialog, ui.card().classes(
        "relative"
    ):
        ui.button(icon="close", on_click=dialog.close).props(
            "flat round dense"
        ).classes("absolute right-2 top-2")
        l = ui.log().classes("m-6")
        for t in txlist: l.push(t)

    dialog.open()

async def publish_transactions(count: int = 10):
    """
    Publish transactions from csv file into the topic
    """

    stream_path = f"{BASEDIR}/{STREAM_INCOMING}"

    # count = 0

    # return if stream not created
    if not os.path.lexists(f"{MOUNT_PATH}/{get_cluster_name()}{stream_path}"):
        ui.notify(f"Stream not created {stream_path}", type="warning")
        return

    try:
        for transaction in await get_new_transactions(count):
            if await run.io_bound(streams.produce, stream_path, TOPIC_TRANSACTIONS, json.dumps(transaction)):
                logger.info("Sent %s", transaction["_id"])
                # add delay
                # await asyncio.sleep(0.2)
            else:
                logger.warning("Failed to send transaction to %s", TOPIC_TRANSACTIONS)

    except Exception as error:
        logger.warning("Cannot read transactions: %s", error)
        ui.notify(error, type='negative')
        return

    ui.notify(f"Published {count} messages into {TOPIC_TRANSACTIONS}", type='positive')


async def dummy_fraud_score():
    """Return a random percentile with adding a delay to simulate querying to an AI model"""

    # add delay
    await asyncio.sleep(0.001)

    # respond with a random probability, using string to avoid OJAI conversion to this \"score\": {\"$numberLong\": 46}}
    return str(random.randint(0, 100))


async def upload_to_s3(file: str):
    """ Upload the file to the external S3 bucket
    :param file str: full path for the file to upload
    """

    from minio import Minio

    bucket_name = DATA_PRODUCT

    try:
        client = Minio(
            endpoint=app.storage.general.get('S3_SERVER',"localhost"),
            access_key=app.storage.general["S3_ACCESS_KEY"],
            secret_key=app.storage.general["S3_SECRET_KEY"],
            secure=False,
        )

        # Make the bucket if it doesn't exist.
        found = client.bucket_exists(bucket_name)
        if not found:
            client.make_bucket(bucket_name)
            logger.info("Created bucket %s", bucket_name)
        else:
            logger.info("Bucket %s already exists", bucket_name)

        # Upload the file, renaming it in the process
        client.fput_object(
            bucket_name,
            os.path.basename(file),
            file,
        )

    except Exception as error:
        logger.warning(error)
        return False

    return True