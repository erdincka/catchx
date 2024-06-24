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
    profile = fake.simple_profile()
    # remove newline from address field
    profile['address'] = profile['address'].replace('\n', ' ')
    return {
        "_id": uuid.uuid4().hex,
        **profile,
        "account_number": fake.iban(),
        "county": fake.county(),
        "country_code": fake.current_country_code()
    }


def fake_transaction(sender: str, receiver: str):
    return {
        "_id": uuid.uuid4().hex,
        "sender_account": sender,
        "receiver_account": receiver,
        "amount": round(fake.pyint(0, 10_000), 2),
        "currency": fake.currency_code(),
        "transaction_date": fake.past_datetime(start_date="-12M").timestamp(),
    }


def create_csv_files():
    """
    Create customers and transactions CSV files with randomly generated data
    """

    number_of_customers = 200
    number_of_transactions = 1_000

    try:
        # customers
        customers = []
        for _ in range(number_of_customers):
            customers.append(fake_customer())

        with open(f"{MOUNT_PATH}{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv", "w", newline='') as csvfile:
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

        with open(f"{MOUNT_PATH}{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv", "w", newline='') as csvfile:
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


async def peek_mocked_data():
    await run_command_with_dialog(f"tail {MOUNT_PATH}{get_cluster_name()}{BASEDIR}/{TABLE_CUSTOMERS}.csv {MOUNT_PATH}{get_cluster_name()}{BASEDIR}/{TABLE_TRANSACTIONS}.csv")


async def publish_transactions(limit: int = 10):
    """
    Publish transactions from csv file into the topic
    """    

    stream_path = f"{BASEDIR}/{STREAM_INCOMING}"

    count = 0
    
    # return if stream not created
    if not os.path.lexists(f"{MOUNT_PATH}{get_cluster_name()}{stream_path}"):
        ui.notify(f"Stream not created {stream_path}", type="warning")
        return 

    try:
        with open(f"{MOUNT_PATH}{get_cluster_name()}{BASEDIR}/transactions.csv", "r", newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            for transaction in csv_reader:
                # only publish randomly selected transactions, and only 50 of them by default
                if count == limit: break
                if random.randrange(10) < 3: # ~30% to be selected randomly

                    if await run.io_bound(streams.produce, stream_path, TOPIC_TRANSACTIONS, json.dumps(transaction)):
                        logger.debug("Sent %s", transaction["_id"])
                        # add delay
                        # await asyncio.sleep(0.2)
                    else:
                        logger.warning("Failed to send transaction to %s", TOPIC_TRANSACTIONS)

                    count += 1

                else: logger.debug("Skipped transaction: %s", transaction["_id"])

    except Exception as error:
        logger.warning("Cannot read transactions: %s", error)
        ui.notify(error, type='negative')
        return

    ui.notify(f"Published {count} messages into {TOPIC_TRANSACTIONS}", type='positive')


async def dummy_fraud_score():
    """Return a random percentile with adding a delay to simulate querying to an AI model"""

    # add delay
    await asyncio.sleep(0.02)

    # respond with a random probability, using string to avoid OJAI conversion to this \"score\": {\"$numberLong\": 46}}
    return str(random.randint(0, 100))

