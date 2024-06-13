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

        with open(f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['tables']['customers']}.csv", "w", newline='') as csvfile:
            fieldnames = fake_customer().keys()
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

        with open(f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['tables']['transactions']}.csv", "w", newline='') as csvfile:
            fieldnames = fake_transaction("X", "Y").keys()
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transactions)

        ui.notify(f"Created files with {len(customers)} customers and {len(transactions)} transactions", type='positive')
        return True
    
    except Exception as error:
        logger.warning(error)
        return False


async def peek_mocked_data():
    await run_command_with_dialog(f"tail /edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['tables']['customers']}.csv /edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['tables']['transactions']}.csv")


async def publish_transactions(limit: int = 10):
    """
    Publish transactions from csv file into the topic
    """    

    stream_path = f"{DATA_DOMAIN['basedir']}/{DATA_DOMAIN['streams']['incoming']}"

    count = 0
    
    # return if stream not created
    if not os.path.lexists(f"/edfs/{get_cluster_name()}{stream_path}"):
        ui.notify(f"Stream not created {stream_path}", type="warning")
        return 

    try:
        with open(f"/edfs/{get_cluster_name()}{DATA_DOMAIN['basedir']}/transactions.csv", "r", newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            for transaction in csv_reader:
                # only publish randomly selected transactions, and only 50 of them by default
                if count == limit: break
                if random.randrange(10) < 3: # ~30% to be selected randomly

                    if await run.io_bound(streams.produce, stream_path, DATA_DOMAIN['topics']['transactions'], json.dumps(transaction)):
                        logger.debug("Sent %s", transaction["_id"])
                        # add delay
                        # await asyncio.sleep(0.2)
                    else:
                        logger.warning("Failed to send transaction to %s", DATA_DOMAIN['topics']['transactions'])

                    count += 1

                else: logger.debug("Skipped transaction: %s", transaction["_id"])

    except Exception as error:
        logger.warning("Cannot read transactions: %s", error)
        ui.notify(error, type='negative')
        return

    ui.notify(f"Published {count} messages into {DATA_DOMAIN['topics']['transactions']}", type='positive')
