import csv
import inspect
import random
import uuid
from faker import Faker

from nicegui import run
from helpers import *
import streams

fake = Faker(["en_GB", "en_US", "de_DE", "ja_JP", "en_IN"])


def fake_customer():
    profile = fake.simple_profile()
    # remove newline from address field
    profile['address'] = profile['address'].replace('\n', ' ')
    return {
        "id": uuid.uuid4().hex,
        **profile,
        "account_number": fake.iban(),
        "country": fake.current_country_code()
    }


def fake_transaction(sender: str, receiver: str):
    return {
        "id": uuid.uuid4().hex,
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

    number_of_customers = 1_000
    number_of_transactions = 10_000

    # customers
    customers = []
    for _ in range(number_of_customers):
        customers.append(fake_customer())

    with open(f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{DEMO['tables']['customers']}.csv", "w", newline='') as csvfile:
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

    with open(f"/mapr/{get_cluster_name()}{DEMO['basedir']}/{DEMO['tables']['transactions']}.csv", "w", newline='') as csvfile:
        fieldnames = fake_transaction("X", "Y").keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(transactions)

    ui.notify(f"Created files with {len(customers)} customers and {len(transactions)} transactions", type='positive')


async def peek_demo_data():
    await run_command_with_dialog(f"head /mapr/{get_cluster_name()}{DEMO['basedir']}/{DEMO['tables']['customers']}.csv /mapr/{get_cluster_name()}{DEMO['basedir']}/{DEMO['tables']['transactions']}.csv")


async def publish_transactions():
    stream_path = f"{DEMO['basedir']}/{DEMO['stream']}"
    count = 0
    
    try:
        with open(f"/mapr/{get_cluster_name()}{DEMO['basedir']}/transactions.csv", "r", newline='') as csvfile:
            csv_reader = csv.DictReader(csvfile)

            for transaction in csv_reader:
                # only publish randomly selected transactions, limited to 50
                if count == 50: break
                if random.randrange(10) < 3: # ~30% to be selected randomly

                    if await run.io_bound(streams.produce, stream_path, DEMO['topic'], json.dumps(transaction)):
                        logger.debug("Sent %s", transaction["id"])
                        # add delay
                        # await asyncio.sleep(0.2)
                    else:
                        logger.warning("Failed to send transaction to %s", DEMO['topic'])

                    count += 1

                else: logger.debug("Skipped transaction: %s", transaction["id"])

    except Exception as error:
        logger.warning("Cannot read transactions: %s", error)

    finally:
        ui.notify(f"Published {count} messages into {DEMO['topic']}", type='positive')
