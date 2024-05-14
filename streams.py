import logging
from time import sleep

from helpers import MAX_POLL_TIME


logger = logging.getLogger()

def produce(stream: str, topic: str, record: str):
    from confluent_kafka import Producer

    p = Producer({"streams.producer.default.stream": stream})

    try:
        # logger.debug("pushing message: %s", record)
        p.produce(topic, record.encode("utf-8"))

    except Exception as error:
        logger.warning(error)
        return False
    
    finally:
        p.flush()

    return True


def consume(stream: str, topic: str):
    from confluent_kafka import Consumer, KafkaError

    consumer = Consumer(
        {"group.id": "demo_consumer_group", "default.topic.config": {"auto.offset.reset": "earliest"}}
    )

    consumer.subscribe([f"{stream}:{topic}"])

    try:
        while True:
            message = consumer.poll(timeout=MAX_POLL_TIME)

            if message is None: continue

            if not message.error(): yield message.value().decode("utf-8")

            elif message.error().code() == KafkaError._PARTITION_EOF:
                raise EOFError
            # silently ignore other errors
            else: logger.debug(message.error())

            # add delay
            # sleep(0.1)

    except Exception as error:
        logger.debug(error)

    finally:
        consumer.close()