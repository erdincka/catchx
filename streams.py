from common import *

logger = logging.getLogger("streams")


def produce(stream: str, topic: str, message: str):
    from confluent_kafka import Producer

    logger.debug("Stream path: %s", stream)
    p = Producer({"streams.producer.default.stream": stream})

    try:
        logger.info("sending message: %s", message)
        p.produce(topic, message.encode("utf-8"))

    except Exception as error:
        logger.warning(error)
        ui.notify(error, type='warning')
        return False

    finally:
        p.flush()

    return True


def consume(stream: str, topic: str, consumer_group: str):
    from confluent_kafka import Consumer, KafkaError

    logger.info("Stream: %s Topic: %s", stream, topic)

    consumer = Consumer(
        {"group.id": consumer_group, "default.topic.config": {"auto.offset.reset": "earliest"}}
    )

    try:

        consumer.subscribe([f"{stream}:{topic}"])

        while True:
            message = consumer.poll(timeout=MAX_POLL_TIME)

            if message is None: raise EOFError

            if not message.error(): yield message.value().decode("utf-8")

            elif message.error().code() == KafkaError._PARTITION_EOF:
                logger.info("No more messages in topic: %s", topic)
                # ui.notify(f"No more messages in {topic}")
                raise EOFError
            # silently ignore other errors
            else: logger.warning(message.error())

    except Exception as error:
        logger.warning(error)

    finally:
        consumer.close()
