import logging

logger = logging.getLogger("streams")


def produce(stream: str, topic: str, message: str) -> bool:
    from confluent_kafka import Producer, KafkaException

    conf = {"streams.producer.default.stream": stream}
    producer = Producer(conf)

    try:
        producer.produce(topic, message.encode("utf-8"))
        producer.flush()
        return True

    except KafkaException as error:
        logger.warning("Kafka produce error: %s", error)
        return False

    except Exception as error:
        logger.warning("Produce error: %s", error)
        return False


def consume(stream: str, topic: str, consumer_group: str):
    from confluent_kafka import Consumer, KafkaException

    conf = {
        "group.id": consumer_group,
        "default.topic.config": {"auto.offset.reset": "earliest"},
        "enable.auto.commit": True,
        "streams.consumer.default.stream": stream,
    }

    consumer = Consumer(conf)

    try:
        consumer.subscribe([topic])

        records = []
        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                break
            if msg.error():
                logger.warning("Consumer error: %s", msg.error())
                break
            records.append(msg.value().decode("utf-8"))

        consumer.close()
        return records

    except KafkaException as error:
        logger.warning("Kafka consume error: %s", error)
        return []

    except Exception as error:
        logger.warning("Consume error: %s", error)
        return []
