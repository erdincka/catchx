import logging

logger = logging.getLogger("streams")


def produce(stream: str, topic: str, message: str) -> bool:
    """Publish a single message. Prefer produce_many for more than one."""
    return produce_many(stream, topic, [message]) == 1


def produce_many(stream: str, topic: str, messages: list) -> int:
    """Publish a batch through one producer and return how many were accepted.

    One producer and a single flush for the whole batch: creating a producer
    and flushing per message turned a few hundred rows into several seconds of
    connection churn.
    """
    from confluent_kafka import Producer, KafkaException

    producer = Producer({"streams.producer.default.stream": stream})
    accepted = 0

    for message in messages:
        try:
            producer.produce(topic, message.encode("utf-8"))
            accepted += 1
        except BufferError:
            # Local queue full — drain and retry this one message.
            producer.flush()
            try:
                producer.produce(topic, message.encode("utf-8"))
                accepted += 1
            except Exception as error:
                logger.warning("Produce retry failed: %s", error)
        except KafkaException as error:
            logger.warning("Kafka produce error: %s", error)
        except Exception as error:
            logger.warning("Produce error: %s", error)

    try:
        producer.flush()
    except Exception as error:
        logger.warning("Producer flush error: %s", error)

    return accepted


def consume(stream: str, topic: str, consumer_group: str):
    """Drain a topic, committing as we read. Prefer consume_batch for ingestion."""
    return consume_batch(stream, topic, consumer_group, sink=None)[0]


def consume_batch(stream: str, topic: str, consumer_group: str, sink=None):
    """Drain a topic and commit only once `sink` reports the records are stored.

    Auto-commit moved the offset as soon as a message was read, so a failed
    write lost those messages permanently — the stream had already forgotten
    them and the only way back was to republish. Holding the commit until the
    sink succeeds makes a failed ingest retryable.

    `sink` is a synchronous callable taking the decoded records and returning
    True on success. It runs on this thread, which is already a worker thread.
    Returns (records, stored).
    """
    from confluent_kafka import Consumer, KafkaException

    conf = {
        "group.id": consumer_group,
        "default.topic.config": {"auto.offset.reset": "earliest"},
        "enable.auto.commit": False,
        "streams.consumer.default.stream": stream,
    }

    consumer = Consumer(conf)
    records = []

    try:
        consumer.subscribe([topic])

        while True:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                break
            if msg.error():
                logger.warning("Consumer error: %s", msg.error())
                break
            records.append(msg.value().decode("utf-8"))

        if not records:
            return [], True

        stored = True
        if sink is not None:
            try:
                stored = bool(sink(records))
            except Exception as error:
                logger.warning("Sink failed, not committing offsets: %s", error)
                stored = False

        if stored:
            consumer.commit(asynchronous=False)
        else:
            logger.warning(
                "Leaving %d messages uncommitted so the ingest can be retried", len(records)
            )

        return records, stored

    except KafkaException as error:
        logger.warning("Kafka consume error: %s", error)
        return [], False

    except Exception as error:
        logger.warning("Consume error: %s", error)
        return [], False

    finally:
        try:
            consumer.close()
        except Exception:
            pass
