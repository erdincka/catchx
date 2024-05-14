from datetime import datetime
import logging
from urllib.parse import quote, urlparse
from nicegui import app
import json


from helpers import DEMO, dt_from_iso
import restrunner

logger = logging.getLogger()


# def push_to_monitorDB(stats_json_str: str):
#     stats_json = json.loads(stats_json_str)
#     stats_json["_id"] = str(stats_json["ts"])
#     # logger.debug(f"_ID: {stats_json['_id']}")

#     ts = stats_json["ts"] / 1_000_000
#     logger.debug(
#         "Submit metrics at %s",
#         datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"),
#     )

#     response = restrunner.dagpost(
#         "/api/v2/table/%2Fapps%2Fezshowmonitor", json_obj=stats_json
#     )

#     # logger.debug("MONITOR PUSH RESPONSE: %s", response)
#     return response


# def create_monitoring_table(table_path):
#     # create monitoring table if not exist
#     path = quote(table_path, safe="")
#     response = restrunner.dagput(f"/api/v2/table/{path}")
#     if isinstance(response, Exception):
#         if response.response.status_code != 409:
#             logger.warning(f"MonitorDB creation FAILED: {response}")


# def kafka_publish(host: str, username: str, password: str, topic: str, messages: list):
#     from confluent_kafka import Producer

#     # logger.info("Sending %d messages to %s topic", len(messages), topic)
#     logger.info(f"Sending {len(messages)} messages to {topic} topic")

#     conf = {
#         "bootstrap.servers": f"{host}:9092",
#         "security.protocol": "SASL_PLAINTEXT",
#         "sasl.mechanism": "PLAIN",
#         # "enable.ssl.certificate.verification": "false", # if needed with SASL_SSL
#         "sasl.username": username,
#         "sasl.password": password,
#         "client.id": "ezshow",
#         "queue.buffering.max.ms": 100,
#         "statistics.interval.ms": 1000,
#         "stats_cb": push_to_monitorDB,
#         "socket.timeout.ms": 1000,
#         "message.send.max.retries": 2,
#         "default.topic.config": {
#             "message.timeout.ms": 1000,
#             "request.timeout.ms": 1000,
#         },
#     }

#     producer = Producer(**conf)

#     # process messages that are already in the queue
#     producer.flush()

#     # update progress on each poll
#     def delivery_report(err, msg):
#         if err is None:
#             app.storage.general["demo"]["counting"] += 1
#             # logger.debug(f"KAFKA DELIVERY: {msg.value().decode()}")
#         else:
#             logger.debug(f"{err}")

#     for msg in messages:
#         # process the queue
#         producer.poll(0)

#         # send the message
#         producer.produce(
#             topic, key="id", value=json.dumps(msg).encode(), on_delivery=delivery_report
#         )

#         # logger.debug(f"KAFKA PRODUCED {msg}")

#     # process the remaining messages in the queue
#     producer.flush()


# def kafka_consume(host: str, username: str, password: str, topic: str):
#     from confluent_kafka import Consumer, KafkaError, KafkaException

#     # logger.info(f"Getting messages from {topic}")
#     logger.info(f"Getting messages from %s", topic)

#     conf = {
#         "bootstrap.servers": f"{host}:9092",
#         "group.id": "ezshow",
#         "auto.offset.reset": "earliest",
#         "security.protocol": "SASL_PLAINTEXT",
#         "sasl.mechanism": "PLAIN",
#         # "enable.ssl.certificate.verification": "false", # if needed with SASL_SSL
#         "sasl.username": username,
#         "sasl.password": password,
#         "statistics.interval.ms": 500,
#         "stats_cb": push_to_monitorDB,  # collect statistics
#     }

#     c = Consumer(conf)

#     # Subscribe to topics
#     c.subscribe([topic])

#     # return the messages, log the errors
#     try:
#         while True:
#             msg = c.poll(timeout=1.0)
#             if msg is None:  # we break when there are no more messages in the topic,
#                 # in real life, you would keep waiting even when there are no messages
#                 logger.debug(f"Nothing left in the topic {topic} for consumption")
#                 # continue
#                 break
#             if msg.error():
#                 if msg.error().code() == KafkaError._PARTITION_EOF:
#                     # End of partition event
#                     logging.info(
#                         "%% %s [%d] reached end at offset %d\n"
#                         % (msg.topic(), msg.partition(), msg.offset())
#                     )
#                 elif msg.error():
#                     raise KafkaException(msg.error())
#                 logger.warning(msg.error())
#                 yield False  # provide feedback on failed message processing
#             else:
#                 logger.debug("Consumer sending message %s", msg.value().decode())
#                 yield msg.value().decode()

#     except Exception as error:
#         logger.warning("KAFKA CONSUME ERROR %s", error)

#     finally:
#         c.close()


# def stream_publish(stream: str, topic: str, messages: list):
#     topic_path = quote(f"{stream}:{topic}", safe="")
#     for msg in messages:
#         response = restrunner.kafkapost(
#             path=f"/topics/{topic_path}", data={"records": [{"value": msg}]}
#         )
#         if isinstance(response, Exception):
#             logger.info(f"Message failed to push: {response}")
#         else:
#             # logger.debug(f"STREAM PUSH {msg}")
#             app.storage.general["demo"]["counting"] += 1

#     logger.info("Sent %s messages to %s:%s", len(messages), stream, topic)


# def stream_consume(stream: str, topic: str):
#     topic_path = f"{stream}:{topic}"

#     result = []

#     try:
#         # Create consumer instance
#         response = restrunner.kafkapost(
#             f"/consumers/{topic}_cg",
#             data={
#                 "name": f"{topic}_ci",
#                 "format": "json",
#                 "auto.offset.reset": "earliest",
#                 # "fetch.max.wait.ms": 1000,
#                 # "consumer.request.timeout.ms": "500",
#             },
#         )

#         if response is None:
#             return result

#         ci = response.json()
#         ci_path = urlparse(ci["base_uri"]).path

#         # subscribe to consumer
#         restrunner.kafkapost(
#             f"{ci_path}/subscription", data={"topics": [topic_path]}
#         )
#         # No content in response

#         # get records
#         records = restrunner.kafkaget(f"{ci_path}/records")
#         if records.ok:
#             for message in records.json():
#                 # logger.debug("CONSUMER GOT MESSAGE: %s", message)
#                 result.append(message["value"])

#     except Exception as error:
#         logger.warning("STREAM CONSUMER ERROR %s", error)

#     finally:
#         # Unsubscribe from consumer instance
#         restrunner.kafkadelete(f"/consumers/{topic}_cg/instances/{topic}_ci")
#         return result


# MONITOR FUNCTIONS SHOULD RETURN
# "name": dict_item["type"],
# "time": dict_item["time"],
# "values": [
#     {"Tx": dict_item["tx"]},
#     {"Rx": dict_item["rx"]},
# ],


def topic_stats(topic: str):
    stream_path = f"{DEMO['volume']}/{DEMO['stream']}"

    try:
        response = restrunner.get(
            f"/rest/stream/topic/info?path={stream_path}&topic={topic}"
        )
        if not response:
            logger.warning(f"Failed to get topic stats for {topic}: {response}")

        else:
            metrics = response.json()
            if not metrics["status"] == "ERROR":
                logger.debug("TOPIC STAT %s", metrics)

                series = []
                for m in metrics["data"]:
                    series.append(
                        {"publishedMsgs": m["maxoffset"] + 1}
                    )  # interestingly, maxoffset starts from -1
                    series.append(
                        {"consumedMsgs": m["minoffsetacrossconsumers"]}
                    )  # this metric starts at 0
                    series.append(
                        {
                            "latestAgo(s)": (
                                datetime.now().astimezone()
                                - dt_from_iso(m["maxtimestamp"])
                            ).total_seconds()
                        }
                    )
                    series.append(
                        {
                            "consumerLag(s)": (
                                dt_from_iso(m["maxtimestamp"])
                                - dt_from_iso(m["mintimestampacrossconsumers"])
                            ).total_seconds()
                        }
                    )
                # logger.info("Metrics %s", series)
                return {
                    "name": topic,
                    "time": datetime.fromtimestamp(
                        metrics["timestamp"] / (10**3)
                    ).strftime("%H:%M:%S"),
                    "values": series,
                }
            else:
                logger.warn("Topic stat query error %s", metrics["errors"])

    except Exception as error:
        logger.warning("Topic stat request error %s", error)


def consumer_stats(topic: str):
    stream_path = f"{DEMO['volume']}/{DEMO['stream']}"

    try:
        response = restrunner.get(
            f"/rest/stream/cursor/list?path={stream_path}&topic={topic}"
        )
        if isinstance(response, Exception):
            logger.warning(f"Failed to get consumer stats for {topic}: {response}")
        else:
            metrics = response.json()

            if not metrics["status"] == "ERROR":
                logger.debug("CONSUMER STAT %s", metrics)
                series = []
                for m in metrics["data"]:
                    series.append(
                        {
                            f"{m['consumergroup']}_{m['partitionid']}_lag(s)": float(
                                m["consumerlagmillis"]
                            )
                            / 1000
                        }
                    )
                    series.append(
                        {
                            f"{m['consumergroup']}_{m['partitionid']}_offsetBehind": int(
                                m["produceroffset"]
                            ) + 1 # starting from -1 !!!
                            - int(m["committedoffset"])
                        }
                    )
                # logger.info("Metrics %s", series)
                return {
                    "name": "Consumer",
                    "time": datetime.fromtimestamp(
                        metrics["timestamp"] / (10**3)
                    ).strftime("%H:%M:%S"),
                    "values": series,
                }
            else:
                logger.warn("Consumer stat query error %s", metrics["errors"])

    except Exception as error:
        logger.warning("Consumer stat request error %s", error)


# Dummy function for informational steps
def noop():
    return True


# def get_jmx_metrics():
#     buffertimestamp = (
#         datetime.now() - timedelta(minutes=0)
#     ).timestamp() * 10**6  # convert to microseconds

#     # logger.debug(
#     #     "Getting metrics from %s",
#     #     datetime.utcfromtimestamp(buffertimestamp/1_000_000).strftime("%Y-%m-%d %H:%M:%S"),
#     # )

#     response = restrunner.dagget(
#         f"/api/v2/table/%2Fapps%2Fezshowmonitor?orderBy=ts&fromId={buffertimestamp}"
#     )

#     # logger.debug("MONITOR GET RESPONSE: %s", response)

#     if isinstance(response, Exception):
#         logger.warning("MONITOR GET FAILED: %s", response)
#         return

#     if response is not None:
#         docs = response.json().get("DocumentStream", list())
#         if len(docs) > 0:
#             # print(docs)
#             for dict_item in sorted(docs, key=lambda x: x['ts']):
#                 yield {
#                     "name": dict_item["type"],
#                     "time": datetime.fromtimestamp(dict_item["time"]).time(),
#                     "values": [
#                         {"name": "Messages", "value": dict_item["msg_cnt"]},
#                         # {"name": "Rx", "value": dict_item["rx"]},
#                     ],
#                 }
#                 # yield { "x_axis": dict_item['time'], "y_axis": [ dict_item['tx'], dict_item['rx'] ] }
#             ######

#             # write to kafta stream, get json, return specific metrics/fields

#             ######
#             # import pandas as pd
#             # df = pd.json_normalize(docs)

#             # # print(df['type'])
#             # # print(df['time'])
#             # # print(df['msg_cnt'])
#             # # print(df['topics.banking.partitions.0.txmsgs'])

#             # metrics = df.pivot(index="ts", columns=["type"], values="msg_cnt")
#             # print(metrics)
#             # yield metrics

