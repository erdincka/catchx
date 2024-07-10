from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
import requests

# Define the default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 6, 24),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    "mapr_streams_to_binary_table",
    default_args=default_args,
    description="Read from MapR Streams and write to MapR Binary Table",
    schedule_interval=timedelta(minutes=10),
)


# Function to consume messages from MapR Streams
def consume_messages_from_stream():
    # Use the MapR Streams Python API to consume messages from the topic
    from mapr_streams import StreamSession

    session = StreamSession()
    consumer = session.consumer(
        topic="/path/to/stream:incoming_transactions",
        group_id="airflow_consumer_group",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
    )

    messages = []
    for message in consumer:
        msg = json.loads(message.value)
        messages.append(msg)
        # Break after consuming a batch of messages
        if len(messages) >= 100:
            break

    consumer.close()
    return messages


# Function to write messages to MapR Binary Table using REST API
def write_to_binary_table(**context):
    messages = context["task_instance"].xcom_pull(task_ids="consume_messages")

    headers = {"Content-Type": "application/json"}

    for msg in messages:
        response = requests.post(
            "http://<mapr-data-access-gateway-url>/rest/table/app/transactions",
            headers=headers,
            data=json.dumps(msg),
        )
        if response.status_code != 200:
            raise Exception(f"Failed to insert record: {response.text}")


# Define the tasks
consume_task = PythonOperator(
    task_id="consume_messages",
    python_callable=consume_messages_from_stream,
    dag=dag,
)

write_task = PythonOperator(
    task_id="write_to_binary_table",
    python_callable=write_to_binary_table,
    provide_context=True,
    dag=dag,
)

# Set up task dependencies
consume_task >> write_task
