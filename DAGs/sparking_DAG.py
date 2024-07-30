from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator


with DAG(
    dag_id="spark_etl_csv_to_delta",
    default_args={
        "owner": "airflow",
        "start_date": datetime(2024, 7, 1),
        "retries": 10,
        "retry_delay": timedelta(minutes=1),
    },
    schedule_interval="0 1 * * *",
) as dag:
    # task_read_csv = PythonOperator(
    #     task_id="read_from_csv", python_callable=spark_etl
    # )

    task_read_csv = SparkSubmitOperator(
        task_id="read_from_csv",
        conn_id="spark",
        application="./dags/spark_readcsv.py",
        application_args=[""],
        total_executor_cores=1,
        executor_memory="1g",
        conf={},
        packages="",
        env_vars={
            "PATH": "/bin:/usr/bin:/usr/local/bin:/opt/mapr/spark/spark-3.3.3/bin/",
            "SPARK_HOME": "/opt/mapr/spark/spark-3.3.3/",
        },
    )

task_read_csv
