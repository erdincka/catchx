from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.iceberg.hooks.iceberg import IcebergHook

from datetime import datetime, timedelta
import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog


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
    "csv_to_iceberg",
    default_args=default_args,
    description="Read from local CSV and write to Apache Iceberg table",
    schedule_interval=timedelta(days=1),
)


# Function to read CSV file
def read_csv_file():
    # Path to your local CSV file
    csv_file_path = "/mapr/fraud/app/customers.csv"
    df = pd.read_csv(csv_file_path, header=0)
    return df.to_dict(orient="records")
    # return df


def get_catalog():
    """Return the catalog, create if not exists"""

    catalog = None

    try:
        from pyiceberg.catalog.sql import SqlCatalog

        catalog = SqlCatalog(
            "default",
            **{
                "uri": f"sqlite:///mapr/fraud/app/iceberg.db",
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
            },
        )

    except Exception as error:
        print("Iceberg Catalog error: %s", error)

    finally:
        return catalog


# Function to upload data to Iceberg table
def upload_to_iceberg(**context):
    records = context["task_instance"].xcom_pull(task_ids="read_csv")
    tier = "bronze"
    tablename = "customers"
    warehouse_path = f"/mapr/fraud/app/{tier}/{tablename}"

    # catalog = get_catalog()

    # if catalog is not None:
    #     # create namespace if missing
    #     if (tier,) not in catalog.list_namespaces():
    #         catalog.create_namespace(tier)

    #     table = None

    #     # Create table if missing
    #     try:
    #         table = catalog.create_table(
    #             f"{tier}.{tablename}",
    #             schema=pa.Table.from_pylist(records).schema,
    #             location=warehouse_path,
    #         )

    #     except:
    #         print("Table exists, appending to: " + tablename)
    #         table = catalog.load_table(f"{tier}.{tablename}")

    #     existing = table.scan().to_pandas()

    #     incoming = pd.DataFrame.from_dict(records)

    #     merged = pd.concat([existing, incoming]).drop_duplicates(subset="_id", keep="last")

    #     table.append(pa.Table.from_pandas(merged, preserve_index=False))

    #     return True

    # catalog not found
    # return False

    df = pd.DataFrame(records)

    # Load the Iceberg catalog
    catalog = load_catalog(
        "fraud_app",
        **{
            "uri": "sqlite:///mapr/fraud/app/iceberg.db",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )

    # Load or create the Iceberg table
    table_identifier = f"{tier}.{tablename}"
    if not catalog.table_exists(table_identifier):
        catalog.create_table(
            identifier=table_identifier,
            schema=pa.Table.from_pylist(records).schema,
            table_location=warehouse_path,
        )

    table = catalog.load_table(table_identifier)

    # Write data to the table
    # with table.new_append() as append:
    #     append.append_records(records)
    #     append.commit()

    existing = table.scan().to_pandas()

    incoming = pd.DataFrame.from_dict(records)

    merged = pd.concat([existing, incoming]).drop_duplicates(subset="_id", keep="last")

    table.append(pa.Table.from_pandas(merged, preserve_index=False))


# Define the tasks
read_csv_task = PythonOperator(
    task_id="read_csv",
    python_callable=read_csv_file,
    dag=dag,
)

bash_command = f"""
echo "Our token: {IcebergHook().get_token_macro()}"
echo "Also as an environment variable:"
env | grep TOKEN
"""

test_iceberg_task = BashOperator(
    task_id="with_iceberg_environment_variable",
    bash_command=bash_command,
    env={"TOKEN": IcebergHook().get_token_macro()},
)

upload_task = PythonOperator(
    task_id="upload_to_iceberg",
    python_callable=upload_to_iceberg,
    provide_context=True,
    dag=dag,
)

# Set up task dependencies
read_csv_task >> test_iceberg_task # >> upload_task
