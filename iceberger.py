import logging
from time import sleep
import pyarrow.parquet as pq
import pyarrow.compute as pc
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyarrow import fs
import json
import pandas as pd
from pyarrow import csv
from kafka import KafkaProducer
from kafka.errors import KafkaError
from pyiceberg import Iceberg, Table

from helpers import DEMO, get_uuid_key

logger = logging.getLogger()

def stock_producer():
    producer = KafkaProducer(key_serializer=str.encode, value_serializer=lambda v: json.dumps(v).encode('ascii'),bootstrap_servers='kafka:9092',retries=3)

    tablename = DEMO['table']
    schemaname = "transactions" 
    local_data_dir = "/tmp/stocks/"

    from pyiceberg.catalog.sql import SqlCatalog
    warehouse_path = "/tmp/warehouse"
    catalog = SqlCatalog(
        "docs",
        **{
            "uri": f"sqlite:///{warehouse_path}/iceberg_catalog.db",
            "warehouse": f"file://{warehouse_path}",
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )

    rowCounter = 0
    isList = []

    while rowCounter >= 0:
        uuid_key = get_uuid_key()
        try:
            row = {'uuid': uuid_key, 'stockname': "DUMBDATA" }
            producer.send(tablename, key=uuid_key, value=row)
            producer.flush()
        except Exception as error:
            logger.warning(error)

        logger.debug(str(rowCounter) + " " + str(row))
        isList.append(row)
        rowCounter = rowCounter + 1

        if ( rowCounter >= 1000 ):
            rowCounter = 0
            
            ## build PyArrow table from python list
            df = pa.Table.from_pylist(isList)
            #### Write to Apache Iceberg on Minio (S3)
            ### - only create it for new table
            table = None
            try:
                table = catalog.create_table(
                    f'{schemaname}.{tablename}',
                    schema=df.schema,
                    location=s3location,
                )
            except:
                print("Table exists, append " + tablename)    
                table = catalog.load_table(f'{schemaname}.{tablename}')

            ### Write table to Iceberg/Minio
            table.append(df)
            isList = []
            df = None 

        sleep(0.05)
            
    producer.close()

def write_iceberg():
    
    # Replace this with your project-specific code
    project = "your_iceberg_project"
    namespace = "your_iceberg_namespace"
    table = "your_iceberg_table"
    filepath = "/opt/demo/data/parquet/file.parquet"

    iceberg = Iceberg()
    iceberg.init(project=project, namespace=namespace)

    # Create a new table if it doesn't exist or update the existing one
    if not iceberg.tables.exists(table):
        table_spec = Table.from_path(filepath).schema("my_schema")
        iceberg.create_table(table, table_spec)
    else:
        table = iceberg.get_table(table)

    # Read the parquet file and update the metadata of the existing table
    data = iceberg.tables.read(table, read_data=False)
    new_metadata = data[-1].metadata + {"some_key": "some_value"}
    iceberg.update_table(table, new_metadata)