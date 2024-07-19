import logging
import os
import sys

from nicegui import app

# sys.path.append("/opt/mapr/spark/spark-3.3.3/python/lib/py4j-0.10.9.5-src.zip")
# sys.path.append("/opt/mapr/spark/spark-3.3.3/python/lib/pyspark.zip")

os.environ["SPARK_HOME"] = "/opt/mapr/spark/spark-3.3.3"

from pyspark.sql import SparkSession

# wget -O /opt/mapr/spark/spark-3.3.3/jars/iceberg-spark-runtime-3.3_2.12-1.4.2.jar https://search.maven.org/remotecontent?filepath=org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.4.2/iceberg-spark-runtime-3.3_2.12-1.4.2.jar

logger = logging.getLogger("sparking")
logger.setLevel(logging.DEBUG)

spark = (
    SparkSession.builder.appName("Spark ETL")
    .config(
        "spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2"
    )
    .config("spark.executor.cores=1")
    .config("spark.driver.cores=1")
    .config("spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.defaultCatalog=fraud")
    .config("spark.sql.catalog.fraud=org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.fraud.type=hive")
    .config("spark.sql.catalog.fraud.uri=thrift://localhost:9083")
    .config("spark.sql.catalog.fraud.default-namespace=bronze")
    .config("spark.sql.catalog.fraud.warehouse=/app/iceberg")
    .config("spark.sql.parquet.writeLegacyFormat=true")
    .config("spark.sql.legacy.pathOptionBehavior.enabled=true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("INFO")

df = spark.read.option("header", True).csv("/mapr/fraud/app/customers.csv")

# check the dataframe
df.show()


# def ingest(input_file: str):
#     try:
#         spark = SparkSession.builder.master(f"spark://{app.storage.general.get('cluster', 'localhost')}:7077").appName("ETL").getOrCreate()
#         spark.sparkContext.setLogLevel("ALL")
#         print(f"Spark context: {spark}")
#         # java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")
#         df = spark.read.csv(input_file, header=True, inferSchema=True)
#         print(df.count())

#     except Exception as error:
#         logger.warning("cannot use spark: %s", error)


# def sse_code():
#     # Define the source and destination Path
#     source_path = 's3a://truecorp-landingzone-rangsit/'
#     dest_path = 's3a://truecorp-raw-rangsit/FN00002/'

#     spark = SparkSession.builder.master(f"spark://{app.storage.general.get('cluster', 'localhost')}:7077").appName("ETL").getOrCreate()
#     # Read CSV file into DataFrame

#     file_path='file:///mounts/shared-volume/shared/jars/cdr_f_mon_dtn.csv'
#     df = spark.read.csv(file_path, header=True, inferSchema=True)

#     # Convert DataFrame to list
#     unl_file_list = df.select('filename').rdd.flatMap(lambda x: x).collect()

#     # Change the default partition size to 4 times to decrease the number of partitions in order to process large file
#     spark.conf.set("spark.sql.files.maxPartitionBytes", str(128 * 4 * 1024 * 1024)+"b")

#     # Verify the partition size
#     partition_size = spark.conf.get("spark.sql.files.maxPartitionBytes").replace("b","")
#     print(f"Partition Size: {partition_size} in bytes and {int(partition_size) / 1024 / 1024} in MB")


#     # Save log entry to a log file with today's date in the filename
#     log_date=datetime.now().strftime("%Y%m%d")

#     ## Completed log file path and schema
#     completed_log_file_path = dest_path+f"completed_files_{log_date}"
#     print(completed_log_file_path)
#     completed_schema = StructType([
#             StructField("filename", StringType(), True),
#             StructField("file_rec_cnt", IntegerType(), True),
#             StructField("runtime", FloatType(), True)
#         ])

#     ## Completed error file path and schema
#     error_log_file_path = dest_path+f"error_files_{log_date}"
#     print(error_log_file_path)
#     error_schema = StructType([
#             StructField("filename", StringType(), True),
#             StructField("file_rec_cnt", IntegerType(), True),
#             StructField("error", StringType(), True)
#         ])

#     def process_unl_file(unl_file):
#         start_time = time.time()
#         print('before read data')
#         data_df = spark.read.csv(unl_file,sep="|").withColumn("file_name",  F.split(F.input_file_name(), '/')[5])
#         data_df.show(2)
#         print('before write parquet')
#         outfilename=Path(unl_file).stem
#         try:
#             data_df.write.parquet(dest_path+'data/'+outfilename,mode='overwrite')

#             # compute the execution time to read and write the input delimited file
#             exec_time = time.time() - start_time
#             # Create a log entry
#             print(exec_time)
#             row_count = data_df.count()
#             completed_row = [(unl_file , row_count, round(exec_time,5))]
#             print(completed_schema)

#             # Create a DataFrame for the completed row
#             completed_row_df = spark.createDataFrame(completed_row, schema=completed_schema)

#             # Write the row DataFrame to completed CSV in append mode
#             completed_row_df.write.csv(completed_log_file_path, mode="append",sep="|")
#         except Exception:
#             #print(err)
#             error_row = [(unl_file,0,'Error while writing the output file')]
#             print(error_row)

#             # Create a DataFrame for the error row
#             error_row_df = spark.createDataFrame(error_row, schema=error_schema)

#             # Write the row DataFrame to completed CSV in append mode
#             error_row_df.write.csv(error_log_file_path, mode="append",sep="|")
#             pass

#         return


if __name__ in ["__main__", "__mp_main" ]:
    pass
