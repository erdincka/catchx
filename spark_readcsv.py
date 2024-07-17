from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("Spark ETL")
    .config(
        "spark.jars.packages",
    )
    .config("org.apache.iceberg:iceberg-spark-runtime-3.3_2.12-1.4.2")
    .config("spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type=hive")
    .config("spark.sql.catalog.fraud = org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.fraud.type = hadoop")
    .config("spark.sql.catalog.fraud.warehouse = /app/iceberg")
    .config("spark.sql.legacy.pathOptionBehavior.enabled=true")
    .getOrCreate()
)

df = spark.read.option("header", True).csv("/mapr/fraud/app/customers.csv")

print(df.show())

df.writeTo("bronze.customers").append()

# spark.read.table("bronze.customers")
