#!/usr/bin/env bash

SPARK_HOME=/opt/mapr/spark/spark-3.3.3

$SPARK_HOME/bin/pyspark \
  --jars /opt/mapr/spark/spark-3.3.3/jars \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2 \
  --conf spark.driver.cores=2 \
  --conf spark.driver.memory=2g \
  --conf spark.executor.cores=2 \
  --conf spark.executor.memory=2g \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.spark_catalog.default-namespace=default \
  --conf spark.sql.catalog.fraud=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.fraud.type=hadoop \
  --conf spark.sql.catalog.fraud.warehouse=/app/iceberg \
  --conf spark.sql.parquet.writeLegacyFormat=true \
  --conf spark.sql.legacy.pathOptionBehavior.enabled=true
