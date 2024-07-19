#!/usr/bin/env bash

SPARK_HOME=/opt/mapr/spark/spark-3.3.3

  # --deploy-mode cluster \
  # --total-executor-cores 2 \
$SPARK_HOME/bin/pyspark \
  --master spark://vm35.ez.win.lab:7077 \
  --jars /opt/mapr/spark/spark-3.3.3/jars \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.2 \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 1G \
  --driver-cores 1 \
  --driver-memory 1G \
  --conf spark.driver.host=10.1.1.110 \
  --conf spark.driver.bindAddress=172.17.0.2 \
  --conf spark.driver.port=51400 \
  --conf spark.driver.blockManagerPort=51500 \
  --conf spark.executor.cores=1 \
  --conf spark.cores.max=2 \
  --conf spark.executor.memory=1g \
  --conf spark.driver.cores=1 \
  --conf spark.driver.memory=1g \
  --py-files sparking.py
  # --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  # --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  # --conf spark.sql.catalog.spark_catalog.type=hive \
  # --conf spark.sql.catalog.spark_catalog.default-namespace=default \
  # --conf spark.sql.catalog.fraud=org.apache.iceberg.spark.SparkCatalog \
  # --conf spark.sql.catalog.fraud.type=hadoop \
  # --conf spark.sql.catalog.fraud.warehouse=/app/iceberg \
  # --conf spark.sql.parquet.writeLegacyFormat=true \
  # --conf spark.sql.legacy.pathOptionBehavior.enabled=true \