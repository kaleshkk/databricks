# Databricks notebook source
# MAGIC %sql
# MAGIC GENERATE symlink_format_manifest FOR TABLE DELTA.`s3://retail-bucket-s3/gold/ubs/orders`

# COMMAND ----------

df = spark.read.format("delta").load("s3a://retail-bucket-s3/silver/ubs/orders")
df.count()

# COMMAND ----------


