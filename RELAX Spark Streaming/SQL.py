# Databricks notebook source
# MAGIC %sql
# MAGIC GENERATE symlink_format_manifest FOR TABLE DELTA.`s3://retail-bucket-s3/gold/po_header`

# COMMAND ----------

df = spark.read.format("delta").load("s3://retail-bucket-s3/gold/po_header")
df.count()

# COMMAND ----------


