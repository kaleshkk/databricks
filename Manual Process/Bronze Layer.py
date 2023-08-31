# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### sales_orders_raw

# COMMAND ----------

s3_bucket = "retail-bucket-s3"
s3_file_path = "/orders/*"

# Read the CSV file from S3 into a DataFrame
sales_order_df = spark.read.csv(f"s3a://{s3_bucket}/{s3_file_path}", header=True, inferSchema=True)
sales_order_df.show(5)
sales_order_df.createOrReplaceGlobalTempView("sales_order")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### customers

# COMMAND ----------

s3_bucket = "retail-bucket-s3"
s3_file_path = "customers/*"

# Read the CSV file from S3 into a DataFrame
customer_df = spark.read.csv(f"s3a://{s3_bucket}/{s3_file_path}", header=True, inferSchema=True)
customer_df.show(5)
customer_df.createOrReplaceGlobalTempView("customer")

# COMMAND ----------


