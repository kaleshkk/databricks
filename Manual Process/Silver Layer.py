# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### sales_orders_cleaned

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sales_order = spark.sql("SELECT * FROM global_temp.sales_order")
customer = spark.sql("SELECT * FROM global_temp.customer")

# Join sales_order and customer DataFrames on customer_id
sales_orders_cleaned = sales_order.join(customer, sales_order.customer_id == customer.customer_id)

# Select specific columns and apply transformations
sales_orders_cleaned = sales_orders_cleaned.select(
    sales_order.customer_id,
    "customer_name",
    col("sales").cast("int").alias("sales"),
    from_unixtime(col("order_date").cast("long")).alias("order_date"),
    "order_number",
    "unit_price",
    "state"
)

# Show the result
sales_orders_cleaned.show(5)
sales_orders_cleaned.createOrReplaceGlobalTempView("sales_orders_cleaned")


# COMMAND ----------


