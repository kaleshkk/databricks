# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## Declare Gold Table

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sales_orders_cleaned = spark.sql("SELECT * FROM global_temp.sales_orders_cleaned")
# Filter the DataFrame for orders in 'NY'
sales_order_in_ny = sales_orders_cleaned.filter(sales_orders_cleaned.state == 'NY')

# Select the required columns
sales_order_in_ny = sales_order_in_ny.select(
    "order_number",
    col("sales").cast("int"),
    "unit_price",
    "customer_id",
    "order_date",
    "state"
)

# Group by columns and calculate sums
sales_order_in_ny = sales_order_in_ny.groupBy(
    "order_number",
    "order_date",
    "state",
    "customer_id"
).agg(
    sum("sales").alias("total_sales"),
    sum("unit_price").alias("total_sales_amt")
)

# Show the result
sales_order_in_ny.show()
sales_order_in_ny.createOrReplaceGlobalTempView("sales_order_in_ny")


# COMMAND ----------


