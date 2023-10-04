# Databricks notebook source
from pyspark.sql.functions import to_date,col,lit

# COMMAND ----------

dataset = "wbs"
bronze_location = f"s3a://retail-bucket-s3/bronze/{dataset}"
silver_location = f"s3a://retail-bucket-s3/silver/{dataset}"
checkpoint_location = f"s3a://retail-bucket-s3/checkpoint/silver/{dataset}"

df = spark.readStream.format("delta").load(bronze_location)

df = (
        df.withColumn(
            "ReceivedDate", 
            to_date(col("ReceivedDate"), "MMyyyy")
        )
        .withColumn(
            "VendorShipDate", 
            to_date(col("VendorShipDate"), "MMyyyy")
        )
        .withColumn("SOURCE_FLAG", lit("WBS"))
        .withColumn("VendorZip", col("VendorZip").cast("long"))
    )

query = (
    df.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint_location)
   .start(silver_location)
)

query.awaitTermination()

# COMMAND ----------


