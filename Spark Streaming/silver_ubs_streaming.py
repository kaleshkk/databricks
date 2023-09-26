# Databricks notebook source
dataset = "ubs/orders"
bronze_location = f"s3a://retail-bucket-s3/bronze/{dataset}"
silver_location = f"s3a://retail-bucket-s3/silver/{dataset}"
checkpoint_location = f"s3a://retail-bucket-s3/checkpoint/silver/{dataset}"

df = spark.readStream.format("delta").load(bronze_location)

query = (
    df.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint_location)
   .start(silver_location)
)

query.awaitTermination()

# COMMAND ----------


