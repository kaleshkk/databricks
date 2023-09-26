# Databricks notebook source
dataset = "ubs/orders"
silver_location = f"s3a://retail-bucket-s3/silver/{dataset}"
gold_location = f"s3a://retail-bucket-s3/gold/{dataset}"
checkpoint_location = f"s3a://retail-bucket-s3/checkpoint/gold/{dataset}"

df = spark.readStream.format("delta").load(silver_location)

query = (
    df.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint_location)
   .start(gold_location)
)

query.awaitTermination()

# COMMAND ----------


