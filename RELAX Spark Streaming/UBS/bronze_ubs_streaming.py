# Databricks notebook source
dataset = "ubs"
landing_location = f"s3a://retail-bucket-s3/landing/{dataset}/*"
schema_location = f"s3a://retail-bucket-s3/schema/{dataset}"
bronze_location = f"s3a://retail-bucket-s3/bronze/{dataset}"
checkpoint_location = f"s3a://retail-bucket-s3/checkpoint/bronze/{dataset}"

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", schema_location)
    .load(landing_location)
)
# df.show(2)

query = (
    df.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint_location)
   .start(bronze_location)
)

query.awaitTermination()

# COMMAND ----------


