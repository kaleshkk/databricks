# Databricks notebook source
from pyspark.sql.functions import to_date,col,lit

# COMMAND ----------

dataset = "ubs"
bronze_location = f"s3a://retail-bucket-s3/bronze/{dataset}"
silver_location = f"s3a://retail-bucket-s3/silver/{dataset}"
checkpoint_location = f"s3a://retail-bucket-s3/checkpoint/silver/{dataset}"

df = spark.readStream.format("delta").load(bronze_location)
vendor_df = spark.read.table("dev.default.mdm_vendor").drop("_rescued_data")

df = (
        df.withColumn(
            "RPOH_RCVDAT", 
            to_date(col("RPOH_RCVDAT"), "yyyyMMdd")
        ).withColumn(
            "RPOH_SHPDAT", 
            to_date(col("RPOH_SHPDAT"), "yyyyMMdd")
        )
        .withColumn("SOURCE_FLAG", lit("UBS"))
        .join(vendor_df, df.RPOH_VEN == vendor_df.VendorId)
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


