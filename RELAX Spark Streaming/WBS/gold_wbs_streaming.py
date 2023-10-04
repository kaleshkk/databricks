# Databricks notebook source
dataset = "wbs"
silver_location = f"s3a://retail-bucket-s3/silver/{dataset}"
gold_location = f"s3a://retail-bucket-s3/gold/po_header"
checkpoint_location = f"s3a://retail-bucket-s3/checkpoint/gold/{dataset}"

df = spark.readStream.format("delta").load(silver_location)
df = df.selectExpr("PONumber", "VendorAcctNumber", "BuyerCode", "ReceivedDate", "VendorInvNumber", "WarehouseOfVendor", "VendorShipDate", "ShippedPallets", "ShippedCubes", "ShippedWeight", "ShippedCases", "VendorName", "VendorAddress", "VendorCity", "VendorState", "VendorZip", "VendorRoute", "SOURCE_FLAG")

query = (
    df.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint_location)
   .start(gold_location)
)

query.awaitTermination()

# COMMAND ----------


