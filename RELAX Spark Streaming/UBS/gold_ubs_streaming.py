# Databricks notebook source
dataset = "ubs"
silver_location = f"s3a://retail-bucket-s3/silver/{dataset}"
gold_location = f"s3a://retail-bucket-s3/gold/po_header"
checkpoint_location = f"s3a://retail-bucket-s3/checkpoint/gold/{dataset}"

df = spark.readStream.format("delta").load(silver_location)
df = df.selectExpr("RPOH_NUM as PONumber", "RPOH_VEN as VendorAcctNumber", " RPOH_BUY as BuyerCode", "RPOH_RCVDAT as ReceivedDate", "RPOH_VENINV as VendorInvNumber", "RPOH_WHSNAM as WarehouseOfVendor","RPOH_SHPDAT as VendorShipDate","RPOH_PALLETS_1 as ShippedPallets","RPOH_CUBE_1 as ShippedCubes","RPOH_WEIGHT_1 as ShippedWeight","RPOH_CASES_1 as ShippedCases","VendorName", "VendorAddress", "VendorCity", "VendorState", "VendorZip", "VendorRoute", "SOURCE_FLAG")

query = (
    df.writeStream
   .format("delta")
   .outputMode("append")
   .option("checkpointLocation", checkpoint_location)
   .start(gold_location)
)

query.awaitTermination()

# COMMAND ----------


