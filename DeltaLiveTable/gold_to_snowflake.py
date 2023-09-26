# Databricks notebook source
spark.readStream.table("dev.default.sales_order_in_ny").show()
