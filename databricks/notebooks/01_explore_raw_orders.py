# Databricks notebook source
raw_path = "abfss://order-data@storderlakehbrs160926.dfs.core.windows.net/raw/orders/"

display(dbutils.fs.ls(raw_path))

# COMMAND ----------

raw_orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_path)
)

display(raw_orders_df)