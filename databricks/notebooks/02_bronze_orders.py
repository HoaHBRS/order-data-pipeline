# Databricks notebook source
catalog_name = "dbw_order_lakehouse_lab"

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.bronze
""")

print("Đã tạo schema bronze")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

raw_path = (
    "abfss://order-data@storderlakehbrs160926."
    "dfs.core.windows.net/raw/orders/"
)

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount_cents", StringType(), True),
    StructField("updated_at", StringType(), True),
])

print("Đã khai báo raw_path và orders_schema")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

bronze_df = (
    spark.read
    .option("header", "true")
    .schema(orders_schema)
    .csv(raw_path)
    .withColumn("_source_file", col("_metadata.file_path"))
    .withColumn("_ingested_at", current_timestamp())
)

display(bronze_df)

# COMMAND ----------

bronze_path = (
    "abfss://order-data@storderlakehbrs160926."
    "dfs.core.windows.net/bronze/orders/"
)

bronze_table = f"{catalog_name}.bronze.orders"

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", bronze_path)
    .saveAsTable(bronze_table)
)

print(f"Đã tạo bảng Delta: {bronze_table}")

# COMMAND ----------

saved_bronze_df = spark.table(bronze_table)

print("Số dòng trong bảng Delta:", saved_bronze_df.count())
display(saved_bronze_df)

# COMMAND ----------

display(dbutils.fs.ls(bronze_path))

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {bronze_table}"))