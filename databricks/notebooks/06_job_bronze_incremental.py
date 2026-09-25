# Databricks notebook source
dbutils.widgets.text(
    "input_file",
    (
        "abfss://order-data@storderlakehbrs160926."
        "dfs.core.windows.net/raw/orders/"
        "orders_2026-08-03.csv"
    )
)

input_file = dbutils.widgets.get("input_file")

print("Input file:", input_file)

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)

from pyspark.sql.functions import (
    col,
    current_timestamp
)

catalog_name = "dbw_order_lakehouse_lab"
bronze_table = f"{catalog_name}.bronze.orders"

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount_cents", StringType(), True),
    StructField("updated_at", StringType(), True)
])

bronze_source_df = (
    spark.read
    .option("header", "true")
    .schema(orders_schema)
    .csv(input_file)
    .withColumn(
        "_source_file",
        col("_metadata.file_path")
    )
    .withColumn(
        "_ingested_at",
        current_timestamp()
    )
)

print("Số dòng trong input file:", bronze_source_df.count())
display(bronze_source_df.orderBy("order_id"))

# COMMAND ----------

bronze_source_df.createOrReplaceTempView(
    "bronze_source_batch"
)

bronze_count_before = spark.table(bronze_table).count()

spark.sql(f"""
    MERGE INTO {bronze_table} AS target
    USING bronze_source_batch AS source

    ON  target._source_file = source._source_file
    AND target.order_id = source.order_id
    AND target.updated_at = source.updated_at

    WHEN NOT MATCHED THEN
        INSERT *
""")

bronze_count_after = spark.table(bronze_table).count()

print("Bronze trước MERGE:", bronze_count_before)
print("Bronze sau MERGE:", bronze_count_after)
print("Số dòng mới:", bronze_count_after - bronze_count_before)
print("Bronze task hoàn thành")