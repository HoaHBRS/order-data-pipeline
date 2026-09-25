# Databricks notebook source
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType
)

order_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("status", StringType()),
    StructField("amount_cents", LongType()),
    StructField("updated_at", StringType())
])

bronze_df = spark.table(
    "order_analytics.bronze_eventhub_orders"
)

structured_events_df = (
    bronze_df
    .withColumn("order", from_json(col("raw_json"), order_schema))
    .select(
        col("order.order_id").alias("order_id"),
        col("order.customer_id").alias("customer_id"),
        col("order.status").alias("status"),
        col("order.amount_cents").alias("amount_cents"),
        to_timestamp(col("order.updated_at")).alias("updated_at"),
        "eventhub_partition",
        "kafka_offset",
        "enqueued_at",
        "ingested_at"
    )
)

display(
    structured_events_df.orderBy("order_id", "updated_at")
)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, row_number, trim

valid_events_df = structured_events_df.filter(
    col("order_id").isNotNull()
    & (trim(col("order_id")) != "")
    & col("customer_id").isNotNull()
    & (trim(col("customer_id")) != "")
    & col("status").isin("PENDING", "PAID", "SHIPPED", "CANCELLED")
    & (col("amount_cents") > 0)
    & col("updated_at").isNotNull()
)

latest_order_window = (
    Window
    .partitionBy("order_id")
    .orderBy(
        col("updated_at").desc(),
        col("enqueued_at").desc(),
        col("eventhub_partition").desc(),
        col("kafka_offset").desc()
    )
)

latest_orders_df = (
    valid_events_df
    .withColumn(
        "row_number",
        row_number().over(latest_order_window)
    )
    .filter(col("row_number") == 1)
    .drop("row_number")
)

display(latest_orders_df.orderBy("order_id"))

# COMMAND ----------

latest_orders_df.createOrReplaceTempView("latest_eventhub_orders")

spark.sql("""
CREATE TABLE IF NOT EXISTS order_analytics.silver_eventhub_orders (
    order_id STRING,
    customer_id STRING,
    status STRING,
    amount_cents BIGINT,
    updated_at TIMESTAMP,
    eventhub_partition INT,
    kafka_offset BIGINT,
    enqueued_at TIMESTAMP,
    ingested_at TIMESTAMP
)
USING DELTA
""")

spark.sql("""
MERGE INTO order_analytics.silver_eventhub_orders AS target
USING latest_eventhub_orders AS source
ON target.order_id = source.order_id

WHEN MATCHED AND (
       source.updated_at > target.updated_at
    OR (
           source.updated_at = target.updated_at
       AND source.enqueued_at > target.enqueued_at
    )
)
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *
""")

display(
    spark.table("order_analytics.silver_eventhub_orders")
         .orderBy("order_id")
)