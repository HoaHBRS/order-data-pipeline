# Databricks notebook source
eventhubs_connection_string = dbutils.secrets.get(
    scope="order-lab-secrets",
    key="eventhubs-connection-string"
)

print("✅ Đã đọc được Event Hubs secret")

# COMMAND ----------

event_hubs_server = "evhns-order-lakehouse-hbrs160926.servicebus.windows.net"
event_hubs_topic = "orders"

sasl_config = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    'username="$ConnectionString" '
    f'password="{eventhubs_connection_string}";'
)

kafka_options = {
    "kafka.bootstrap.servers": f"{event_hubs_server}:9093",
    "subscribe": event_hubs_topic,
    "kafka.group.id": "databricks-orders",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": sasl_config,
    "startingOffsets": "earliest",
    "endingOffsets": "latest"
}

events_df = (
    spark.read
         .format("kafka")
         .options(**kafka_options)
         .load()
         .selectExpr(
             "CAST(value AS STRING) AS event_json",
             "partition",
             "offset",
             "timestamp"
         )
)

display(events_df.orderBy("partition", "offset"))

# COMMAND ----------

from pyspark.sql.functions import col, from_json
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

parsed_events_df = (
    events_df
    .withColumn("order", from_json(col("event_json"), order_schema))
    .select(
        "order.*",
        col("partition").alias("eventhub_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("enqueued_at")
    )
)

display(
    parsed_events_df.orderBy("eventhub_partition", "kafka_offset")
)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

bronze_batch_df = (
    events_df
    .select(
        col("event_json").alias("raw_json"),
        col("partition").alias("eventhub_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("enqueued_at")
    )
    .withColumn("ingested_at", current_timestamp())
)

bronze_batch_df.createOrReplaceTempView("eventhub_bronze_batch")

spark.sql("CREATE SCHEMA IF NOT EXISTS order_analytics")

spark.sql("""
CREATE TABLE IF NOT EXISTS order_analytics.bronze_eventhub_orders (
    raw_json STRING,
    eventhub_partition INT,
    kafka_offset BIGINT,
    enqueued_at TIMESTAMP,
    ingested_at TIMESTAMP
)
USING DELTA
""")

spark.sql("""
MERGE INTO order_analytics.bronze_eventhub_orders AS target
USING eventhub_bronze_batch AS source
ON  target.eventhub_partition = source.eventhub_partition
AND target.kafka_offset = source.kafka_offset
WHEN NOT MATCHED THEN INSERT *
""")

display(
    spark.table("order_analytics.bronze_eventhub_orders")
         .orderBy("eventhub_partition", "kafka_offset")
)