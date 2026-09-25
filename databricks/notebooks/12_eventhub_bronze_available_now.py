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

streaming_kafka_options = {
    "kafka.bootstrap.servers": f"{event_hubs_server}:9093",
    "subscribe": event_hubs_topic,
    "kafka.group.id": "databricks-orders",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": sasl_config,
    "startingOffsets": "earliest"
}

event_stream_df = (
    spark.readStream
         .format("kafka")
         .options(**streaming_kafka_options)
         .load()
         .selectExpr(
             "CAST(value AS STRING) AS raw_json",
             "partition AS eventhub_partition",
             "offset AS kafka_offset",
             "timestamp AS enqueued_at"
         )
)

print("Streaming DataFrame:", event_stream_df.isStreaming)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

spark.sql("""
CREATE TABLE IF NOT EXISTS
dbw_order_lakehouse_lab.order_analytics.bronze_eventhub_orders (
    raw_json STRING,
    eventhub_partition INT,
    kafka_offset BIGINT,
    enqueued_at TIMESTAMP,
    ingested_at TIMESTAMP
)
USING DELTA
""")

def upsert_bronze(micro_batch_df, batch_id):
    prepared_df = micro_batch_df.withColumn(
        "ingested_at",
        current_timestamp()
    )

    row_count = prepared_df.count()
    print(f"batch_id={batch_id}, rows_read={row_count}")

    if row_count == 0:
        return

    target = DeltaTable.forName(
        spark,
        "dbw_order_lakehouse_lab.order_analytics.bronze_eventhub_orders"
    )

    (
        target.alias("target")
        .merge(
            prepared_df.alias("source"),
            """
            target.eventhub_partition = source.eventhub_partition
            AND target.kafka_offset = source.kafka_offset
            """
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

print("✅ Hàm foreachBatch đã sẵn sàng")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS
# MAGIC   dbw_order_lakehouse_lab.order_analytics.eventhub_checkpoints;

# COMMAND ----------

checkpoint_path = (
    "/Volumes/dbw_order_lakehouse_lab/"
    "order_analytics/eventhub_checkpoints/bronze_orders"
)

print(checkpoint_path)

# COMMAND ----------

query = (
    event_stream_df.writeStream
        .foreachBatch(upsert_bronze)
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True)
        .start()
)

query.awaitTermination()

print("✅ AvailableNow hoàn tất")