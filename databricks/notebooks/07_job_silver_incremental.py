# Databricks notebook source
from pyspark.sql.functions import col

dbutils.widgets.text(
    "input_file",
    (
        "abfss://order-data@storderlakehbrs160926."
        "dfs.core.windows.net/raw/orders/"
        "orders_2026-08-03.csv"
    )
)

input_file = dbutils.widgets.get("input_file")

catalog_name = "dbw_order_lakehouse_lab"

bronze_table = f"{catalog_name}.bronze.orders"
silver_table = f"{catalog_name}.silver.orders"
quarantine_table = f"{catalog_name}.quarantine.orders"

incremental_bronze_df = (
    spark.table(bronze_table)
    .filter(col("_source_file") == input_file)
)

print("Input file:", input_file)
print("Số dòng đọc từ Bronze:", incremental_bronze_df.count())

display(incremental_bronze_df.orderBy("order_id"))

# COMMAND ----------

from pyspark.sql.functions import (
    expr,
    trim,
    when,
    lit,
    concat_ws
)

valid_statuses = [
    "PAID",
    "SHIPPED",
    "PENDING",
    "CANCELLED"
]

incremental_validated_df = (
    incremental_bronze_df

    .withColumn(
        "_amount_cents_typed",
        expr("try_cast(amount_cents AS BIGINT)")
    )

    .withColumn(
        "_updated_at_typed",
        expr("try_cast(updated_at AS TIMESTAMP)")
    )

    .withColumn(
        "_error_reason",
        concat_ws(
            "; ",

            when(
                col("order_id").isNull()
                | (trim(col("order_id")) == ""),
                lit("missing_order_id")
            ),

            when(
                col("customer_id").isNull()
                | (trim(col("customer_id")) == ""),
                lit("missing_customer_id")
            ),

            when(
                col("status").isNull()
                | (~col("status").isin(valid_statuses)),
                lit("invalid_status")
            ),

            when(
                col("_amount_cents_typed").isNull(),
                lit("invalid_amount")
            ),

            when(
                col("_amount_cents_typed") < 0,
                lit("negative_amount")
            ),

            when(
                col("_updated_at_typed").isNull(),
                lit("invalid_updated_at")
            )
        )
    )
)

incremental_valid_df = incremental_validated_df.filter(
    col("_error_reason") == ""
)

incremental_quarantine_df = incremental_validated_df.filter(
    col("_error_reason") != ""
)

print("Hợp lệ:", incremental_valid_df.count())
print("Quarantine:", incremental_quarantine_df.count())

display(incremental_quarantine_df)

# COMMAND ----------

incremental_silver_df = incremental_valid_df.select(
    "order_id",
    "customer_id",
    "status",

    col("_amount_cents_typed")
    .alias("amount_cents"),

    col("_updated_at_typed")
    .alias("updated_at"),

    "_source_file",
    "_ingested_at"
)

incremental_silver_df.createOrReplaceTempView(
    "incremental_silver_batch"
)

silver_count_before = spark.table(silver_table).count()

spark.sql(f"""
    MERGE INTO {silver_table} AS target
    USING incremental_silver_batch AS source

    ON target.order_id = source.order_id

    WHEN MATCHED
         AND source.updated_at > target.updated_at
    THEN UPDATE SET *

    WHEN NOT MATCHED
    THEN INSERT *
""")

silver_count_after = spark.table(silver_table).count()

print("Silver trước MERGE:", silver_count_before)
print("Silver sau MERGE:", silver_count_after)
print("Số order mới:", silver_count_after - silver_count_before)
print("Silver MERGE hoàn thành")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

incremental_quarantine_output_df = (
    incremental_quarantine_df
    .select(
        "order_id",
        "customer_id",
        "status",
        "amount_cents",
        "updated_at",
        "_source_file",
        "_ingested_at",
        "_error_reason"
    )
    .withColumn(
        "_quarantined_at",
        current_timestamp()
    )
)

incremental_quarantine_output_df.createOrReplaceTempView(
    "incremental_quarantine_batch"
)

quarantine_count_before = spark.table(
    quarantine_table
).count()

spark.sql(f"""
    MERGE INTO {quarantine_table} AS target
    USING incremental_quarantine_batch AS source

    ON  target._source_file = source._source_file
    AND target.order_id <=> source.order_id
    AND target.updated_at <=> source.updated_at
    AND target._error_reason <=> source._error_reason

    WHEN NOT MATCHED THEN
        INSERT *
""")

quarantine_count_after = spark.table(
    quarantine_table
).count()

print("Quarantine trước MERGE:", quarantine_count_before)
print("Quarantine sau MERGE:", quarantine_count_after)
print(
    "Số dòng lỗi mới:",
    quarantine_count_after - quarantine_count_before
)
print("Silver + Quarantine task hoàn thành")