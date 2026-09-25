# Databricks notebook source
catalog_name = "dbw_order_lakehouse_lab"

bronze_table = f"{catalog_name}.bronze.orders"
silver_table = f"{catalog_name}.silver.orders"
quarantine_table = f"{catalog_name}.quarantine.orders"

new_raw_file = (
    "abfss://order-data@storderlakehbrs160926.dfs.core.windows.net/"
    "raw/orders/orders_2026-08-03.csv"
)

csv_content = """order_id,customer_id,status,amount_cents,updated_at
ORD-1001,CUST-501,SHIPPED,4990,2026-08-03T09:00:00
ORD-1009,CUST-509,PAID,3200,2026-08-03T09:05:00
ORD-1011,CUST-511,PAID,6500,2026-08-03T09:10:00
ORD-1012,,PAID,2100,2026-08-03T09:15:00
ORD-1002,CUST-502,CANCELLED,8950,2026-08-01T07:00:00
"""

dbutils.fs.put(new_raw_file, csv_content, True)

print("Đã tạo:", new_raw_file)
print(dbutils.fs.head(new_raw_file))

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)
from pyspark.sql.functions import col, current_timestamp

orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("amount_cents", StringType(), True),
    StructField("updated_at", StringType(), True)
])

incremental_bronze_df = (
    spark.read
    .option("header", "true")
    .schema(orders_schema)
    .csv(new_raw_file)
    .withColumn(
        "_source_file",
        col("_metadata.file_path")
    )
    .withColumn(
        "_ingested_at",
        current_timestamp()
    )
)

print("Số dòng trong batch mới:", incremental_bronze_df.count())
incremental_bronze_df.printSchema()

display(incremental_bronze_df)

# COMMAND ----------

incremental_bronze_df.createOrReplaceTempView(
    "incremental_bronze_batch"
)

bronze_count_before = spark.table(bronze_table).count()

spark.sql(f"""
    MERGE INTO {bronze_table} AS target
    USING incremental_bronze_batch AS source

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

# COMMAND ----------

from pyspark.sql.functions import col, expr, trim, when, lit, concat_ws

valid_statuses = ["PAID", "SHIPPED", "PENDING", "CANCELLED"]

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
                col("order_id").isNull() | (trim(col("order_id")) == ""),
                lit("missing_order_id")
            ),
            when(
                col("customer_id").isNull() | (trim(col("customer_id")) == ""),
                lit("missing_customer_id")
            ),
            when(
                col("status").isNull() | (~col("status").isin(valid_statuses)),
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
    col("_amount_cents_typed").alias("amount_cents"),
    col("_updated_at_typed").alias("updated_at"),
    "_source_file",
    "_ingested_at"
)

incremental_silver_df.printSchema()
display(incremental_silver_df.orderBy("order_id"))

# COMMAND ----------

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

display(
    spark.table(silver_table)
    .filter(
        col("order_id").isin(
            "ORD-1001", "ORD-1002", "ORD-1009", "ORD-1011"
        )
    )
    .orderBy("order_id")
)

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
    .withColumn("_quarantined_at", current_timestamp())
)

incremental_quarantine_output_df.createOrReplaceTempView(
    "incremental_quarantine_batch"
)

quarantine_count_before = spark.table(quarantine_table).count()

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

quarantine_count_after = spark.table(quarantine_table).count()

print("Quarantine trước MERGE:", quarantine_count_before)
print("Quarantine sau MERGE:", quarantine_count_after)
print("Số dòng lỗi mới:", quarantine_count_after - quarantine_count_before)

display(
    spark.table(quarantine_table)
    .filter(col("order_id") == "ORD-1012")
)

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum as spark_sum,
    when,
    current_timestamp
)

gold_table = f"{catalog_name}.gold.daily_order_metrics"

current_silver_df = spark.table(silver_table)

gold_daily_df = (
    current_silver_df
    .withColumn(
        "report_date",
        to_date(col("updated_at"))
    )
    .groupBy("report_date")
    .agg(
        count("*").alias("total_orders"),

        spark_sum("amount_cents")
        .alias("total_amount_cents"),

        spark_sum(
            when(col("status") == "PAID", 1).otherwise(0)
        ).alias("paid_orders"),

        spark_sum(
            when(col("status") == "SHIPPED", 1).otherwise(0)
        ).alias("shipped_orders"),

        spark_sum(
            when(col("status") == "PENDING", 1).otherwise(0)
        ).alias("pending_orders"),

        spark_sum(
            when(col("status") == "CANCELLED", 1).otherwise(0)
        ).alias("cancelled_orders"),

        spark_sum(
            when(
                col("status") == "PAID",
                col("amount_cents")
            ).otherwise(0)
        ).alias("paid_amount_cents")
    )
    .withColumn("_calculated_at", current_timestamp())
)

display(gold_daily_df.orderBy("report_date"))

# COMMAND ----------

gold_path = (
    "abfss://order-data@storderlakehbrs160926."
    "dfs.core.windows.net/gold/daily_order_metrics/"
)

(
    gold_daily_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", gold_path)
    .saveAsTable(gold_table)
)

saved_gold_df = spark.table(gold_table)

print("Số dòng Gold:", saved_gold_df.count())

print(
    "Tổng order Gold:",
    saved_gold_df
    .agg(spark_sum("total_orders"))
    .first()[0]
)

print(
    "Tổng tiền Gold:",
    saved_gold_df
    .agg(spark_sum("total_amount_cents"))
    .first()[0]
)

display(saved_gold_df.orderBy("report_date"))

# COMMAND ----------

silver_final_df = spark.table(silver_table)
gold_final_df = spark.table(gold_table)

gold_total_orders = (
    gold_final_df
    .agg(spark_sum("total_orders").alias("value"))
    .first()["value"]
)

silver_total_amount = (
    silver_final_df
    .agg(spark_sum("amount_cents").alias("value"))
    .first()["value"]
)

gold_total_amount = (
    gold_final_df
    .agg(spark_sum("total_amount_cents").alias("value"))
    .first()["value"]
)

duplicate_silver_orders = (
    silver_final_df
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

duplicate_gold_dates = (
    gold_final_df
    .groupBy("report_date")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Bronze events:", spark.table(bronze_table).count())
print("Silver orders:", silver_final_df.count())
print("Quarantine:", spark.table(quarantine_table).count())
print("Gold days:", gold_final_df.count())
print("Tổng order Gold:", gold_total_orders)
print("Tổng tiền Silver:", silver_total_amount)
print("Tổng tiền Gold:", gold_total_amount)
print("Order trùng trong Silver:", duplicate_silver_orders)
print("Ngày trùng trong Gold:", duplicate_gold_dates)