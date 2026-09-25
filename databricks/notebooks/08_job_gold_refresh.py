# Databricks notebook source
from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum as spark_sum,
    when,
    current_timestamp
)

catalog_name = "dbw_order_lakehouse_lab"

silver_table = f"{catalog_name}.silver.orders"
gold_table = f"{catalog_name}.gold.daily_order_metrics"

gold_path = (
    "abfss://order-data@storderlakehbrs160926."
    "dfs.core.windows.net/gold/daily_order_metrics/"
)

silver_df = spark.table(silver_table)

gold_daily_df = (
    silver_df
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
    .withColumn(
        "_calculated_at",
        current_timestamp()
    )
)

print("Số order trong Silver:", silver_df.count())
print("Số ngày được tính cho Gold:", gold_daily_df.count())

display(gold_daily_df.orderBy("report_date"))

# COMMAND ----------

(
    gold_daily_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", gold_path)
    .saveAsTable(gold_table)
)

saved_gold_df = spark.table(gold_table)

gold_total_orders = (
    saved_gold_df
    .agg(
        spark_sum("total_orders").alias("value")
    )
    .first()["value"]
)

gold_total_amount = (
    saved_gold_df
    .agg(
        spark_sum("total_amount_cents").alias("value")
    )
    .first()["value"]
)

print("Số dòng Gold:", saved_gold_df.count())
print("Tổng order Gold:", gold_total_orders)
print("Tổng tiền Gold:", gold_total_amount)
print("Gold task hoàn thành")