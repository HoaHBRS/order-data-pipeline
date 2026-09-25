# Databricks notebook source
catalog_name = "dbw_order_lakehouse_lab"

silver_table = f"{catalog_name}.silver.orders"
gold_schema = f"{catalog_name}.gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_schema}")

silver_df = spark.table(silver_table)

print("Số dòng Silver:", silver_df.count())
silver_df.printSchema()

display(silver_df.orderBy("updated_at"))

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum as spark_sum,
    when,
    current_timestamp
)

gold_daily_df = (
    silver_df
    .withColumn("report_date", to_date(col("updated_at")))
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
    .orderBy("report_date")
)

display(gold_daily_df)

# COMMAND ----------

gold_table = f"{catalog_name}.gold.daily_order_metrics"

gold_path = "abfss://order-data@storderlakehbrs160926.dfs.core.windows.net/gold/daily_order_metrics/"

(
    gold_daily_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", gold_path)
    .saveAsTable(gold_table)
)

saved_gold_df = spark.table(gold_table)

print("Đã lưu:", gold_table)
print("Số dòng Gold:", saved_gold_df.count())

display(saved_gold_df.orderBy("report_date"))

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT
# MAGIC     report_date,
# MAGIC     total_orders,
# MAGIC     ROUND(total_amount_cents / 100.0, 2) AS total_amount_eur,
# MAGIC     paid_orders,
# MAGIC     shipped_orders,
# MAGIC     pending_orders,
# MAGIC     cancelled_orders,
# MAGIC     ROUND(paid_amount_cents / 100.0, 2) AS paid_amount_eur
# MAGIC FROM dbw_order_lakehouse_lab.gold.daily_order_metrics
# MAGIC ORDER BY report_date;

# COMMAND ----------

gold_order_count = (
    saved_gold_df
    .agg(spark_sum("total_orders").alias("value"))
    .first()["value"]
)

gold_total_amount = (
    saved_gold_df
    .agg(spark_sum("total_amount_cents").alias("value"))
    .first()["value"]
)

silver_order_count = silver_df.count()

silver_total_amount = (
    silver_df
    .agg(spark_sum("amount_cents").alias("value"))
    .first()["value"]
)

invalid_status_total_rows = (
    saved_gold_df
    .filter(
        col("total_orders") != (
            col("paid_orders")
            + col("shipped_orders")
            + col("pending_orders")
            + col("cancelled_orders")
        )
    )
    .count()
)

duplicate_date_count = (
    saved_gold_df
    .groupBy("report_date")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Số đơn Silver:", silver_order_count)
print("Tổng đơn Gold:", gold_order_count)
print("Tổng tiền Silver:", silver_total_amount)
print("Tổng tiền Gold:", gold_total_amount)
print("Dòng sai tổng trạng thái:", invalid_status_total_rows)
print("Ngày bị trùng:", duplicate_date_count)

assert gold_order_count == silver_order_count
assert gold_total_amount == silver_total_amount
assert invalid_status_total_rows == 0
assert duplicate_date_count == 0