# Databricks notebook source
catalog_name = "dbw_order_lakehouse_lab"

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.silver
""")

spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {catalog_name}.quarantine
""")

print("Đã tạo schema silver và quarantine")

# COMMAND ----------

bronze_table = f"{catalog_name}.bronze.orders"

bronze_df = spark.table(bronze_table)

print("Số dòng Bronze:", bronze_df.count())
bronze_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import expr

typed_df = (
    bronze_df
    .withColumn(
        "_amount_cents_typed",
        expr("try_cast(amount_cents AS BIGINT)")
    )
    .withColumn(
        "_updated_at_typed",
        expr("try_cast(updated_at AS TIMESTAMP)")
    )
)

display(
    typed_df.select(
        "order_id",
        "amount_cents",
        "_amount_cents_typed",
        "updated_at",
        "_updated_at_typed"
    )
)

# COMMAND ----------

from pyspark.sql.functions import col, trim, when, lit, concat_ws

valid_statuses = ["PAID", "SHIPPED", "PENDING", "CANCELLED"]

validated_df = typed_df.withColumn(
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

display(
    validated_df.select(
        "order_id",
        "customer_id",
        "status",
        "amount_cents",
        "updated_at",
        "_error_reason"
    )
)

# COMMAND ----------

valid_before_dedup_df = validated_df.filter(
    col("_error_reason") == ""
)

quarantine_df = validated_df.filter(
    col("_error_reason") != ""
)

print(
    "Hợp lệ trước deduplicate:",
    valid_before_dedup_df.count()
)

print(
    "Quarantine:",
    quarantine_df.count()
)

display(
    quarantine_df.select(
        "order_id",
        "customer_id",
        "status",
        "amount_cents",
        "updated_at",
        "_error_reason"
    )
)

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number

latest_order_window = (
    Window
    .partitionBy("order_id")
    .orderBy(col("_updated_at_typed").desc())
)

ranked_df = valid_before_dedup_df.withColumn(
    "_row_number",
    row_number().over(latest_order_window)
)

display(
    ranked_df.select(
        "order_id",
        "status",
        "_amount_cents_typed",
        "_updated_at_typed",
        "_row_number"
    ).orderBy("order_id", "_row_number")
)

# COMMAND ----------

silver_df = (
    ranked_df
    .filter(col("_row_number") == 1)
    .select(
        "order_id",
        "customer_id",
        "status",
        col("_amount_cents_typed").alias("amount_cents"),
        col("_updated_at_typed").alias("updated_at"),
        "_source_file",
        "_ingested_at"
    )
)

print("Silver sau deduplicate:", silver_df.count())
silver_df.printSchema()

display(silver_df.orderBy("order_id"))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

quarantine_output_df = (
    quarantine_df
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

print("Số dòng Quarantine:", quarantine_output_df.count())

display(
    quarantine_output_df
    .select(
        "order_id",
        "customer_id",
        "status",
        "amount_cents",
        "updated_at",
        "_error_reason",
        "_quarantined_at"
    )
)

# COMMAND ----------

silver_table = f"{catalog_name}.silver.orders"
quarantine_table = f"{catalog_name}.quarantine.orders"

silver_path = (
    "abfss://order-data@storderlakehbrs160926."
    "dfs.core.windows.net/silver/orders/"
)

quarantine_path = (
    "abfss://order-data@storderlakehbrs160926."
    "dfs.core.windows.net/quarantine/orders/"
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", silver_path)
    .saveAsTable(silver_table)
)

(
    quarantine_output_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("path", quarantine_path)
    .saveAsTable(quarantine_table)
)

print("Đã lưu:", silver_table)
print("Đã lưu:", quarantine_table)

# COMMAND ----------

saved_silver_df = spark.table(silver_table)
saved_quarantine_df = spark.table(quarantine_table)

input_count = bronze_df.count()
silver_count = saved_silver_df.count()
quarantine_count = saved_quarantine_df.count()

removed_duplicates = (
    valid_before_dedup_df.count() - silver_count
)

duplicate_order_count = (
    saved_silver_df
    .groupBy("order_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Bronze đầu vào:", input_count)
print("Silver:", silver_count)
print("Quarantine:", quarantine_count)
print("Bản ghi cũ bị deduplicate:", removed_duplicates)
print("Order trùng còn lại trong Silver:", duplicate_order_count)

assert input_count == silver_count + quarantine_count + removed_duplicates
assert duplicate_order_count == 0

display(saved_silver_df.orderBy("order_id"))