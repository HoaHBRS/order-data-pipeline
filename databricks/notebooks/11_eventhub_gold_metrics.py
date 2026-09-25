# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE order_analytics.gold_eventhub_order_metrics
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC     current_timestamp() AS refreshed_at,
# MAGIC     COUNT(*) AS total_orders,
# MAGIC     SUM(amount_cents) AS total_amount_cents,
# MAGIC     SUM(CASE WHEN status = 'PENDING'   THEN 1 ELSE 0 END) AS pending_orders,
# MAGIC     SUM(CASE WHEN status = 'PAID'      THEN 1 ELSE 0 END) AS paid_orders,
# MAGIC     SUM(CASE WHEN status = 'SHIPPED'   THEN 1 ELSE 0 END) AS shipped_orders,
# MAGIC     SUM(CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END) AS cancelled_orders
# MAGIC FROM order_analytics.silver_eventhub_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM order_analytics.gold_eventhub_order_metrics