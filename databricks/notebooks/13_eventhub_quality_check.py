# Databricks notebook source
quality = spark.sql("""
WITH silver_stats AS (
    SELECT
        COUNT(*) AS silver_rows,
        COUNT(DISTINCT order_id) AS distinct_order_ids,
        COALESCE(SUM(amount_cents), 0) AS silver_total_amount,

        SUM(
            CASE
                WHEN TRIM(COALESCE(order_id, '')) = ''
                  OR TRIM(COALESCE(customer_id, '')) = ''
                  OR status IS NULL
                  OR status NOT IN (
                      'PENDING', 'PAID', 'SHIPPED', 'CANCELLED'
                  )
                  OR amount_cents IS NULL
                  OR amount_cents <= 0
                  OR updated_at IS NULL
                THEN 1
                ELSE 0
            END
        ) AS invalid_rows
    FROM dbw_order_lakehouse_lab.order_analytics.silver_eventhub_orders
),

gold_stats AS (
    SELECT
        total_orders AS gold_total_orders,
        total_amount_cents AS gold_total_amount,
        pending_orders
          + paid_orders
          + shipped_orders
          + cancelled_orders AS gold_status_total
    FROM dbw_order_lakehouse_lab.order_analytics.gold_eventhub_order_metrics
)

SELECT *
FROM silver_stats
CROSS JOIN gold_stats
""").first()

checks = {
    "Silver order_id duy nhất":
        quality.silver_rows == quality.distinct_order_ids,

    "Silver không có dòng lỗi":
        quality.invalid_rows == 0,

    "Gold total_orders khớp Silver":
        quality.gold_total_orders == quality.silver_rows,

    "Gold total_amount khớp Silver":
        quality.gold_total_amount == quality.silver_total_amount,

    "Tổng các status khớp total_orders":
        quality.gold_status_total == quality.gold_total_orders
}

failed_checks = []

for check_name, passed in checks.items():
    if passed:
        print(f"✅ {check_name}")
    else:
        print(f"❌ {check_name}")
        failed_checks.append(check_name)

if failed_checks:
    raise ValueError(
        "Data Quality thất bại: " + ", ".join(failed_checks)
    )

print("✅ Tất cả Data Quality checks đều pass")