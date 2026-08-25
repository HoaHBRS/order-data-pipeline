{{ config(materialized='table') }}

SELECT
    orders.status,
    statuses.status_label,
    COUNT(*) AS order_count,
    SUM(orders.amount) AS total_amount
FROM {{ ref('int_orders') }} AS orders
LEFT JOIN {{ ref('order_statuses') }} AS statuses
    ON orders.status = statuses.status
GROUP BY
    orders.status,
    statuses.status_label