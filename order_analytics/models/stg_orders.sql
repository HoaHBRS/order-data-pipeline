SELECT *
FROM {{ source('order_pipeline', 'orders') }}
