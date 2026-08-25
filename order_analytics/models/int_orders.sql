SELECT
    order_id,
    customer_id,
    status,
    amount_cents,
    {{ cents_to_amount('amount_cents') }} AS amount,
    updated_at
FROM {{ ref('stg_orders') }}
