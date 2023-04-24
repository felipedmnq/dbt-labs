WITH 

orders AS (
    SELECT * FROM {{ ref('stg_jaffle_shop__orders') }} 
),

payments AS (
    SELECT * FROM {{ ref('stg_stripe__payments') }} 
    WHERE payment_status != 'fail'
),

order_totals AS (
    SELECT 
        order_id,
        payment_status,
        SUM(payment_amount) AS order_value_dollars
    FROM payments
    GROUP BY 1, 2
),

order_values_joined AS (
    SELECT
        orders.*,
        order_totals.payment_status,
        order_totals.order_value_dollars
    FROM orders 
    LEFT JOIN order_totals
    ON orders.order_id = order_totals.order_id
)

SELECT * FROM order_values_joined
