WITH payments AS (
    SELECT
        id AS payment_id,
        orderid AS order_id,
        status AS payment_status,
        ROUND(amount/100.0, 2) AS payment_amount
    FROM {{ source('stripe', 'payment') }}
)

SELECT * FROM payments