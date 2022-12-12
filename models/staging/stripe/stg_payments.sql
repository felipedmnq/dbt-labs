WITH payments AS (
    SELECT 
        id AS payment_id,
        orderid AS order_id,
        paymentmethod as payment_method,
        status,
        amount / 100 as amount,
        created AS created_at
    FROM `dbt-tutorial.stripe.payment`
)

SELECT * FROM payments