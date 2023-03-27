--WITH payments AS (
--    SELECT
--        id AS payment_id,
--        orderid AS order_id,
--        paymentmethod as payment_method,
--        status,
--        amount / 100 as amount,
--        created AS created_at
--    FROM {{ source('stripe', 'payment') }}
--)
--
--SELECT *
--FROM payments 

-- USING MACRO

--WITH payments AS (
--    SELECT
--        id AS payment_id,
--        orderid AS order_id,
--        paymentmethod AS payment_method,
--        status,
--        {{ cents_to_dolars() }} As amaount,
--        created AS created_at
--    FROM {{ source('stripe', 'payment') }}
--)
--
--SELECT *
--FROM payments 

-- USING VARIABLES

--WITH payments AS (
--    SELECT
--        id AS payment_id,
--        orderid AS order_id,
--        paymentmethod AS payment_method,
--        status,
--        {{ cents_to_dolars_with_param('amount') }} As amaount,
--        created AS created_at
--    FROM {{ source('stripe', 'payment') }}
--)
--
--SELECT *
--FROM payments 

-- USING MULTIPLE ARGS

WITH payments AS (
    SELECT
        id AS payment_id,
        orderid AS order_id,
        paymentmethod AS payment_method,
        status,
        {{ cents_to_dollars_percent('amount') }} AS amount,
        created AS created_at
    FROM {{ source('stripe', 'payment') }}
)

SELECT *
FROM payments 