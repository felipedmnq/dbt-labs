WITH src_orders AS (
    SELECT * FROM {{ source('jaffle_shop', 'orders') }}
),

orders AS (
    SELECT
        id AS order_id,
        user_id AS customer_id,
        order_date,
        status AS order_status,
        CASE WHEN status NOT IN ('returned', 'return_pending') 
             THEN order_date
        END AS valid_order_date,
        ROW_NUMBER() OVER (
            PARTITION BY user_id 
            ORDER BY order_date, id
        ) AS user_order_seq
    FROM src_orders
)

SELECT * FROM orders