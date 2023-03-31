WITH 

orders AS (
    SELECT * FROM {{ ref('stg_jaffle_shop__orders') }} 
),

customers AS (
    SELECT * FROM {{ ref('stg_jaffle_shop__customers') }}
),

payments AS (
    SELECT * FROM {{ ref('stg_stripe__payments') }} 
),

-- mart
customer_order_history AS (
    SELECT
        customers.customer_id,
        customers.surname,
        customers.givenname,
        customers.full_name,
        MIN(order_date) AS first_order_date,
        MIN(CASE WHEN orders.order_status NOT IN ('returned', 'return_pending') 
            THEN order_date END) AS first_non_returned_order_date,
        MAX(CASE WHEN orders.order_status NOT IN ('returned', 'return_pending') 
            THEN order_date END) AS most_recent_non_returned_order_date,
        COALESCE(MAX(user_order_seq), 0) AS order_count,
        COALESCE(COUNT(CASE WHEN orders.order_status != 'returned' 
            THEN 1 END), 0) AS non_returned_order_count,
        SUM(CASE WHEN orders.order_status NOT IN ('returned', 'return_pending') 
            THEN payments.payment_amount ELSE 0 END) AS total_lifetime_value,
        SUM(CASE WHEN orders.order_status NOT IN ('returned', 'return_pending') 
            THEN payments.payment_amount ELSE 0 END)/NULLIF(
            COUNT(CASE WHEN orders.order_status NOT IN ('returned', 'return_pending') 
                THEN 1 END), 0
        ) AS avg_non_returned_order_count,
        ARRAY_AGG(DISTINCT orders.order_id) AS order_ids
    FROM orders

    JOIN customers
    ON orders.customer_id = customers.customer_id

    LEFT OUTER JOIN payments
    ON orders.order_id = payments.order_id

    WHERE orders.order_status NOT IN ('pending') AND payments.payment_status != 'fail'
    GROUP BY customers.customer_id, customers.full_name, customers.surname, customers.givenname
),

-- FINAL CTE
final AS (
    SELECT
        orders.order_id,
        orders.customer_id,
        customers.surname,
        customers.givenname,
        first_order_date,
        order_count,
        total_lifetime_value,
        payment_amount AS order_value_dollars,
        orders.order_status,
        payment_status
    FROM orders

    JOIN customers
    ON orders.customer_id = customers.customer_id

    JOIN customer_order_history
    ON orders.customer_id = customer_order_history.customer_id

    LEFT OUTER JOIN payments
    ON orders.order_id = payments.order_id

    WHERE payments.payment_status != 'fail'
)

SELECT * FROM final