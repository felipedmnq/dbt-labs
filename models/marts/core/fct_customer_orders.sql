WITH 

orders AS (
    SELECT * FROM {{ ref('int_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_jaffle_shop__customers') }}
),

customer_order_history AS (
    SELECT
        customers.customer_id,
        customers.surname,
        customers.givenname,
        customers.full_name,
        MIN(orders.order_date) AS first_order_date,
        MIN(orders.valid_order_date) AS first_non_returned_order_date,
        MAX(orders.valid_order_date) AS most_recent_non_returned_order_date,
        COALESCE(MAX(orders.user_order_seq), 0) AS order_count,
        COALESCE(COUNT(CASE 
            WHEN orders.valid_order_date IS NOT NULL
            THEN 1 
        END), 0) AS non_returned_order_count,
        SUM(CASE 
            WHEN orders.valid_order_date IS NOT NULL 
            THEN orders.order_value_dollars ELSE 0 
        END) AS total_lifetime_value,
        SUM(CASE 
            WHEN orders.valid_order_date IS NOT NULL 
            THEN orders.order_value_dollars ELSE 0 
        END)/NULLIF(COUNT(CASE 
                WHEN orders.valid_order_date IS NOT NULL 
                THEN 1 
            END), 0
        ) AS avg_non_returned_order_count,
        ARRAY_AGG(DISTINCT orders.order_id) AS order_ids
    FROM orders

    JOIN customers
    ON orders.customer_id = customers.customer_id

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
        orders.order_value_dollars,
        orders.order_status,
        orders.payment_status
    FROM orders

    JOIN customers
    ON orders.customer_id = customers.customer_id

    JOIN customer_order_history
    ON orders.customer_id = customer_order_history.customer_id
)

SELECT * FROM final