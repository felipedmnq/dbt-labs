WITH 

orders AS (
    SELECT * FROM {{ ref('int_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_jaffle_shop__customers') }}
),
---
customer_orders AS (
    SELECT
        orders.*,
        customers.full_name,
        customers.surname,
        customers.givenname,
        --- customer level agg
        MIN(orders.order_date) OVER (
            PARTITION BY customers.customer_id
        ) AS customer_first_order_date,

        MIN(orders.valid_order_date) OVER (
            PARTITION BY customers.customer_id
        ) AS customer_first_non_returned_order_date,

        MAX(orders.valid_order_date) OVER (
            PARTITION BY customers.customer_id
        ) AS customer_most_recent_non_returned_order_date,

        COUNT(*) OVER (
            PARTITION BY customers.customer_id
        ) AS customer_order_count,

        SUM(CASE WHEN orders.valid_order_date IS NOT NULL THEN 1 ELSE 0 END) OVER (
            PARTITION BY customers.customer_id
        ) AS customer_non_returned_order_count,

        SUM(
            orders.valid_order_date,
            orders.order_value_dollars,
            0
        ) OVER (
            PARTITION BY customers.customer_id
        ) AS customer_total_lifetime_value,

        ARRAY_AGG(DISTINCT orders.order_id) OVER (
            PARTITION BY customers.customer_id
        ) AS customer_order_ids
    FROM orders
    INNER JOIN customers
    ON orders.customer_id = customers.customer_id
),

avg_order_values AS (
    SELECT 
        *,
        total_time_value / non_returned_order_count AS customer_avg_non_returned_order_count
    FROM customer_orders
),

-- FINAL CTE
final AS (
    SELECT
        order_id,
        customer_id,
        surname,
        givenname,
        customer_first_order_date AS first_order_date,
        customer_order_count AS order_count,
        customer_total_lifetime_value AS total_lifetime_value,
        order_value_dollars,
        order_status,
        payment_status
    FROM avg_order_values
)

SELECT * FROM final