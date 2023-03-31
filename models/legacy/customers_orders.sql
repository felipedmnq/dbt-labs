SELECT
    orders.id AS order_id,
    orders.user_id AS customer_id,
    last_name AS surname,
    first_name AS givenname,
    first_order_date,
    order_count,
    total_lifetime_value,
    ROUND(amount/100, 2) AS order_value_dollars,
    orders.status AS order_status,
    payments.status AS payment_status
FROM `dbt-tutorial.jaffle_shop.orders` AS orders

JOIN (
    SELECT
        first_name || ' ' || last_name AS name,
        *
    FROM `dbt-tutorial.jaffle_shop.customers`
) customers
ON orders.user_id = customers.id

JOIN (
    SELECT
        b.id AS customer_id,
        b.name AS full_name,
        b.last_name AS surname,
        b.first_name AS givenname,
        MIN(order_date) AS first_order_date,
        MIN(CASE WHEN a.status NOT IN ('returned', 'return_pending') THEN order_date END) AS first_non_returned_order_date,
        MAX(CASE WHEN a.status NOT IN ('returned', 'return_pending') THEN order_date END) AS most_recent_non_returned_order_date,
        COALESCE(MAX(user_order_seq), 0) AS order_count,
        COALESCE(COUNT(CASE WHEN a.status != 'returned' THEN 1 END), 0) AS non_returned_order_count,
        SUM(CASE WHEN a.status NOT IN ('returned', 'return_pending') THEN ROUND(c.amount/100, 2) ELSE 0 END) AS total_lifetime_value,
        SUM(CASE WHEN a.status NOT IN ('returned', 'return_pending') THEN ROUND(c.amount/100, 2) ELSE 0 END)/NULLIF(
            COUNT(CASE WHEN a.status NOT IN ('returned', 'return_pending') THEN 1 END), 0
        ) AS avg_non_returned_order_count
    FROM (
        SELECT
            ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY order_date, id) AS user_order_seq,
            *
        FROM `dbt-tutorial.jaffle_shop.orders`
    ) a 

    JOIN (
        SELECT
            first_name || ' ' || last_name AS name,
            *
        FROM `dbt-tutorial.jaffle_shop.customers`
    ) b 
    ON a.user_id = b.id

    LEFT OUTER JOIN `dbt-tutorial.stripe.payment` c
    ON a.id = c.orderid

    WHERE a.status NOT IN ('pending') AND c.status != 'fail'
    GROUP BY b.id, b.name, b.last_name, b.first_name
) customer_order_history
ON orders.user_id = customer_order_history.customer_id

LEFT OUTER JOIN `dbt-tutorial.stripe.payment` payments
ON orders.id = payments.orderid

WHERE payments.status != 'fail'