WITH source as (
    SELECT *
    FROM {{ source('jaffle_shop', 'orders') }}
),
staged AS (
    SELECT 
        id AS order_id,
        user_id AS customer_id,
        order_date,
        status
    FROM source
)

SELECT * FROM staged