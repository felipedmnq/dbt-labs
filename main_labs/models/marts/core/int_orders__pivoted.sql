--WITH payments AS (
--    SELECT * FROM {{ ref('stg_payments') }}
--),
--pivoted AS (
--    SELECT
--        order_id,
--        SUM(CASE WHEN payment_method = 'bank_transfer' THEN amount ELSE 0 END) AS bank_transfer_amount,
--        SUM(CASE WHEN payment_method = 'coupon' THEN amount ELSE 0 END) AS coupon_amount,
--        SUM(CASE WHEN payment_method = 'credit_card' THEN amount ELSE 0 END) AS credit_card_amount,
--        SUM(CASE WHEN payment_method = 'gift_card' THEN amount ELSE 0 END) AS gift_card_amount
--    FROM payments
--    WHERE status = 'success'
--    GROUP BY 1
--
--SELECT * FROM pivoted

{%- set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] -%}

WITH payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),
pivoted AS (
    SELECT
        order_id,

        {% for payment in payment_methods -%}

            SUM(CASE WHEN payment_method = '{{ payment }}' THEN amount ELSE 0 END) AS {{ payment }}_amount
            {%- if not loop.last -%}
                ,
            {%- endif %}
        {% endfor -%}
    FROM payments
    WHERE status = 'success'
    GROUP BY 1
)

SELECT * FROM pivoted