with orders as (
    SELECT *
    FROM {{ ref('stg_orders') }}
),

payment as (
    SELECT *
    FROM {{ ref('stg_payments') }}
),

order_payment as (
    SELECT
        order_id,
        sum(case when status = 'success' then amount end) as amount
    from payment
    group by 1
),

final as (
    SELECT
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce(order_payment.amount, 0) as amount
    FROM orders
    left join order_payment using (order_id)
)

SELECT * 
from final