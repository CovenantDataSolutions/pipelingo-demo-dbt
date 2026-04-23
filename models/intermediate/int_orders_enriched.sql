with orders as (
    select * from {{ ref('stg_orders') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

payments as (
    select order_id, sum(amount_usd) as paid_amount_usd
    from {{ ref('stg_payments') }}
    where is_captured
    group by 1
),

promotions as (
    select * from {{ ref('stg_promotions') }}
),

order_totals as (
    select
        order_id,
        count(order_item_id) as item_count,
        sum(quantity) as total_quantity,
        sum(line_total_usd) as subtotal_usd
    from order_items
    group by 1
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    o.shipping_address_country,
    o.is_completed,
    ot.item_count,
    ot.total_quantity,
    ot.subtotal_usd,
    coalesce(p.paid_amount_usd, 0) as paid_amount_usd,
    pr.promotion_code,
    pr.discount_percent
from orders o
left join order_totals ot on o.order_id = ot.order_id
left join payments p on o.order_id = p.order_id
left join promotions pr on o.promotion_id = pr.promotion_id
