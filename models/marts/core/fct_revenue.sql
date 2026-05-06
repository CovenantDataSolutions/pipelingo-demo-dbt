{{ config(materialized='table') }}

-- Revenue fact at the payment-event grain. Reports daily/weekly/monthly
-- revenue tend to roll up from this. We keep it separate from fct_orders
-- because not every order has a payment, and some orders have multiple.

with payments as (
    select * from {{ ref('stg_payments') }}
    where is_captured  -- only count captured (settled) payments as revenue
),

orders_lookup as (
    -- Pull the priority + market_segment from the order/customer for slicing.
    select
        o.order_id,
        o.priority,
        c.market_segment,
        c.customer_segment_lifecycle
    from {{ ref('stg_orders') }} o
    left join {{ ref('int_customer_lifetime') }} c on o.customer_id = c.customer_id
)

select
    p.payment_id,
    p.order_id,
    p.customer_id,
    p.amount_usd                             as revenue_usd,
    p.payment_method,
    p.paid_at,
    cast(p.paid_at as date)                  as paid_date,
    extract(year from p.paid_at)             as paid_year,
    extract(quarter from p.paid_at)          as paid_quarter,
    extract(month from p.paid_at)            as paid_month,
    extract(week from p.paid_at)             as paid_week,
    o.priority                               as order_priority,
    o.market_segment                         as customer_segment,
    o.customer_segment_lifecycle,
    current_timestamp()                      as fact_last_refreshed_at
from payments p
inner join orders_lookup o on p.order_id = o.order_id
