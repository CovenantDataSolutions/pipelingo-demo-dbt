-- Joins orders with their items, payments, and shipments into a single
-- order-grain view. Aggregates line items so we have one row per order.
-- This is the most-joined table in the project — most marts read from here.

with orders as (
    select * from {{ ref('stg_orders') }}
),

items_per_order as (
    select
        order_id,
        count(*)                               as item_count,
        sum(quantity)                          as total_quantity,
        sum(line_total_usd)                    as items_subtotal_usd,
        sum(case when was_returned then 1 else 0 end) as returned_item_count
    from {{ ref('stg_order_items') }}
    group by 1
),

payments_per_order as (
    select
        order_id,
        sum(case when is_captured then amount_usd else 0 end) as captured_amount_usd,
        max(case when is_captured then paid_at else null end) as last_captured_at,
        count(*)                               as payment_attempt_count,
        sum(case when is_problem then 1 else 0 end) as problem_payment_count
    from {{ ref('stg_payments') }}
    group by 1
),

shipments_per_order as (
    select
        order_id,
        max(shipped_at)                        as latest_shipped_at,
        max(delivered_at)                      as latest_delivered_at,
        max(case when is_delivered then 1 else 0 end) as has_delivery,
        max(case when is_returned then 1 else 0 end)  as has_return
    from {{ ref('stg_shipments') }}
    group by 1
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.order_status,
    o.priority,
    o.is_completed,
    o.order_total_usd,
    -- Item-level aggregates
    coalesce(it.item_count, 0)               as item_count,
    coalesce(it.total_quantity, 0)           as total_quantity,
    coalesce(it.items_subtotal_usd, 0)       as items_subtotal_usd,
    coalesce(it.returned_item_count, 0)      as returned_item_count,
    -- Payment aggregates
    coalesce(p.captured_amount_usd, 0)       as captured_amount_usd,
    p.last_captured_at,
    coalesce(p.payment_attempt_count, 0)     as payment_attempt_count,
    coalesce(p.problem_payment_count, 0)     as problem_payment_count,
    case when p.captured_amount_usd > 0 then true else false end as has_payment,
    -- Shipment aggregates
    s.latest_shipped_at,
    s.latest_delivered_at,
    coalesce(s.has_delivery, 0) = 1          as is_delivered,
    coalesce(s.has_return, 0) = 1            as has_return,
    -- Realized order economics
    case
        when p.captured_amount_usd > 0 and o.is_completed then 'completed'
        when p.captured_amount_usd > 0 and not o.is_completed then 'paid_pending'
        when p.captured_amount_usd is null and o.is_completed then 'fulfilled_unpaid'
        else 'open'
    end                                      as economic_status
from orders o
left join items_per_order it      on o.order_id = it.order_id
left join payments_per_order p    on o.order_id = p.order_id
left join shipments_per_order s   on o.order_id = s.order_id
