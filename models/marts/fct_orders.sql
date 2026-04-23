with orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

customers as (
    select customer_id, full_name, region from {{ ref('stg_customers') }}
),

shipments as (
    select order_id, carrier, shipment_status, transit_days
    from {{ ref('stg_shipments') }}
)

select
    oe.order_id,
    oe.order_date,
    oe.customer_id,
    c.full_name as customer_name,
    c.region as customer_region,
    oe.shipping_address_country,
    oe.status as order_status,
    oe.is_completed,
    oe.item_count,
    oe.total_quantity,
    oe.subtotal_usd,
    oe.paid_amount_usd,
    oe.promotion_code,
    oe.discount_percent,
    sh.carrier,
    sh.shipment_status,
    sh.transit_days
from orders_enriched oe
left join customers c on oe.customer_id = c.customer_id
left join shipments sh on oe.order_id = sh.order_id
