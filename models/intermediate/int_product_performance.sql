with products as (
    select * from {{ ref('stg_products') }}
),

suppliers as (
    select * from {{ ref('stg_suppliers') }}
),

order_items as (
    select * from {{ ref('stg_order_items') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

reviews as (
    select
        product_id,
        count(review_id) as review_count,
        avg(rating) as avg_rating,
        sum(case when is_positive then 1 else 0 end) as positive_review_count
    from {{ ref('stg_product_reviews') }}
    group by 1
),

sales as (
    select
        oi.product_id,
        sum(oi.quantity) as units_sold,
        sum(oi.line_total_usd) as revenue_usd
    from order_items oi
    join orders o on oi.order_id = o.order_id
    where o.is_completed
    group by 1
)

select
    p.product_id,
    p.product_name,
    p.category,
    p.supplier,
    p.price_usd,
    p.gross_margin_usd,
    p.in_stock,
    s.supplier_country,
    s.is_preferred as supplier_is_preferred,
    coalesce(sl.units_sold, 0) as units_sold,
    coalesce(sl.revenue_usd, 0) as revenue_usd,
    coalesce(r.review_count, 0) as review_count,
    r.avg_rating,
    coalesce(r.positive_review_count, 0) as positive_review_count
from products p
left join suppliers s on p.supplier = s.supplier_name
left join sales sl on p.product_id = sl.product_id
left join reviews r on p.product_id = r.product_id
