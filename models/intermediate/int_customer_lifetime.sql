with customers as (
    select * from {{ ref('stg_customers') }}
),

orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
),

reviews as (
    select
        customer_id,
        count(review_id) as review_count,
        avg(rating) as avg_rating_given
    from {{ ref('stg_product_reviews') }}
    group by 1
),

customer_stats as (
    select
        customer_id,
        count(distinct order_id) as total_orders,
        sum(case when is_completed then paid_amount_usd else 0 end) as lifetime_value_usd,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from orders_enriched
    group by 1
)

select
    c.customer_id,
    c.full_name,
    c.email,
    c.signup_date,
    c.region,
    c.is_active,
    coalesce(cs.total_orders, 0) as total_orders,
    coalesce(cs.lifetime_value_usd, 0) as lifetime_value_usd,
    cs.first_order_date,
    cs.last_order_date,
    coalesce(r.review_count, 0) as reviews_submitted,
    r.avg_rating_given
from customers c
left join customer_stats cs on c.customer_id = cs.customer_id
left join reviews r on c.customer_id = r.customer_id
