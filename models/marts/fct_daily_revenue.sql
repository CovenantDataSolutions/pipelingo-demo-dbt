with orders_enriched as (
    select * from {{ ref('int_orders_enriched') }}
)

select
    order_date as revenue_date,
    count(distinct order_id) as order_count,
    count(distinct customer_id) as unique_customers,
    sum(item_count) as items_sold,
    sum(case when is_completed then paid_amount_usd else 0 end) as revenue_usd,
    sum(case when promotion_code is not null then 1 else 0 end) as promoted_order_count
from orders_enriched
group by 1
order by 1
