{{ config(materialized='table') }}

-- Customer LTV summary by lifetime segment. Powers the "who are our best
-- customers" exec dashboard.

select
    lifetime_segment,
    market_segment,
    count(*)                                 as customer_count,
    sum(case when is_active then 1 else 0 end) as active_customer_count,
    round(sum(total_revenue_usd), 2)         as total_revenue_usd,
    round(avg(total_revenue_usd), 2)         as avg_ltv_usd,
    round(percentile_cont(0.5) within group (order by total_revenue_usd), 2) as median_ltv_usd,
    round(avg(avg_order_value_usd), 2)       as avg_aov_usd,
    round(avg(total_orders), 1)              as avg_orders_per_customer,
    round(avg(return_rate_pct), 2)           as avg_return_rate_pct
from {{ ref('dim_customers') }}
group by 1, 2
order by total_revenue_usd desc
