{{ config(materialized='table') }}

-- Customer dimension. Slowly-changing? No — TPC-H is static. In a real
-- warehouse this would be Type 2 SCD with effective_from/effective_to.

select
    customer_id,
    full_name,
    market_segment,
    customer_segment_lifecycle               as lifetime_segment,
    total_orders,
    total_revenue_usd,
    avg_order_value_usd,
    first_order_date,
    last_order_date,
    is_active,
    acquisition_campaign_id,
    acquired_at,
    delivered_orders,
    returned_orders,
    case
        when total_orders > 0
            then round((returned_orders::float / total_orders) * 100, 2)
        else 0
    end                                      as return_rate_pct,
    current_timestamp()                      as dim_last_refreshed_at
from {{ ref('int_customer_lifetime') }}
