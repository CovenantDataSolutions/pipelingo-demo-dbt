{{ config(materialized='table') }}

-- Order-grain fact table. The center of the star schema — joins to
-- dim_customers and dim_products via order_items.

select
    order_id,
    customer_id,
    order_date,
    extract(year from order_date)            as order_year,
    extract(quarter from order_date)         as order_quarter,
    extract(month from order_date)           as order_month,
    extract(dow from order_date)             as order_day_of_week,
    order_status,
    economic_status,
    priority,
    is_completed,
    is_delivered,
    has_return,
    item_count,
    total_quantity,
    items_subtotal_usd,
    captured_amount_usd                      as revenue_usd,
    order_total_usd,
    payment_attempt_count,
    problem_payment_count,
    case
        when problem_payment_count > 0 and not is_completed then 'at_risk'
        when problem_payment_count > 0                      then 'recovered'
        else 'clean'
    end                                      as payment_health,
    latest_shipped_at,
    latest_delivered_at,
    case
        when latest_delivered_at is not null
            then datediff('day', order_date, latest_delivered_at)
        else null
    end                                      as days_to_delivery,
    current_timestamp()                      as fact_last_refreshed_at
from {{ ref('int_orders_enriched') }}
