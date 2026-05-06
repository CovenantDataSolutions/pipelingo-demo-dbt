-- Customer-level lifetime aggregates. Joins enriched orders + clicks back
-- to the customer so we can compute LTV, last-seen, and acquisition channel.

with customers as (
    select * from {{ ref('stg_customers') }}
),

order_aggregates as (
    select
        customer_id,
        count(*)                               as total_orders,
        sum(captured_amount_usd)               as total_revenue_usd,
        min(order_date)                        as first_order_date,
        max(order_date)                        as last_order_date,
        sum(case when is_delivered then 1 else 0 end)  as delivered_orders,
        sum(case when has_return then 1 else 0 end)    as returned_orders,
        avg(captured_amount_usd)               as avg_order_value_usd
    from {{ ref('int_orders_enriched') }}
    where customer_id is not null
    group by 1
),

acquisition as (
    -- First-touch attribution: which marketing campaign brought this
    -- customer in (the campaign that drove their earliest click).
    select
        customer_id,
        campaign_id                            as acquisition_campaign_id,
        clicked_at                             as acquired_at
    from (
        select
            customer_id,
            campaign_id,
            clicked_at,
            row_number() over (partition by customer_id order by clicked_at) as click_rank
        from {{ ref('stg_marketing_clicks') }}
        where customer_id is not null
    )
    where click_rank = 1
)

select
    c.customer_id,
    c.full_name,
    c.market_segment,
    c.account_balance_usd,
    coalesce(o.total_orders, 0)              as total_orders,
    coalesce(o.total_revenue_usd, 0)         as total_revenue_usd,
    coalesce(o.delivered_orders, 0)          as delivered_orders,
    coalesce(o.returned_orders, 0)           as returned_orders,
    o.avg_order_value_usd,
    o.first_order_date,
    o.last_order_date,
    case when o.last_order_date >= dateadd('day', -90, current_date()) then true else false end as is_active,
    a.acquisition_campaign_id,
    a.acquired_at,
    case
        when o.total_orders is null or o.total_orders = 0 then 'never_ordered'
        when o.total_orders = 1                then 'one_time'
        when o.total_orders between 2 and 5    then 'repeat'
        else 'loyal'
    end                                      as customer_segment_lifecycle
from customers c
left join order_aggregates o on c.customer_id = o.customer_id
left join acquisition a      on c.customer_id = a.customer_id
