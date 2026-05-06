{{ config(materialized='table') }}

-- Single-row monthly executive summary. Designed to fit on one slide.
-- Read by the CEO and CFO; failures here propagate immediately to the
-- "wrong number on a board deck" failure mode that justifies the whole
-- observability pipeline.

with revenue_by_month as (
    select
        date_trunc('month', paid_date)       as report_month,
        sum(revenue_usd)                     as revenue_usd,
        count(distinct order_id)             as orders_paid,
        count(distinct customer_id)          as paying_customers
    from {{ ref('fct_revenue') }}
    group by 1
),

orders_by_month as (
    select
        date_trunc('month', order_date)      as report_month,
        count(*)                             as orders_placed,
        sum(case when has_return then 1 else 0 end) as orders_returned,
        avg(days_to_delivery)                as avg_delivery_days
    from {{ ref('fct_orders') }}
    group by 1
),

customer_acquisition_by_month as (
    select
        date_trunc('month', acquired_at)     as report_month,
        count(*)                             as new_customers
    from {{ ref('int_customer_lifetime') }}
    where acquired_at is not null
    group by 1
)

select
    coalesce(r.report_month, o.report_month, c.report_month) as report_month,
    coalesce(round(r.revenue_usd, 2), 0)     as revenue_usd,
    coalesce(r.orders_paid, 0)               as orders_paid,
    coalesce(o.orders_placed, 0)             as orders_placed,
    coalesce(o.orders_returned, 0)           as orders_returned,
    case when o.orders_placed > 0
        then round(100.0 * o.orders_returned / o.orders_placed, 2)
        else 0
    end                                      as return_rate_pct,
    coalesce(r.paying_customers, 0)          as paying_customers,
    coalesce(c.new_customers, 0)             as new_customers,
    round(o.avg_delivery_days, 1)            as avg_delivery_days,
    -- Month-over-month revenue change
    round(
        100.0 * (r.revenue_usd - lag(r.revenue_usd) over (order by r.report_month))
            / nullif(lag(r.revenue_usd) over (order by r.report_month), 0),
        2
    )                                        as revenue_mom_change_pct
from revenue_by_month r
full outer join orders_by_month o                 on r.report_month = o.report_month
full outer join customer_acquisition_by_month c   on coalesce(r.report_month, o.report_month) = c.report_month
order by report_month desc
