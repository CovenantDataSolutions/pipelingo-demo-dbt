{{ config(materialized='table') }}

-- Daily revenue rollup with day-over-day comparison. This is what powers
-- the morning standup dashboard tile.

with daily as (
    select
        paid_date,
        count(*)                              as payment_count,
        count(distinct order_id)              as orders_paid,
        count(distinct customer_id)           as paying_customers,
        sum(revenue_usd)                      as revenue_usd,
        avg(revenue_usd)                      as avg_payment_usd
    from {{ ref('fct_revenue') }}
    group by 1
)

select
    paid_date,
    payment_count,
    orders_paid,
    paying_customers,
    round(revenue_usd, 2)                    as revenue_usd,
    round(avg_payment_usd, 2)                as avg_payment_usd,
    round(lag(revenue_usd) over (order by paid_date), 2) as prior_day_revenue_usd,
    round(
        revenue_usd - lag(revenue_usd) over (order by paid_date),
        2
    )                                        as day_over_day_change_usd,
    round(
        100.0 * (revenue_usd - lag(revenue_usd) over (order by paid_date))
            / nullif(lag(revenue_usd) over (order by paid_date), 0),
        2
    )                                        as day_over_day_change_pct,
    -- 7-day rolling average — smooths out the weekday noise
    round(
        avg(revenue_usd) over (order by paid_date rows between 6 preceding and current row),
        2
    )                                        as rolling_7d_avg_usd
from daily
order by paid_date desc
