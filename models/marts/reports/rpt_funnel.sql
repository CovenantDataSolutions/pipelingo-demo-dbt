{{ config(materialized='table') }}

-- Marketing funnel by channel: clicks -> conversions -> first orders ->
-- revenue. Lets the marketing team see drop-off at each stage and which
-- channels have the strongest top-of-funnel vs strongest conversion.

with channel_stats as (
    select
        cmp.channel,
        cmp.campaign_type,
        count(distinct cmp.campaign_id)       as campaign_count,
        sum(cmp.budget_usd)                   as total_budget_usd
    from {{ ref('stg_marketing_campaigns') }} cmp
    group by 1, 2
),

clicks_per_channel as (
    select
        cmp.channel,
        cmp.campaign_type,
        count(*)                              as total_clicks,
        count(distinct mc.customer_id)        as unique_clickers,
        sum(case when mc.did_convert then 1 else 0 end) as conversions
    from {{ ref('stg_marketing_clicks') }} mc
    join {{ ref('stg_marketing_campaigns') }} cmp on mc.campaign_id = cmp.campaign_id
    group by 1, 2
),

revenue_per_channel as (
    select
        cmp.channel,
        cmp.campaign_type,
        count(distinct cl.customer_id)        as paying_customers,
        sum(cl.total_revenue_usd)             as channel_revenue_usd
    from {{ ref('int_customer_lifetime') }} cl
    join {{ ref('stg_marketing_campaigns') }} cmp on cl.acquisition_campaign_id = cmp.campaign_id
    group by 1, 2
)

select
    cs.channel,
    cs.campaign_type,
    cs.campaign_count,
    cs.total_budget_usd,
    coalesce(cpc.total_clicks, 0)            as total_clicks,
    coalesce(cpc.unique_clickers, 0)         as unique_clickers,
    coalesce(cpc.conversions, 0)             as conversions,
    coalesce(rpc.paying_customers, 0)        as paying_customers,
    coalesce(round(rpc.channel_revenue_usd, 2), 0) as channel_revenue_usd,
    -- Funnel rates
    case when cpc.total_clicks > 0
        then round(100.0 * cpc.conversions / cpc.total_clicks, 2)
        else 0
    end                                      as click_to_conversion_pct,
    case when cpc.conversions > 0
        then round(100.0 * rpc.paying_customers / cpc.conversions, 2)
        else 0
    end                                      as conversion_to_paid_pct,
    case when cs.total_budget_usd > 0 and rpc.channel_revenue_usd is not null
        then round(rpc.channel_revenue_usd / cs.total_budget_usd, 2)
        else null
    end                                      as channel_roi
from channel_stats cs
left join clicks_per_channel cpc   on cs.channel = cpc.channel and cs.campaign_type = cpc.campaign_type
left join revenue_per_channel rpc  on cs.channel = rpc.channel and cs.campaign_type = rpc.campaign_type
order by channel_revenue_usd desc nulls last
