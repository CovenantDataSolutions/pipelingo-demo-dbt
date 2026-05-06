{{ config(materialized='table') }}

-- Marketing ROI per campaign. Joins click attribution to revenue from
-- those acquired customers. The "did our marketing money work" report.

with campaign_clicks as (
    select
        campaign_id,
        count(*)                              as click_count,
        count(distinct customer_id)           as unique_clickers,
        sum(case when did_convert then 1 else 0 end) as conversions
    from {{ ref('stg_marketing_clicks') }}
    group by 1
),

attributed_revenue as (
    -- Revenue from customers acquired by this campaign
    select
        c.acquisition_campaign_id            as campaign_id,
        sum(c.total_revenue_usd)             as attributed_revenue_usd,
        count(*)                             as attributed_customer_count
    from {{ ref('int_customer_lifetime') }} c
    where c.acquisition_campaign_id is not null
    group by 1
)

select
    cmp.campaign_id,
    cmp.campaign_name,
    cmp.channel,
    cmp.campaign_type,
    cmp.budget_usd,
    cmp.duration_days,
    coalesce(cl.click_count, 0)              as click_count,
    coalesce(cl.unique_clickers, 0)          as unique_clickers,
    coalesce(cl.conversions, 0)              as conversions,
    case
        when cl.click_count > 0
            then round(100.0 * cl.conversions / cl.click_count, 2)
        else 0
    end                                      as conversion_rate_pct,
    coalesce(ar.attributed_customer_count, 0) as customers_acquired,
    coalesce(round(ar.attributed_revenue_usd, 2), 0) as attributed_revenue_usd,
    case
        when cmp.budget_usd > 0 and ar.attributed_revenue_usd is not null
            then round(ar.attributed_revenue_usd / cmp.budget_usd, 2)
        else null
    end                                      as roi_multiplier,
    case
        when cmp.budget_usd > 0 and cl.click_count > 0
            then round(cmp.budget_usd / cl.click_count, 2)
        else null
    end                                      as cost_per_click_usd,
    case
        when ar.attributed_customer_count > 0
            then round(cmp.budget_usd / ar.attributed_customer_count, 2)
        else null
    end                                      as cost_per_acquisition_usd
from {{ ref('stg_marketing_campaigns') }} cmp
left join campaign_clicks cl     on cmp.campaign_id = cl.campaign_id
left join attributed_revenue ar  on cmp.campaign_id = ar.campaign_id
order by cmp.budget_usd desc
