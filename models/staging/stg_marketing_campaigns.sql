{{ config(materialized='view') }}

-- Marketing campaign metadata. Computes campaign duration and active flag
-- so downstream reports can filter to "live" campaigns easily.

select
    campaign_id,
    trim(campaign_name)                      as campaign_name,
    channel,
    campaign_type,
    cast(budget_usd as number(12, 2))        as budget_usd,
    cast(started_at as timestamp_ntz)        as started_at,
    cast(ended_at as timestamp_ntz)          as ended_at,
    datediff('day', cast(started_at as timestamp_ntz), cast(ended_at as timestamp_ntz)) as duration_days,
    case
        when current_timestamp() between cast(started_at as timestamp_ntz) and cast(ended_at as timestamp_ntz)
            then true
        else false
    end                                      as is_active
from {{ source('raw', 'raw_marketing_campaigns') }}
where campaign_id is not null
