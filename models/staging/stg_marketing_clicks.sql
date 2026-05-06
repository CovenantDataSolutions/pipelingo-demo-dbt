{{ config(materialized='view') }}

-- Click events from marketing campaigns. Anonymous clicks (no customer_id)
-- are kept but flagged so attribution models can choose to include them
-- or not.

select
    click_id,
    campaign_id,
    customer_id,
    cast(clicked_at as timestamp_ntz)        as clicked_at,
    source_url,
    cast(converted as boolean)               as did_convert,
    case when customer_id is null then true else false end as is_anonymous
from {{ source('raw', 'raw_marketing_clicks') }}
where click_id is not null
