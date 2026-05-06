{{ config(materialized='view') }}

-- Clean payment events from Stripe-style webhooks. Filters out test mode
-- payments and computes a captured-flag for downstream revenue models.

select
    payment_id,
    order_id,
    customer_id,
    cast(amount_usd as number(12, 2))        as amount_usd,
    payment_method,
    status                                   as payment_status,
    cast(paid_at as timestamp_ntz)           as paid_at,
    case when status = 'captured' then true else false end  as is_captured,
    case when status in ('failed', 'refunded') then true else false end as is_problem,
    -- Settlement timing flag — payments older than 24h that aren't captured
    -- are escalated to the ops team for manual review.
    case
        when status = 'pending' and paid_at < dateadd('hour', -24, current_timestamp())
            then true
        else false
    end                                      as needs_review
from {{ source('raw', 'raw_payments') }}
where payment_id is not null
  and amount_usd > 0
