with source as (
    select * from {{ ref('raw_payments') }}
)

select
    payment_id,
    order_id,
    payment_method,
    amount_usd,
    cast(paid_at as date) as paid_at,
    status as payment_status,
    case when status = 'captured' then true else false end as is_captured
from source
