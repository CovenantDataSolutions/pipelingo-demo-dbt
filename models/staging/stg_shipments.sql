with source as (
    select * from {{ ref('raw_shipments') }}
)

select
    shipment_id,
    order_id,
    carrier,
    tracking_number,
    cast(shipped_at as date) as shipped_at,
    cast(delivered_at as date) as delivered_at,
    status as shipment_status,
    case
        when delivered_at is not null and shipped_at is not null
            then datediff('day', cast(shipped_at as date), cast(delivered_at as date))
        else null
    end as transit_days
from source
