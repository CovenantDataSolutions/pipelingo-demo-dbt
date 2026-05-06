{{ config(materialized='view') }}

-- Shipment events. Computes delivery time in days where applicable; null
-- for shipments still in transit or returned.

select
    shipment_id,
    order_id,
    carrier,
    tracking_number,
    status                                   as shipment_status,
    cast(shipped_at as timestamp_ntz)        as shipped_at,
    cast(delivered_at as timestamp_ntz)      as delivered_at,
    case when status = 'delivered' then true else false end  as is_delivered,
    case when status = 'returned' then true else false end   as is_returned,
    case
        when delivered_at is not null
            then datediff('day', cast(shipped_at as timestamp_ntz), cast(delivered_at as timestamp_ntz))
        else null
    end                                      as delivery_time_days
from {{ source('raw', 'raw_shipments') }}
where shipment_id is not null
