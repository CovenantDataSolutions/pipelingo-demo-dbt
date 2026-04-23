with source as (
    select * from {{ ref('raw_orders') }}
)

select
    order_id,
    customer_id,
    cast(order_date as date) as order_date,
    status,
    promotion_id,
    shipping_address_country,
    case when status = 'completed' then true else false end as is_completed
from source
