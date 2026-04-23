with lifetime as (
    select * from {{ ref('int_customer_lifetime') }}
)

select
    customer_id,
    full_name,
    email,
    signup_date,
    region,
    is_active,
    total_orders,
    lifetime_value_usd,
    first_order_date,
    last_order_date,
    reviews_submitted,
    avg_rating_given,
    case
        when total_orders = 0 then 'prospect'
        when total_orders = 1 then 'new'
        when total_orders between 2 and 3 then 'returning'
        else 'vip'
    end as customer_segment
from lifetime
