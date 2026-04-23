with source as (
    select * from {{ ref('raw_promotions') }}
)

select
    promotion_id,
    promotion_code,
    description,
    discount_percent,
    cast(valid_from as date) as valid_from,
    cast(valid_to as date) as valid_to
from source
