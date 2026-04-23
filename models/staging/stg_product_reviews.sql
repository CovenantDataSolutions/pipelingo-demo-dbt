with source as (
    select * from {{ ref('raw_product_reviews') }}
)

select
    review_id,
    product_id,
    customer_id,
    rating,
    review_text,
    cast(review_date as date) as review_date,
    case when rating >= 4 then true else false end as is_positive
from source
