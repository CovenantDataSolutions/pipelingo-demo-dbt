with performance as (
    select * from {{ ref('int_product_performance') }}
)

select
    product_id,
    product_name,
    category,
    supplier,
    supplier_country,
    supplier_is_preferred,
    price_usd,
    gross_margin_usd,
    in_stock,
    units_sold,
    revenue_usd,
    review_count,
    avg_rating,
    positive_review_count,
    case
        when units_sold = 0 then 'unsold'
        when units_sold < 3 then 'slow_mover'
        when units_sold between 3 and 5 then 'steady'
        else 'bestseller'
    end as sales_tier
from performance
