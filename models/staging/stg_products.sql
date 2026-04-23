with source as (
    select * from {{ ref('raw_products') }}
)

select
    product_id,
    product_name,
    category,
    supplier,
    price_usd,
    cost_usd,
    (price_usd - cost_usd) as gross_margin_usd,
    in_stock
from source
