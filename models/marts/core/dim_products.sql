{{ config(materialized='table') }}

-- Product dimension with denormalized sales-tier classification baked in.

select
    product_id,
    product_name,
    brand,
    manufacturer,
    quality_tier,
    material,
    product_size,
    retail_price_usd,
    units_sold,
    gross_revenue_usd,
    return_rate_pct,
    sales_tier,
    case
        when sales_tier in ('bestseller', 'strong') then 'core_catalog'
        when sales_tier in ('steady', 'slow')       then 'long_tail'
        else 'unsold'
    end                                      as catalog_role,
    current_timestamp()                      as dim_last_refreshed_at
from {{ ref('int_product_performance') }}
