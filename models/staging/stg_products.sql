{{ config(materialized='view') }}

-- Product catalog. TPC-H part data is realistic — manufacturer, brand,
-- container, retail price. We split the type field into a category
-- hierarchy so reports can roll up at different levels.

select
    p_partkey                                as product_id,
    trim(p_name)                             as product_name,
    p_mfgr                                   as manufacturer,
    p_brand                                  as brand,
    p_type                                   as product_type,
    -- type is shaped like "ECONOMY ANODIZED STEEL" — first word is tier
    split_part(p_type, ' ', 1)               as quality_tier,
    -- last word is the material
    split_part(p_type, ' ', -1)              as material,
    cast(p_size as integer)                  as product_size,
    p_container                              as container,
    cast(p_retailprice as number(12, 2))     as retail_price_usd,
    trim(p_comment)                          as product_notes
from {{ source('tpch', 'part') }}
where p_partkey is not null
