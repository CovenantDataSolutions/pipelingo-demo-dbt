{{ config(materialized='view') }}

-- Order line items. TPC-H's lineitem table is ~6M rows at SF1; we clean
-- it up and compute the line total here so downstream models don't have
-- to repeat the math.

select
    l_orderkey || '-' || l_linenumber        as order_item_id,
    l_orderkey                               as order_id,
    l_partkey                                as product_id,
    l_suppkey                                as supplier_id,
    l_linenumber                             as line_number,
    cast(l_quantity as number(8, 2))         as quantity,
    cast(l_extendedprice as number(12, 2))   as gross_amount_usd,
    cast(l_discount as number(5, 4))         as discount_pct,
    cast(l_tax as number(5, 4))              as tax_pct,
    cast(l_extendedprice * (1 - l_discount) * (1 + l_tax) as number(12, 2)) as line_total_usd,
    l_shipdate                               as shipped_date,
    l_commitdate                             as committed_date,
    l_receiptdate                            as receipt_date,
    case l_returnflag
        when 'R' then true
        when 'A' then true
        else false
    end                                      as was_returned,
    case l_linestatus
        when 'O' then 'open'
        when 'F' then 'fulfilled'
        else 'unknown'
    end                                      as line_status
from {{ source('tpch', 'lineitem') }}
where l_orderkey is not null
