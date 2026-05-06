{{ config(materialized='view') }}

-- Clean customer master records from TPC-H. Renames cryptic c_* columns to
-- readable names, parses the embedded segment from comment-style metadata.

select
    c_custkey                                as customer_id,
    trim(c_name)                             as full_name,
    trim(c_address)                          as address,
    c_nationkey                              as nation_id,
    c_phone                                  as phone,
    cast(c_acctbal as number(12, 2))         as account_balance_usd,
    c_mktsegment                             as market_segment,
    trim(c_comment)                          as customer_notes
from {{ source('tpch', 'customer') }}
where c_custkey is not null
