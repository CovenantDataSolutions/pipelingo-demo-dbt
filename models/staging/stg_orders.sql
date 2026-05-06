{{ config(materialized='view') }}

-- Clean order headers. Maps TPC-H's terse status codes to readable strings.

select
    o_orderkey                               as order_id,
    o_custkey                                as customer_id,
    cast(o_totalprice as number(12, 2))      as order_total_usd,
    o_orderdate                              as order_date,
    case o_orderstatus
        when 'O' then 'open'
        when 'F' then 'fulfilled'
        when 'P' then 'pending'
        else 'unknown'
    end                                      as order_status,
    case o_orderpriority
        when '1-URGENT'   then 'urgent'
        when '2-HIGH'     then 'high'
        when '3-MEDIUM'   then 'medium'
        when '4-NOT SPECIFIED' then 'normal'
        when '5-LOW'      then 'low'
        else 'normal'
    end                                      as priority,
    trim(o_clerk)                            as sales_clerk,
    o_shippriority                           as shipping_priority,
    case when o_orderstatus = 'F' then true else false end as is_completed
from {{ source('tpch', 'orders') }}
where o_orderkey is not null
