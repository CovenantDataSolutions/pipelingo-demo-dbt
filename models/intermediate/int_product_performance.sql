-- Per-product sales rollup. Joins products with all their line items to
-- compute units sold, revenue, return rate, etc.

with products as (
    select * from {{ ref('stg_products') }}
),

product_sales as (
    select
        product_id,
        count(distinct order_id)              as orders_containing_product,
        sum(quantity)                         as units_sold,
        sum(line_total_usd)                   as gross_revenue_usd,
        sum(case when was_returned then quantity else 0 end) as units_returned,
        sum(case when was_returned then line_total_usd else 0 end) as returned_revenue_usd,
        avg(line_total_usd / nullif(quantity, 0)) as avg_unit_price_usd
    from {{ ref('stg_order_items') }}
    group by 1
)

select
    p.product_id,
    p.product_name,
    p.brand,
    p.manufacturer,
    p.quality_tier,
    p.material,
    p.product_size,
    p.retail_price_usd,
    coalesce(s.units_sold, 0)                as units_sold,
    coalesce(s.orders_containing_product, 0) as orders_containing_product,
    coalesce(s.gross_revenue_usd, 0)         as gross_revenue_usd,
    coalesce(s.units_returned, 0)            as units_returned,
    coalesce(s.returned_revenue_usd, 0)      as returned_revenue_usd,
    s.avg_unit_price_usd,
    case
        when s.units_sold is null or s.units_sold = 0 then 0
        else round((s.units_returned::float / s.units_sold) * 100, 2)
    end                                      as return_rate_pct,
    case
        when coalesce(s.units_sold, 0) >= 10000 then 'bestseller'
        when coalesce(s.units_sold, 0) >= 1000  then 'strong'
        when coalesce(s.units_sold, 0) >= 100   then 'steady'
        when coalesce(s.units_sold, 0) > 0      then 'slow'
        else 'no_sales'
    end                                      as sales_tier
from products p
left join product_sales s on p.product_id = s.product_id
