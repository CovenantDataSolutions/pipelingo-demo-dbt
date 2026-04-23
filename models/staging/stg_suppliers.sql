with source as (
    select * from {{ ref('raw_suppliers') }}
)

select
    supplier_id,
    supplier_name,
    country as supplier_country,
    contact_email,
    is_preferred
from source
