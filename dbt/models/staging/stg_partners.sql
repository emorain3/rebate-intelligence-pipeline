with source_data as (
    select * from {{ source('bronze_source', 'synthetic_dim_partner') }}
)

select
    cast(partner_id as varchar) as partner_id,
    cast(partner_name as varchar) as partner_name,
    cast(partner_type as varchar) as partner_type
from source_data