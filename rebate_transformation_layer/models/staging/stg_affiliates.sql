with source_data as (
    select * from {{ source('bronze_source', 'synthetic_dim_affiliate') }}
)

select 
    cast(affiliate_id as varchar) as affiliate_id,
    cast(affiliate_name as varchar) as affiliate_name,
    cast(parent_id as varchar) as parent_id,
    cast(state as varchar) as state,
    cast(program_tier as varchar) as program_tier
from source_data