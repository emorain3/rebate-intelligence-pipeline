with staging_affiliates as (
    select * from {{ ref('stg_affiliates') }}
)

select
    affiliate_id,
    upper(trim(affiliate_name)) as affiliate_name,
    parent_id,
    upper(trim(state)) as state,
    upper(trim(program_tier)) as program_tier
from staging_affiliates