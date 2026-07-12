with staging_partners as (
    select * from {{ ref('stg_partners') }}
)

select
    partner_id,
    upper(trim(partner_name)) as partner_name,
    upper(trim(partner_type)) as partner_type
from staging_partners