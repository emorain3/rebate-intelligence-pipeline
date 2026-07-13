
    
    

with child as (
    select affiliate_id as from_field
    from DEV_MCP_DB.PUBLIC.fact_rebate_payouts
    where affiliate_id is not null
),

parent as (
    select affiliate_id as to_field
    from DEV_MCP_DB.PUBLIC.dim_partners
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


