with affiliates as (
    select * from DEV_MCP_DB.PUBLIC.int_affiliates
),

partners as (
    select * from DEV_MCP_DB.PUBLIC.int_partners
)

select
    a.affiliate_id,
    a.affiliate_name,
    a.parent_id,
    a.state,
    a.program_tier,
    p.partner_id,
    p.partner_name,
    p.partner_type
from affiliates a
left join partners p on a.parent_id = p.partner_id