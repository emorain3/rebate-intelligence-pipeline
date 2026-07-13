
    
    

select
    partner_id as unique_field,
    count(*) as n_records

from DEV_MCP_DB.PUBLIC.dim_partners
where partner_id is not null
group by partner_id
having count(*) > 1


