
    
    

select
    affiliate_id as unique_field,
    count(*) as n_records

from DEV_MCP_DB.PUBLIC.dim_partners
where affiliate_id is not null
group by affiliate_id
having count(*) > 1


