
    
    

select
    unique_key as unique_field,
    count(*) as n_records

from DEV_MCP_DB.PUBLIC.int_rebates
where unique_key is not null
group by unique_key
having count(*) > 1


