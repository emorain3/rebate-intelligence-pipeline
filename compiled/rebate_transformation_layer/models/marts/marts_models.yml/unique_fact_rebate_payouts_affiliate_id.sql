
    
    

select
    affiliate_id as unique_field,
    count(*) as n_records

from DEV_MCP_DB.PUBLIC.fact_rebate_payouts
where affiliate_id is not null
group by affiliate_id
having count(*) > 1


