
    
    

select
    rebate_payout_id as unique_field,
    count(*) as n_records

from DEV_MCP_DB.PUBLIC.fact_rebate_payouts
where rebate_payout_id is not null
group by rebate_payout_id
having count(*) > 1


