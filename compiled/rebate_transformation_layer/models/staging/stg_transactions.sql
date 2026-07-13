with source_data as (
    select * from DEV_MCP_DB.PUBLIC.synthetic_fact_rebate
)

select 
    cast(transaction_id as varchar) as transaction_id,
    cast(affiliate_id as varchar) as affiliate_id,
    cast(partner_id as varchar) as partner_id,
    cast(transaction_date as date) as transaction_date,
    cast(memo as varchar) as memo,
    cast(net_amount as decimal(18, 2)) as net_amount
from source_data