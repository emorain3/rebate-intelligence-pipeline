with rebates as (
    select * from DEV_MCP_DB.PUBLIC.int_rebates
)

select
    unique_key as rebate_payout_id,
    transaction_id,
    affiliate_id,
    partner_id,
    transaction_date,
    transaction_year,
    transaction_month,
    memo,
    net_amount as rebate_amount
from rebates
where is_exception = false