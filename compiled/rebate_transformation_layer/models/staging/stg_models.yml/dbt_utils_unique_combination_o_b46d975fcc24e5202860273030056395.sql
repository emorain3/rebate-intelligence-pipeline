





with validation_errors as (

    select
        transaction_id, partner_id, memo
    from DEV_MCP_DB.PUBLIC.stg_transactions
    group by transaction_id, partner_id, memo
    having count(*) > 1

)

select *
from validation_errors


