-- Intermediate Layer: Rebate table Deduplication and Anaomaly Detection
-- This model performs row-level deduplication of rebate transactions
-- and flags anomalies to be interpreted by the gold/marts layer 
-- Current anomalies tracked:  
-- - null_amount: Transacted amount is missing or null 
-- - negative_amount: Transacted amount falls below $0.00 
-- - zero_amount: Transacted amount is exactly $0.00 
-- - null_memo: Transaction is missing descriptive tracking notes 
-- - null_partner: Transaction is missing the partner_id identifier 
-- Finally, this table uses a composite surrogate key to prevent tracking collisions and
-- provides standardized calendar dimensions (year/month) for downstream analysis.


with staging_transactions as (
    select * from {{ ref('stg_transactions') }}
),

deduplicated as (
    select
        transaction_id,
        affiliate_id,
        partner_id,
        -- Date standardization
        cast(transaction_date as date) as transaction_date,
        extract(year from cast(transaction_date as date)) as transaction_year,
        extract(month from cast(transaction_date as date)) as transaction_month,
        memo,
        net_amount,
        md5(cast(coalesce(cast(transaction_id as varchar), '') || '-' ||
                 coalesce(cast(partner_id as varchar), '') || '-' ||
                 coalesce(cast(memo as varchar), '') as varchar)) as unique_key,
        row_number() over (
            partition by transaction_id, partner_id, memo, net_amount
            order by cast(transaction_date as date) desc
        ) as row_num
    from staging_transactions
),

flagged as (
    select
        unique_key,
        transaction_id,
        affiliate_id,
        partner_id,
        transaction_date,
        transaction_year,
        transaction_month,
        memo,
        net_amount,
        -- Exception typing at the row level
        -- Silver identifies WHAT is anomalous, Gold determines WHY
        case
            when net_amount is null then 'null_amount'
            when net_amount < 0.00 then 'negative_amount'
            when net_amount = 0.00 then 'zero_amount'
            when memo is null then 'null_memo'
            when partner_id is null then 'null_partner'
            else null
        end as exception_type,
        case
            when net_amount is null
              or net_amount <= 0.00
              or memo is null
              or partner_id is null
            then true
            else false
        end as is_exception
    from deduplicated
    where row_num = 1
)

select * from flagged