with rebates as (
    select * from {{ ref('int_rebates') }}
),

dim_partners as (
    select * from {{ ref('dim_partners') }}
)

select
    dp.affiliate_id,
    dp.affiliate_name,
    dp.state,
    dp.program_tier,
    dp.partner_name,
    count(r.unique_key) as total_transactions,
    count(case when r.is_exception = false then 1 end) as clean_transactions,
    count(case when r.is_exception = true then 1 end) as exception_count,
    count(case when r.exception_type = 'zero_amount' then 1 end) as zero_amount_count,
    count(case when r.exception_type = 'null_partner' then 1 end) as null_partner_count,
    sum(case when r.is_exception = false then r.net_amount else 0 end) as total_rebate_amount,
    
    case 
    when 
        -- Affiliate has transactions
        count(r.unique_key) > 0
        -- AND every transaction has either zero, null, or negative amount
        -- avoid memo/partner issue flags as indicative of being a shop silent
        and count(case when r.exception_type not in ('zero_amount', 'null_amount', 'negative_amount') 
                       and r.is_exception = false 
                       then 1 end) = 0
        and count(case when r.exception_type in ('zero_amount', 'null_amount', 'negative_amount') 
                       then 1 end) > 0
    then true
    else false
end as is_silent_shop

from dim_partners dp
left join rebates r on dp.affiliate_id = r.affiliate_id
group by 1, 2, 3, 4, 5