with rebates as (
    select * from {{ ref('int_rebates') }}
),

affiliates as (
    select * from {{ ref('int_affiliates') }}
),

partners as (
    select * from {{ ref('dim_partners') }}
)

select
    a.affiliate_id,
    a.affiliate_name,
    a.state,
    a.program_tier,
    -- Aggregate partner name to keep exactly one row per affiliate_id
    max(p.partner_name) as partner_name,
    count(r.unique_key) as total_transactions,
    count(case when r.is_exception = false then 1 end) as clean_transactions,
    count(case when r.is_exception = true then 1 end) as exception_count,
    count(case when r.exception_type = 'zero_amount' then 1 end) as zero_amount_count,
    count(case when r.exception_type = 'null_partner' then 1 end) as null_partner_count,
    sum(case when r.is_exception = false then r.net_amount else 0 end) as total_rebate_amount,
    case 
        -- Affiliate has transactions
        when count(r.unique_key) > 0 
        -- AND every transaction has either zero, null, or negative amount
        -- avoid memo/partner issue flags as indicative of being a shop silent
        and count(case when r.exception_type not in ('zero_amount', 'null_amount', 'negative_amount') and r.is_exception = false then 1 end) = 0
        and count(case when r.exception_type in ('zero_amount', 'null_amount', 'negative_amount') then 1 end) > 0
        then true 
        else false 
    end as is_silent_shop
from affiliates a
left join rebates r on a.affiliate_id = r.affiliate_id
left join partners p on r.partner_id = p.partner_id
group by 1, 2, 3, 4 -- Group strictly by the affiliate dimension columns