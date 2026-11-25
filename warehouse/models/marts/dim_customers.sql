-- One row per real PERSON (duplicate accounts merged into the oldest one).
with accounts as (
    select * from {{ ref('stg_customers') }}
),

-- If the main account has no country, borrow it from one of the person's other accounts
known_country as (
    select canonical_customer_id, max(country) as country
    from accounts
    group by 1
)

select
    a.customer_id,
    a.first_name,
    a.last_name,
    a.email,
    coalesce(a.country, k.country)  as country,
    a.signup_date,
    n.accounts                      as accounts_merged
from accounts a
join known_country k on k.canonical_customer_id = a.customer_id
join (select canonical_customer_id, count(*) as accounts from accounts group by 1) n
    on n.canonical_customer_id = a.customer_id
where a.customer_id = a.canonical_customer_id
