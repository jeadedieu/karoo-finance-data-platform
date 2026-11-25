-- Every customer ACCOUNT, cleaned. Some people signed up twice (same email, different
-- capitalisation): both accounts are kept, because orders may exist on either one,
-- and each account points to the person's oldest account (canonical_customer_id).
with cleaned as (
    select
        customer_id,
        first_name,
        last_name,
        lower(trim(email))          as email,
        nullif(trim(country), '')   as country,
        signup_date::date           as signup_date
    from {{ source('raw', 'customers') }}
)

select
    *,
    first_value(customer_id) over (partition by email order by signup_date, customer_id) as canonical_customer_id
from cleaned
