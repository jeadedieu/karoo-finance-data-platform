-- Payments in major units (rand, dollars...) with proper types and local dates.
select
    payment_id,
    order_id,
    upper(currency)                                         as currency,
    (amount::bigint / 100.0)::numeric(14, 2)                as amount,        -- 125000 cents -> 1250.00
    status,
    created_at::timestamptz                                 as created_at,
    updated_at::timestamptz                                 as updated_at,
    (created_at::timestamptz at time zone '{{ var("local_timezone") }}')::date as payment_date_local
from {{ source('raw', 'payments') }}
