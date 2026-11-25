-- Every raw order row with clean types. Still contains duplicates and bad rows:
-- deduplication and validation happen in the intermediate layer.
with source as (
    select * from {{ source('raw', 'orders') }}
),

typed as (
    select
        order_id,

        -- Three timestamp formats exist. Timestamps without a timezone are SA local time.
        case
            when order_ts ~ '[+-]\d{2}:\d{2}$'                    -- JSON era: 2026-06-01T10:58:59+02:00
                then order_ts::timestamptz
            when order_ts ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'     -- legacy:   01/11/2025 14:03
                then to_timestamp(order_ts, 'DD/MM/YYYY HH24:MI')::timestamp at time zone '{{ var("local_timezone") }}'
            when order_ts ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'  -- CSV era: 2025-11-01 14:03:22
                then order_ts::timestamp at time zone '{{ var("local_timezone") }}'
        end                                                       as ordered_at,

        case
            when updated_ts ~ '[+-]\d{2}:\d{2}$' then updated_ts::timestamptz
            else updated_ts::timestamp at time zone '{{ var("local_timezone") }}'
        end                                                       as updated_at,

        nullif(trim(customer_id), '')                             as customer_id,
        country,
        product_id,
        upper(trim(currency))                                     as currency_raw,

        -- 'R1,250.00' -> 1250.00 ; '1290' -> 1290.00
        nullif(regexp_replace(quantity, '\.0+$', ''), '')::integer                as quantity,
        nullif(regexp_replace(unit_price, '^R|,', '', 'g'), '')::numeric(12, 2)   as unit_price,
        nullif(regexp_replace(total_amount, '^R|,', '', 'g'), '')::numeric(12, 2) as total_amount,

        lower(status)                                             as status,
        _file_date,
        _file_row,
        _source
    from source
)

select
    typed.*,
    aliases.currency,                                             -- 'GPB' -> 'GBP', 'EURO' -> 'EUR'
    (ordered_at at time zone '{{ var("local_timezone") }}')::date as order_date_local
from typed
left join {{ ref('currency_aliases') }} as aliases
    on typed.currency_raw = aliases.raw_code
