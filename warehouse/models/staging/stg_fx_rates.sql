-- One row per published rate: 1 unit of `currency` = `rate_to_zar` rand.
select
    date::date                          as rate_date,
    upper(base)                         as currency,
    rate::numeric(18, 8)                as rate_to_zar
from {{ source('raw', 'fx_rates') }}
where upper(quote) = 'ZAR'
