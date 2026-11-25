-- Exactly one rate per currency per calendar day (no gaps were double-filled).
select rate_date, currency, count(*) as n
from {{ ref('int_fx_daily') }}
group by 1, 2
having count(*) > 1
