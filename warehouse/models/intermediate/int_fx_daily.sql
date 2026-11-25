-- A rate for EVERY calendar day and currency, including weekends and bank holidays,
-- when the ECB publishes nothing: those days carry forward the last published rate.
-- ZAR is added with a rate of 1 so every order can use the same join.
with calendar as (
    -- From the first published rate up to today OR the latest order, whichever is later,
    -- so an order can never fall outside the calendar (e.g. during a backfill).
    select generate_series(
        min(rate_date),
        greatest(current_date, (select max(order_date_local) from {{ ref('stg_orders') }})),
        interval '1 day'
    )::date as rate_date
    from {{ ref('stg_fx_rates') }}
),

currencies as (
    select distinct currency from {{ ref('stg_fx_rates') }}
),

spine as (
    select c.rate_date, cur.currency, r.rate_to_zar
    from calendar c
    cross join currencies cur
    left join {{ ref('stg_fx_rates') }} r
        on r.rate_date = c.rate_date and r.currency = cur.currency
),

grouped as (
    -- count() ignores nulls, so the group number only increases on days with a real rate:
    -- each gap day shares its group with the last published day before it.
    select *,
        count(rate_to_zar) over (partition by currency order by rate_date) as rate_group
    from spine
)

select
    rate_date,
    currency,
    first_value(rate_to_zar) over (partition by currency, rate_group order by rate_date) as rate_to_zar,
    rate_to_zar is null                                                                 as is_carried_forward
from grouped

union all

select rate_date, 'ZAR', 1.0, false from calendar
