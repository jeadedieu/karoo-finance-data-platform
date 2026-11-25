-- Every deduplicated order must end up in EXACTLY one place: fct_orders or quarantine.
-- Returns a row (= test failure) if the counts don't add up.
with counts as (
    select
        (select count(*) from {{ ref('int_orders_deduped') }})  as deduped,
        (select count(*) from {{ ref('fct_orders') }})          as valid,
        (select count(*) from {{ ref('orders_quarantine') }})   as quarantined
)
select * from counts where deduped <> valid + quarantined
