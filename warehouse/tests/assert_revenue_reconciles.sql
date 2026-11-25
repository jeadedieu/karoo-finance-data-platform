-- The daily revenue report must add up to exactly the same total as the order table.
with totals as (
    select
        (select coalesce(sum(net_revenue_zar), 0) from {{ ref('mart_daily_revenue') }}) as report_total,
        (select coalesce(sum(net_amount_zar), 0)  from {{ ref('fct_orders') }})        as orders_total
)
select * from totals where abs(report_total - orders_total) > 0.01
