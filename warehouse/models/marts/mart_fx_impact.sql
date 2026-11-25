-- How much did exchange-rate movements add to (or take from) foreign revenue?
-- "Constant currency" = what the same sales would have been worth at the baseline
-- month's average rate. FX impact = actual rand revenue - constant-currency revenue.
with baseline as (
    select currency, avg(rate_to_zar) as baseline_rate
    from {{ ref('int_fx_daily') }}
    where date_trunc('month', rate_date) = '{{ var("fx_baseline_month") }}'::date
    group by 1
),

monthly as (
    select
        date_trunc('month', order_date_local)::date     as revenue_month,
        currency,
        sum(total_amount) filter (where status = 'completed') as net_revenue_local,
        sum(net_amount_zar)                              as net_revenue_zar_actual
    from {{ ref('fct_orders') }}
    where currency <> 'ZAR'
    group by 1, 2
)

select
    m.revenue_month,
    m.currency,
    m.net_revenue_local,
    round(m.net_revenue_zar_actual / nullif(m.net_revenue_local, 0), 4)   as avg_rate_actual,
    round(b.baseline_rate, 4)                                             as baseline_rate,
    m.net_revenue_zar_actual,
    round(m.net_revenue_local * b.baseline_rate, 2)                       as net_revenue_zar_constant,
    round(m.net_revenue_zar_actual - m.net_revenue_local * b.baseline_rate, 2) as fx_impact_zar,
    round(100 * (m.net_revenue_zar_actual / nullif(m.net_revenue_local * b.baseline_rate, 0) - 1), 2) as fx_impact_pct
from monthly m
join baseline b using (currency)
