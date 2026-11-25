-- Finance's daily revenue report, in rand, by country and product category.
select
    order_date_local                                            as revenue_date,
    country,
    category,
    count(*) filter (where status in ('completed', 'refunded'))  as orders,
    sum(quantity) filter (where status in ('completed', 'refunded')) as units,
    sum(gross_amount_zar) filter (where status in ('completed', 'refunded')) as gross_revenue_zar,
    sum(gross_amount_zar) filter (where status = 'refunded')     as refunds_zar,
    sum(net_amount_zar)                                          as net_revenue_zar
from {{ ref('fct_orders') }}
group by 1, 2, 3
