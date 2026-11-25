-- One row per valid order, converted to rand at the rate of the day it was placed.
select
    o.order_id,
    o.ordered_at,
    o.order_date_local,
    o.canonical_customer_id                                 as customer_id,   -- duplicate accounts merged
    o.customer_id                                           as account_id,
    o.country,
    o.product_id,
    p.product_name,
    p.category,
    o.quantity,
    o.unit_price,
    o.currency,
    o.total_amount,
    o.status,
    fx.rate_to_zar,
    fx.is_carried_forward                                   as fx_rate_carried_forward,
    round(o.total_amount * fx.rate_to_zar, 2)               as gross_amount_zar,
    case when o.status = 'completed'
         then round(o.total_amount * fx.rate_to_zar, 2) else 0 end as net_amount_zar,
    o.rows_received,
    o._source                                               as latest_source_file
from {{ ref('int_orders_validated') }} as o
join {{ ref('stg_products') }} as p on o.product_id = p.product_id
left join {{ ref('int_fx_daily') }} as fx
    on fx.rate_date = o.order_date_local and fx.currency = o.currency
where o.rejection_reason is null
