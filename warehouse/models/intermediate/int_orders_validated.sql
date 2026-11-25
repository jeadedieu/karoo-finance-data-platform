-- Every deduplicated order, with the FIRST rule it breaks (null = valid).
-- Valid orders flow to fct_orders; the rest go to quarantine with their reason.
-- Nothing is silently dropped.
select
    o.*,
    c.canonical_customer_id,
    case
        when o.ordered_at is null                                   then 'unparseable_timestamp'
        when o.customer_id is null                                  then 'missing_customer'
        when c.customer_id is null                                  then 'unknown_customer'
        when p.product_id is null                                   then 'unknown_product'
        when o.currency is null                                     then 'unknown_currency'
        when o.quantity is null or o.quantity <= 0                  then 'invalid_quantity'
        when o.unit_price is null or o.total_amount is null         then 'missing_amount'
        when abs(o.total_amount - o.quantity * o.unit_price) > 0.01 then 'total_mismatch'
    end                                                             as rejection_reason
from {{ ref('int_orders_deduped') }} as o
left join {{ ref('stg_customers') }} as c on o.customer_id = c.customer_id
left join {{ ref('stg_products') }}  as p on o.product_id  = p.product_id
