-- Does every order have exactly the right payment, and every payment an order?
-- Matched on order_id (not on date: payments are UTC, orders are SA time).
with captured as (
    -- 'refunded' payments were captured first, so they count as paid
    select
        order_id,
        sum(amount) filter (where status in ('succeeded', 'refunded'))   as amount_captured,
        count(*)    filter (where status in ('succeeded', 'refunded'))   as successful_payments,
        count(*)    filter (where status = 'failed')                     as failed_attempts,
        min(currency)                                                     as payment_currency,
        min(created_at)                                                   as first_payment_at
    from {{ ref('stg_payments') }}
    group by 1
),

orders as (
    select order_id, order_date_local, status, currency, total_amount, rejection_reason
    from {{ ref('int_orders_validated') }}
),

joined as (
    select
        coalesce(o.order_id, p.order_id)                    as order_id,
        o.order_date_local,
        o.status                                            as order_status,
        o.currency                                          as order_currency,
        o.total_amount                                      as order_total,
        p.payment_currency,
        coalesce(p.amount_captured, 0)                      as amount_captured,
        coalesce(p.successful_payments, 0)                  as successful_payments,
        coalesce(p.failed_attempts, 0)                      as failed_attempts,
        o.order_id is not null                              as order_exists,
        o.rejection_reason                                  as order_rejection_reason,
        coalesce(o.order_date_local, (p.first_payment_at at time zone '{{ var("local_timezone") }}')::date) as recon_date
    from orders o
    full outer join captured p on o.order_id = p.order_id
)

select
    j.*,
    case
        when not order_exists                                       then 'orphan_payment'
        when order_rejection_reason is not null                     then 'order_in_quarantine'  -- fix the order data first
        when order_status = 'cancelled' and amount_captured = 0     then 'ok_cancelled'
        when order_status = 'cancelled'                             then 'paid_but_cancelled'
        when amount_captured = 0                                    then 'unpaid'
        when successful_payments > 1                                then 'double_charge'
        when payment_currency <> order_currency                     then 'currency_mismatch'
        when abs(amount_captured - order_total) > 0.01              then 'amount_mismatch'
        else 'matched'
    end                                                             as recon_status,
    -- Money gap: positive = captured too much, negative = captured too little.
    -- Zero for fine and cancelled orders, null when the order total itself is untrusted.
    case when order_rejection_reason is not null then null
         when order_status = 'cancelled' and amount_captured = 0 then 0
         else round(amount_captured - coalesce(order_total, 0), 2) end as difference,
    case when order_rejection_reason is not null then null
         when order_status = 'cancelled' and amount_captured = 0 then 0
         else round((amount_captured - coalesce(order_total, 0)) * fx.rate_to_zar, 2) end as difference_zar
from joined j
left join {{ ref('int_fx_daily') }} fx
    on fx.rate_date = j.recon_date
   and fx.currency = coalesce(j.order_currency, j.payment_currency)
