{{ config(severity='warn') }}
-- WARN (not fail) if more than 2% of orders are quarantined: that usually means a
-- source changed, not that a few rows are bad.
select count(*) filter (where rejection_reason is not null) * 1.0 / count(*) as quarantine_rate
from {{ ref('int_orders_validated') }}
having count(*) filter (where rejection_reason is not null) * 1.0 / count(*) > 0.02
