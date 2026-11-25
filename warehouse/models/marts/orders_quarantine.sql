{{ config(schema='quarantine') }}
-- Orders that failed validation, with the reason and the file they came from,
-- so someone can investigate and fix them at the source.
select
    rejection_reason,
    order_id,
    ordered_at,
    customer_id,
    product_id,
    currency_raw,
    quantity,
    unit_price,
    total_amount,
    status,
    _source as source_file,
    _file_row as source_row
from {{ ref('int_orders_validated') }}
where rejection_reason is not null
