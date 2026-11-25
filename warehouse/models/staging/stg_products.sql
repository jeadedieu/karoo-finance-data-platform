select
    product_id,
    name                        as product_name,
    category,
    price_zar::numeric(12, 2)   as price_zar,
    price_gbp::numeric(12, 2)   as price_gbp,
    price_eur::numeric(12, 2)   as price_eur,
    price_usd::numeric(12, 2)   as price_usd
from {{ source('raw', 'products') }}
