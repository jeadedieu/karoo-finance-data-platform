# Source-system simulator

Karoo Goods is a fictional company, so its source systems are simulated here.
All data is **synthetic** (no real people or transactions) and generated from fixed
random seeds, so every run produces identical data.

| Source | What it simulates | Format |
|---|---|---|
| `data/orders/orders_YYYY-MM-DD.*` | Daily order export from the web shop | CSV until 2026-05-31, JSON from 2026-06-01 (platform migration) |
| `data/customers.csv` | Customer list from the CRM | CSV |
| `data/products.csv` | Product catalogue with per-market prices | CSV |
| Payments API (`payments_api.py`) | Third-party payment provider | REST/JSON, paginated, rate-limited |
| Frankfurter API (real, external) | Daily exchange rates | REST/JSON |

## Usage

```bash
python simulator/generate_data.py                  # data up to yesterday
python simulator/generate_data.py --end 2026-11-30 # data up to a chosen date
python simulator/payments_api.py                   # API on http://127.0.0.1:8000 (docs at /docs)
```

## Data issues built in on purpose

These are the problems the pipeline has to handle, each modelled on something
that happens in real companies.

**Order files**
- Duplicate rows in the same export (~1.5%)
- Late-arriving updates: refunded orders re-appear in a later day's file with a newer `last_modified` / `updated_at`
- Schema change on 2026-06-01: CSV → JSON, renamed fields (`order_id` → `id`, `order_date` → `created_at`), nested `customer` and `line` objects
- Mixed date formats in the CSV era (`2025-11-01 14:03:22` and `01/11/2025 14:03`); local time with no timezone in CSV, ISO 8601 with offset in JSON
- Money as formatted text for some ZAR rows (`"R1,250.00"`)
- Inconsistent currency codes (`zar`, ` EUR`, `Usd`) and typos (`GPB`, `EURO`)
- Missing `customer_id`, zero/negative quantities, unknown product (`P999`), totals that don't equal quantity × price

**Customers**
- Duplicate accounts (same email, different case), missing country, emails with stray spaces

**Payments API**
- Amounts in minor units (cents), lowercase currency codes, timestamps in UTC (orders are in SAST, UTC+2)
- Failed attempts before a successful payment, unpaid orders, amount mismatches, double charges, orphan payments with no matching order
- HTTP 429 rate limiting, random HTTP 503 errors and slow responses
