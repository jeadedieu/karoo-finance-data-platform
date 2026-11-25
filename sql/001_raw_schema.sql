-- =============================================================================
-- RAW layer: data exactly as the sources delivered it (all values as TEXT),
-- plus lineage columns (_source, _extracted_at, _loaded_at).
-- Safe to run any number of times: everything is "IF NOT EXISTS".
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS raw;    -- landed source data
CREATE SCHEMA IF NOT EXISTS meta;   -- the pipeline's own bookkeeping

-- Exchange rates: one row per (day, currency pair). Re-loading a day UPDATES it.
CREATE TABLE IF NOT EXISTS raw.fx_rates (
    date            text NOT NULL,
    base            text NOT NULL,
    quote           text NOT NULL,
    rate            text NOT NULL,
    _source         text,
    _extracted_at   text,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (date, base, quote)
);

-- Payments: one row per payment, always holding its LATEST state
-- (a payment can change from 'succeeded' to 'refunded' later).
CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id      text PRIMARY KEY,
    order_id        text,
    amount          text,
    currency        text,
    status          text,
    created_at      text,
    updated_at      text,
    _source         text,
    _extracted_at   text,
    _loaded_at      timestamptz NOT NULL DEFAULT now()
);

-- Orders: every row of every daily file, duplicates and all.
-- Identified by (file date, row number in file): re-loading a file REPLACES its rows.
CREATE TABLE IF NOT EXISTS raw.orders (
    order_id        text,
    order_ts        text,
    updated_ts      text,
    customer_id     text,
    country         text,
    product_id      text,
    quantity        text,
    unit_price      text,
    currency        text,
    total_amount    text,
    status          text,
    _file_date      date NOT NULL,
    _file_row       integer NOT NULL,
    _source         text,
    _extracted_at   text,
    _loaded_at      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (_file_date, _file_row)
);
CREATE INDEX IF NOT EXISTS ix_raw_orders_order_id ON raw.orders (order_id);

-- Reference data: small tables, fully replaced on every load.
CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id     text,
    first_name      text,
    last_name       text,
    email           text,
    country         text,
    signup_date     text,
    _source         text,
    _extracted_at   text,
    _loaded_at      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id      text,
    name            text,
    category        text,
    price_zar       text,
    price_gbp       text,
    price_eur       text,
    price_usd       text,
    _source         text,
    _extracted_at   text,
    _loaded_at      timestamptz NOT NULL DEFAULT now()
);

-- A diary of every load: what was loaded, how, and how many rows.
CREATE TABLE IF NOT EXISTS meta.load_log (
    id              bigserial PRIMARY KEY,
    source          text NOT NULL,
    strategy        text NOT NULL,
    rows_received   integer NOT NULL,
    rows_inserted   integer,
    rows_updated    integer,
    detail          text,
    loaded_at       timestamptz NOT NULL DEFAULT now()
);
