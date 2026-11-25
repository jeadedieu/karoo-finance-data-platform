"""
EXTRACT: pull data out of every source system, changing it as little as possible.

Sources:
  1. Exchange rates  - Frankfurter API (real, public)
  2. Payments        - the payment provider's API (paginated, rate-limited, unreliable)
  3. Orders          - daily export files (CSV before the 2026-06-01 migration, JSON after)
  4. Customers and products - CSV reference files

Rules every extractor follows:
  - Keep values as TEXT. Interpreting types is the Transform step's job.
  - Add lineage columns (_source, _extracted_at) so every row can be traced back.
  - Fail LOUDLY with a clear message when a source is missing or changes shape.
    A crash you notice beats wrong numbers you don't.

Try it:  python -m src.extract
"""
from __future__ import annotations

import json
import logging
import random
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from src import config

log = logging.getLogger("extract")


class ExtractError(Exception):
    """A source could not be read. The message says which source and why."""


# =============================================================================
# Shared helper: an HTTP GET that survives unreliable APIs
# =============================================================================
def get_with_retries(session: requests.Session, url: str, params: dict | None = None,
                     max_attempts: int = 6, timeout: tuple[float, float] = (5, 2.5)) -> object:
    """
    GET a URL and return the parsed JSON, retrying the failures that are worth retrying.

    Retry (temporary problems - waiting may fix them):
      - 429 Too Many Requests -> wait as long as the API's Retry-After header says
      - 5xx server errors, timeouts, dropped connections -> exponential backoff: 1s, 2s, 4s, 8s...
    Don't retry (our fault - retrying would just fail again):
      - other 4xx errors such as 401 (bad API key) or 400 (bad request)
    """
    for attempt in range(1, max_attempts + 1):
        try:
            # timeout=(connect, read): never let one slow response hang the whole pipeline
            response = session.get(url, params=params, timeout=timeout)
        except (requests.Timeout, requests.ConnectionError) as exc:
            reason, wait = type(exc).__name__, _backoff(attempt)
        else:
            if response.status_code == 200:
                return response.json()
            if response.status_code == 429:
                reason = "429 rate limited"
                wait = float(response.headers.get("Retry-After", _backoff(attempt)))
            elif response.status_code >= 500:
                reason, wait = f"{response.status_code} server error", _backoff(attempt)
            else:
                raise ExtractError(f"{response.status_code} from {url}: {response.text[:200]}")

        if attempt == max_attempts:
            raise ExtractError(f"Gave up on {url} after {max_attempts} attempts (last problem: {reason})")
        log.warning("%s - retry %d/%d in %.1fs", reason, attempt, max_attempts - 1, wait)
        time.sleep(wait)
    raise AssertionError("unreachable")


def _backoff(attempt: int) -> float:
    """Exponential backoff with jitter: ~1s, 2s, 4s, 8s... capped at 30s.
    The random 'jitter' stops many clients retrying at the exact same moment."""
    return min(2 ** (attempt - 1), 30) + random.uniform(0, 0.5)


def _now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# =============================================================================
# 1. Exchange rates (Frankfurter API)
# =============================================================================
def extract_fx_rates(start: date, end: date, currencies: tuple[str, ...] = ("USD", "EUR", "GBP")) -> pd.DataFrame:
    """
    Daily rates for 1 unit of each currency in rand, e.g. 1 USD = 17.54 ZAR.

    Uses the European Central Bank's rates, published on working days only:
    weekends and holidays are simply ABSENT. That's normal, not an error -
    the Transform step decides which rate applies on those days.
    """
    records = []
    with requests.Session() as session:
        for cur in currencies:
            params = {"from": start.isoformat(), "to": end.isoformat(),
                      "base": cur.lower(), "quotes": "zar", "providers": "ecb"}
            data = get_with_retries(session, config.FX_API_URL, params)
            if not isinstance(data, list):
                raise ExtractError(f"FX API changed shape: expected a list, got {type(data).__name__}")
            records.extend(data)
            log.info("FX %s->ZAR: %d daily rates (%s to %s)", cur, len(data), start, end)

    df = pd.DataFrame(records)
    _require_columns(df, {"date", "base", "quote", "rate"}, source="Frankfurter FX API")
    df["rate"] = df["rate"].astype(str)
    df["_source"] = "frankfurter_v2_ecb"
    df["_extracted_at"] = _now_utc()
    return df[["date", "base", "quote", "rate", "_source", "_extracted_at"]]


# =============================================================================
# 2. Payments (payment provider API)
# =============================================================================
PAYMENT_FIELDS = {"payment_id", "order_id", "amount", "currency", "status", "created_at", "updated_at"}


def extract_payments(updated_since: str, updated_before: str | None = None, page_size: int = 1000) -> pd.DataFrame:
    """
    All payments created or changed in a time window, page by page.

    updated_since is the WATERMARK: "give me only what changed since my last run".
    That makes daily runs small (a few hundred rows) instead of re-reading everything.
    """
    url = f"{config.PAYMENTS_API_URL}/v1/payments"
    params = {"updated_since": updated_since, "page_size": page_size}
    if updated_before:
        params["updated_before"] = updated_before

    rows: list[dict] = []
    with requests.Session() as session:
        session.headers["Authorization"] = f"Bearer {config.PAYMENTS_API_KEY}"
        page = 1
        while True:
            body = get_with_retries(session, url, {**params, "page": page})
            batch = body["data"]
            if batch and not PAYMENT_FIELDS <= batch[0].keys():
                missing = PAYMENT_FIELDS - batch[0].keys()
                raise ExtractError(f"Payments API changed shape: missing fields {sorted(missing)}")
            rows.extend(batch)
            if page % 20 == 0:
                log.info("Payments: %d pages, %d rows so far", page, len(rows))
            if not body["has_more"]:
                break
            page += 1

    log.info("Payments: %d rows in %d page(s) updated since %s", len(rows), page, updated_since)
    df = pd.DataFrame(rows, columns=sorted(PAYMENT_FIELDS)).astype("string")
    df["_source"] = "payments_api_v1"
    df["_extracted_at"] = _now_utc()
    return df


# =============================================================================
# 3. Orders (daily export files, two formats)
# =============================================================================
# One common shape for orders, whichever platform exported them.
ORDER_COLUMNS = ["order_id", "order_ts", "updated_ts", "customer_id", "country", "product_id",
                 "quantity", "unit_price", "currency", "total_amount", "status"]

# Old platform (CSV) column names -> common names
CSV_RENAMES = {"order_date": "order_ts", "last_modified": "updated_ts"}
EXPECTED_CSV_COLUMNS = {"order_id", "order_date", "last_modified", "customer_id", "country", "product_id",
                        "quantity", "unit_price", "currency", "total_amount", "status"}

# New platform (JSON) field paths -> common names
JSON_FIELDS = {
    "order_id": ("id",), "order_ts": ("created_at",), "updated_ts": ("updated_at",),
    "customer_id": ("customer", "id"), "country": ("customer", "country"),
    "product_id": ("line", "sku"), "quantity": ("line", "qty"), "unit_price": ("line", "unit_price"),
    "currency": ("currency",), "total_amount": ("total",), "status": ("status",),
}


def extract_orders(start: date, end: date) -> pd.DataFrame:
    """Read every daily order file from start to end (inclusive) into one table."""
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    files, missing = [], []
    for d in days:
        found = sorted(config.ORDERS_DIR.glob(f"orders_{d}.*"))
        (files.append(found[0]) if found else missing.append(d.isoformat()))
    if missing:
        # A missing daily file means we'd silently under-report revenue. Stop instead.
        raise ExtractError(f"Missing order file(s) for {len(missing)} day(s): {', '.join(missing[:10])}")

    frames = [read_order_file(f) for f in files]
    df = pd.concat(frames, ignore_index=True)
    log.info("Orders: %d rows from %d file(s), %s to %s", len(df), len(files), start, end)
    return df


def read_order_file(path: Path) -> pd.DataFrame:
    if path.suffix == ".csv":
        df = _read_orders_csv(path)
        fmt = "csv_v1"
    elif path.suffix == ".json":
        df = _read_orders_json(path)
        fmt = "json_v2"
    else:
        raise ExtractError(f"Unknown order file type: {path.name}")
    df["_source"] = f"{path.name} ({fmt})"
    df["_extracted_at"] = _now_utc()
    return df


def _read_orders_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)               # text only: no guessing
    _require_columns(df, EXPECTED_CSV_COLUMNS, source=path.name)
    return df.rename(columns=CSV_RENAMES)[ORDER_COLUMNS]


def _read_orders_json(path: Path) -> pd.DataFrame:
    body = json.loads(path.read_text(encoding="utf-8"))
    if "orders" not in body:
        raise ExtractError(f"{path.name}: expected a top-level 'orders' list")
    rows = []
    for i, rec in enumerate(body["orders"]):
        row = {}
        for col, keys in JSON_FIELDS.items():
            value = rec
            for k in keys:                          # walk nested keys: rec["customer"]["id"]
                if not isinstance(value, dict) or k not in value:
                    raise ExtractError(f"{path.name}: record {i} has no field {'.'.join(keys)}")
                value = value[k]
            row[col] = None if value is None else str(value)
        rows.append(row)
    return pd.DataFrame(rows, columns=ORDER_COLUMNS)


# =============================================================================
# 4. Reference files: customers and products
# =============================================================================
def extract_customers() -> pd.DataFrame:
    return _read_reference_csv("customers.csv",
                               {"customer_id", "first_name", "last_name", "email", "country", "signup_date"})


def extract_products() -> pd.DataFrame:
    return _read_reference_csv("products.csv",
                               {"product_id", "name", "category", "price_zar", "price_gbp", "price_eur", "price_usd"})


def _read_reference_csv(name: str, expected: set[str]) -> pd.DataFrame:
    path = config.DATA_DIR / name
    if not path.exists():
        raise ExtractError(f"{path} not found - run: python simulator/generate_data.py")
    df = pd.read_csv(path, dtype=str)
    _require_columns(df, expected, source=name)
    df["_source"] = name
    df["_extracted_at"] = _now_utc()
    log.info("%s: %d rows", name, len(df))
    return df


# =============================================================================
# Shared check: did the source change shape?
# =============================================================================
def _require_columns(df: pd.DataFrame, expected: set[str], source: str) -> None:
    missing = expected - set(df.columns)
    if missing:
        raise ExtractError(f"Schema change in {source}: missing column(s) {sorted(missing)}; "
                           f"found {sorted(df.columns)}")
    extra = set(df.columns) - expected
    if extra:
        log.warning("%s has new column(s) %s - ignoring them for now", source, sorted(extra))


# =============================================================================
# Demo: python -m src.extract
# =============================================================================
if __name__ == "__main__":
    config.setup_logging()
    pd.set_option("display.width", 160)
    pd.set_option("display.max_columns", 12)

    print("\n=== 1. FX rates: last 14 days (note the missing weekend dates) ===")
    fx = extract_fx_rates(date(2026, 11, 11), date(2026, 11, 24))
    print(fx[fx["base"] == "USD"][["date", "base", "quote", "rate"]].to_string(index=False))

    print("\n=== 2. Payments updated since 20 Nov (watch for retries) ===")
    payments = extract_payments(updated_since="2026-11-20T00:00:00Z")
    print(payments.head(3).to_string(index=False))

    print("\n=== 3. Orders across the platform migration (CSV on 31 May, JSON on 1 June) ===")
    orders = extract_orders(date(2026, 5, 31), date(2026, 6, 1))
    print(orders.groupby("_source").size().to_string())
    print(orders.sample(4, random_state=1)[ORDER_COLUMNS[:8]].to_string(index=False))

    print("\n=== 4. Reference files ===")
    customers, products = extract_customers(), extract_products()
    print(f"customers: {len(customers):,}   products: {len(products):,}")
