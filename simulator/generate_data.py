"""
Karoo Goods source-system simulator.

In a real company, this data would already exist in the shop's systems.
This script *creates* it, so the pipeline has something realistic to work on:

  data/products.csv                 product catalogue (clean-ish)
  data/customers.csv                customer list (some duplicates, missing countries)
  data/orders/orders_YYYY-MM-DD.*   one order export per day (messy!)
                                      - up to 2026-05-31: CSV from the old shop platform
                                      - from 2026-06-01:  JSON from the new platform
                                        (different column names + nested fields)
  data/payments_api.db              the payment provider's database, served by
                                    simulator/payments_api.py as a REST API

Everything is generated from fixed random seeds, so every run produces exactly
the same data. Running it again later with a later --end date only ADDS new days.

Run:  python simulator/generate_data.py                  (up to yesterday)
      python simulator/generate_data.py --end 2026-11-30 (up to a chosen date)
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

# --- SETTINGS -----------------------------------------------------------------
SEED = 42
START_DATE = date(2025, 11, 1)          # first day of trading data
PLATFORM_MIGRATION = date(2026, 6, 1)   # order exports switch from CSV to JSON
BASE_ORDERS_PER_DAY = 240
N_CUSTOMERS = 9000
SAST = timezone(timedelta(hours=2))     # South African Standard Time (UTC+2)

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"

# Which countries buy, how often, and what currency they pay in.
MARKETS = {  # country: (share of customers, currency, faker locales)
    "ZA": (0.55, "ZAR", ["zu_ZA", "en_GB"]),
    "GB": (0.12, "GBP", ["en_GB"]),
    "DE": (0.08, "EUR", ["de_DE"]),
    "FR": (0.05, "EUR", ["fr_FR"]),
    "NL": (0.05, "EUR", ["nl_NL"]),
    "US": (0.15, "USD", ["en_US"]),
}

# Product catalogue: (name, category, price in ZAR). Foreign prices are set
# per market (rounded to .99), like a real shop's price list.
PRODUCTS = [
    ("Rooibos Tea Loose Leaf 250g", "Tea & Coffee", 129), ("Rooibos Chai Blend 200g", "Tea & Coffee", 149),
    ("Honeybush Tea 40 Bags", "Tea & Coffee", 99), ("Single-Origin Coffee Beans 500g", "Tea & Coffee", 239),
    ("Moka Pot Gift Set", "Tea & Coffee", 549), ("Biltong Spice Kit", "Pantry", 189),
    ("Peri-Peri Sauce Trio", "Pantry", 219), ("Cape Malay Curry Spice Box", "Pantry", 279),
    ("Fynbos Honey 500g", "Pantry", 169), ("Marula Fruit Preserve", "Pantry", 119),
    ("Ostrich Leather Wallet", "Leather", 1290), ("Leather Weekender Bag", "Leather", 4890),
    ("Leather Belt Classic", "Leather", 790), ("Leather Card Holder", "Leather", 450),
    ("Leather Laptop Sleeve 14in", "Leather", 1650), ("Zulu Beaded Necklace", "Crafts", 520),
    ("Ndebele Print Cushion Cover", "Crafts", 389), ("Handwoven Ilala Palm Basket", "Crafts", 690),
    ("Soapstone Carved Bowl", "Crafts", 850), ("Wire Art Rhino Sculpture", "Crafts", 1150),
    ("Shweshwe Table Runner", "Home", 349), ("Ceramic Serving Platter", "Home", 980),
    ("Mohair Throw Blanket", "Home", 2450), ("Karoo Lamb Candle Set", "Home", 299),
    ("Baobab Wood Cutting Board", "Home", 640), ("Marula Body Butter 200ml", "Beauty", 249),
    ("Rooibos Face Serum 30ml", "Beauty", 389), ("Baobab Oil 50ml", "Beauty", 299),
    ("Kalahari Melon Lip Balm", "Beauty", 79), ("Fynbos Soap Gift Box", "Beauty", 329),
    ("Safari Linen Shirt", "Apparel", 899), ("Shweshwe Wrap Skirt", "Apparel", 750),
    ("Merino Wool Beanie", "Apparel", 349), ("Veldskoen Classic Shoes", "Apparel", 1490),
    ("Springbok Supporter Cap", "Apparel", 399), ("Cape Winelands Photo Book", "Books", 650),
    ("South African Cookbook", "Books", 420), ("Birds of Southern Africa Guide", "Books", 495),
    ("Wine Aerator & Stopper Set", "Wine Accessories", 359), ("Wine Tasting Journal", "Wine Accessories", 189),
    ("Corkscrew Waiter's Friend", "Wine Accessories", 249), ("Wine Bottle Leather Carrier", "Wine Accessories", 890),
]
FOREIGN_PRICE_FACTOR = {"GBP": 1 / 23.5, "EUR": 1 / 20.2, "USD": 1 / 18.1}  # rough list-price conversion

# How often each kind of mess appears (share of rows in a daily file).
MESS = {
    "duplicate_row": 0.015,      # the export repeated a row
    "legacy_date_format": 0.02,  # DD/MM/YYYY HH:MM instead of ISO (CSV era)
    "messy_currency": 0.03,      # ' zar', 'Usd'
    "money_as_text": 0.10,       # 'R1,250.00' for ZAR rows (CSV era)
    "missing_customer": 0.005,
    "bad_quantity": 0.002,       # 0 or negative
    "unknown_product": 0.002,
    "currency_typo": 0.002,      # 'GPB', 'EURO'
    "wrong_total": 0.003,        # total_amount != quantity * unit_price
}
PAYMENT_ISSUES = {
    "failed_first_attempt": 0.05,  # card declined, then paid
    "unpaid": 0.015,               # order completed but payment never captured
    "amount_mismatch": 0.007,      # captured amount differs from order total
    "double_charge": 0.003,        # customer charged twice
    "orphan_payment": 0.003,       # payment with no matching order
}


def rng_for(*keys: int) -> np.random.Generator:
    """A random generator that depends only on the keys, so results are repeatable."""
    return np.random.default_rng([SEED, *keys])


# --- PRODUCTS & CUSTOMERS -------------------------------------------------------
def build_products() -> pd.DataFrame:
    rows = []
    for i, (name, category, price_zar) in enumerate(PRODUCTS, start=1):
        row = {"product_id": f"P{i:03d}", "name": name, "category": category, "price_zar": float(price_zar)}
        for cur, factor in FOREIGN_PRICE_FACTOR.items():
            row[f"price_{cur.lower()}"] = max(round(price_zar * factor) - 0.01, 2.99)
        rows.append(row)
    return pd.DataFrame(rows)


def build_customers() -> pd.DataFrame:
    rng = rng_for(1)
    countries = list(MARKETS)
    shares = np.array([MARKETS[c][0] for c in countries])
    fakers = {loc: Faker(loc) for c in countries for loc in MARKETS[c][2]}
    for i, f in enumerate(fakers.values()):
        f.seed_instance(SEED + i)

    # Sign-ups spread from early 2024; growth means more recent sign-ups.
    signup_start = date(2024, 1, 1)
    span = (date(2026, 12, 31) - signup_start).days
    offsets = np.sort((rng.beta(1.6, 1.0, N_CUSTOMERS) * span).astype(int))

    rows = []
    for i in range(N_CUSTOMERS):
        country = rng.choice(countries, p=shares)
        f = fakers[rng.choice(MARKETS[country][2])]
        first, last = f.first_name(), f.last_name()
        domain = rng.choice(["gmail.com", "outlook.com", "yahoo.com", "icloud.com"])
        email = f"{first}.{last}{rng.integers(1, 999)}@{domain}"
        rows.append({
            "customer_id": f"C{i + 1:05d}",
            "first_name": first,
            "last_name": last,
            "email": email.lower().replace(" ", "").replace("'", ""),
            "country": country,
            "signup_date": (signup_start + timedelta(days=int(offsets[i]))).isoformat(),
        })
    df = pd.DataFrame(rows)

    # Mess: ~1% of customers registered twice (same email, different case, new id)
    dupes = df.sample(frac=0.01, random_state=SEED).copy()
    dupes["customer_id"] = [f"C{N_CUSTOMERS + j + 1:05d}" for j in range(len(dupes))]
    dupes["email"] = dupes["email"].str.upper()
    df = pd.concat([df, dupes], ignore_index=True)
    # Mess: ~1% missing country, a few emails with stray spaces
    df.loc[df.sample(frac=0.01, random_state=SEED + 1).index, "country"] = None
    idx = df.sample(frac=0.005, random_state=SEED + 2).index
    df.loc[idx, "email"] = " " + df.loc[idx, "email"] + " "
    return df


# --- ORDERS ---------------------------------------------------------------------------
def orders_expected(day: date) -> float:
    """How busy the shop is on a given day: growth trend + weekly + seasonal pattern."""
    t = (day - START_DATE).days / 365
    level = BASE_ORDERS_PER_DAY * (1 + 0.35 * t)
    level *= {5: 1.2, 6: 1.15}.get(day.weekday(), 1.0)          # weekends busier
    if day in (date(2025, 11, 28), date(2026, 11, 27)):         # Black Friday
        level *= 4.0
    elif day in (date(2025, 12, 1), date(2026, 11, 30)):        # Cyber Monday
        level *= 2.5
    elif day.month == 12 and day.day <= 22:                     # Christmas shopping
        level *= 1.4
    elif day.month == 12 and day.day in (25, 26):
        level *= 0.6
    elif day.month == 1:                                        # January slump
        level *= 0.8
    return level


@dataclass
class DayOrders:
    clean: pd.DataFrame      # the true orders created that day
    updates: pd.DataFrame    # status changes that will appear in LATER files


def build_day_orders(day: date, customers: pd.DataFrame, products: pd.DataFrame) -> DayOrders:
    rng = rng_for(2, day.toordinal())
    n = int(rng.poisson(orders_expected(day)))

    # Only customers who have signed up can order; some customers order far more often.
    eligible = customers[(customers["signup_date"] <= day.isoformat()) & customers["country"].notna()]
    eligible = eligible.drop_duplicates("email")   # the shop's own data uses the original account
    weights = 1 / np.arange(1, len(eligible) + 1) ** 0.35
    weights = rng.permutation(weights)
    weights /= weights.sum()
    picked = eligible.iloc[rng.choice(len(eligible), size=n, p=weights)]

    # Evening peak in order times (local SAST)
    hours = rng.choice(24, size=n, p=_hour_profile())
    seconds = rng.integers(0, 3600, size=n)
    ts = [datetime(day.year, day.month, day.day, int(h), tzinfo=SAST) + timedelta(seconds=int(s))
          for h, s in zip(hours, seconds, strict=True)]

    prod = products.iloc[rng.integers(0, len(products), size=n)].reset_index(drop=True)
    qty = rng.choice([1, 2, 3, 4, 5], size=n, p=[0.7, 0.18, 0.07, 0.03, 0.02])
    currency = picked["country"].map(lambda c: MARKETS[c][1]).to_numpy()
    unit_price = np.array([prod.loc[i, f"price_{currency[i].lower()}"] for i in range(n)])

    status = np.where(rng.random(n) < 0.02, "cancelled", "completed")
    df = pd.DataFrame({
        "order_id": [f"KG-{day:%Y%m%d}-{i + 1:05d}" for i in range(n)],
        "order_ts": ts,
        "customer_id": picked["customer_id"].to_numpy(),
        "country": picked["country"].to_numpy(),
        "product_id": prod["product_id"].to_numpy(),
        "quantity": qty,
        "unit_price": unit_price,
        "currency": currency,
        "total_amount": np.round(qty * unit_price, 2),
        "status": status,
    })
    df["updated_ts"] = df["order_ts"]
    df = df.sort_values("order_ts", ignore_index=True)

    # ~3% of completed orders get refunded 1-14 days later -> appear again in a later file
    refund = (df["status"] == "completed") & (rng.random(len(df)) < 0.03)
    upd = df[refund].copy()
    delay = rng.integers(1, 15, size=len(upd))
    upd["status"] = "refunded"
    upd["updated_ts"] = [t + timedelta(days=int(d), hours=int(rng.integers(0, 10)))
                         for t, d in zip(upd["order_ts"], delay, strict=True)]
    return DayOrders(clean=df, updates=upd)


def _hour_profile() -> np.ndarray:
    p = np.array([1, 0.5, 0.3, 0.2, 0.2, 0.4, 1, 2, 3, 3.5, 3.5, 3.5, 4, 4, 3.5, 3.5,
                  3.5, 4, 5, 6, 6.5, 6, 4, 2], dtype=float)
    return p / p.sum()


def make_messy(df: pd.DataFrame, day: date, legacy: bool) -> pd.DataFrame:
    """Inject the kinds of problems real exports have."""
    rng = rng_for(3, day.toordinal())
    df = df.copy()
    n = len(df)
    pick = lambda share: rng.random(n) < share  # noqa: E731

    df["quantity"] = df["quantity"].astype(object)
    df["unit_price"] = df["unit_price"].astype(object)
    df["total_amount"] = df["total_amount"].astype(object)

    m = pick(MESS["messy_currency"])
    df.loc[m, "currency"] = df.loc[m, "currency"].map(lambda c: rng.choice([c.lower(), f" {c}", c.title()]))
    m = pick(MESS["currency_typo"])
    df.loc[m, "currency"] = df.loc[m, "currency"].map(lambda c: {"GBP": "GPB", "EUR": "EURO"}.get(c.strip().upper(), c))
    df.loc[pick(MESS["missing_customer"]), "customer_id"] = None
    df.loc[pick(MESS["bad_quantity"]), "quantity"] = rng.choice([0, -1])
    df.loc[pick(MESS["unknown_product"]), "product_id"] = "P999"
    m = pick(MESS["wrong_total"])
    df.loc[m, "total_amount"] = (df.loc[m, "total_amount"].astype(float) * rng.uniform(0.5, 0.9)).round(2)

    if legacy:
        # Old platform: money exported as formatted text for ZAR, local time without timezone
        zar = (df["currency"] == "ZAR") & pick(MESS["money_as_text"])
        for col in ("unit_price", "total_amount"):
            df.loc[zar, col] = df.loc[zar, col].map(lambda v: f"R{float(v):,.2f}")
        fmt = np.where(pick(MESS["legacy_date_format"]), "%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S")
        df["order_ts"] = [t.strftime(f) for t, f in zip(df["order_ts"], fmt, strict=True)]
        df["updated_ts"] = [t.strftime("%Y-%m-%d %H:%M:%S") for t in df["updated_ts"]]

    dup = df[pick(MESS["duplicate_row"])]
    df = pd.concat([df, dup], ignore_index=True)
    return df.sample(frac=1, random_state=int(rng.integers(1e9)), ignore_index=True)  # exports aren't sorted


def write_order_file(df: pd.DataFrame, day: date, out_dir: Path) -> Path:
    if day < PLATFORM_MIGRATION:
        path = out_dir / f"orders_{day}.csv"
        df.rename(columns={"order_ts": "order_date", "updated_ts": "last_modified"}).to_csv(path, index=False)
        return path

    # New platform: JSON, renamed fields, nested customer object, ISO timestamps with offset
    records = []
    for r in df.to_dict("records"):
        records.append({
            "id": r["order_id"],
            "created_at": r["order_ts"].isoformat(),
            "updated_at": r["updated_ts"].isoformat(),
            "customer": {"id": _text(r["customer_id"]), "country": _text(r["country"])},
            "line": {"sku": r["product_id"], "qty": _num(r["quantity"]), "unit_price": _num(r["unit_price"])},
            "currency": r["currency"],
            "total": _num(r["total_amount"]),
            "status": r["status"],
        })
    path = out_dir / f"orders_{day}.json"
    body = json.dumps({"export_date": day.isoformat(), "orders": records}, indent=1, allow_nan=False)
    path.write_text(body, encoding="utf-8")
    return path


def _text(v):
    """Missing values must be JSON null, not NaN (NaN isn't valid JSON)."""
    return None if v is None or (isinstance(v, float) and pd.isna(v)) else v


def _num(v):
    """JSON-friendly number: whole numbers stay ints (qty 1, not 1.0); missing -> null."""
    if v is None or isinstance(v, str):
        return v
    if pd.isna(v):
        return None
    v = float(v)
    return int(v) if v.is_integer() else v


# --- PAYMENTS ---------------------------------------------------------------------------
def build_payments(orders: pd.DataFrame, refunds: pd.DataFrame, end: date) -> pd.DataFrame:
    """The payment provider's view of the world. Amounts are in MINOR units (cents), like Stripe."""
    rows = []
    refund_ts = dict(zip(refunds["order_id"], refunds["updated_ts"], strict=True))
    for day, day_orders in orders.groupby(orders["order_ts"].map(lambda t: t.date())):
        rng = rng_for(4, day.toordinal())
        for r in day_orders.itertuples():
            paid_at = r.order_ts.astimezone(timezone.utc) + timedelta(seconds=int(rng.integers(5, 180)))
            cents = int(round(r.total_amount * 100))
            cur = r.currency.lower()
            u = rng.random(5)
            if u[0] < PAYMENT_ISSUES["failed_first_attempt"] or r.status == "cancelled":
                rows.append(_pay(_payment_id(rng), r.order_id, cents, cur, "failed", paid_at - timedelta(seconds=40)))
            if r.status == "cancelled" or u[1] < PAYMENT_ISSUES["unpaid"]:
                continue
            amount = cents
            if u[2] < PAYMENT_ISSUES["amount_mismatch"]:
                amount = int(cents * rng.uniform(0.85, 0.98))
            status, updated = "succeeded", paid_at
            if r.order_id in refund_ts and refund_ts[r.order_id].date() <= end:
                status, updated = "refunded", refund_ts[r.order_id].astimezone(timezone.utc)
            rows.append(_pay(_payment_id(rng), r.order_id, amount, cur, status, paid_at, updated))
            if u[3] < PAYMENT_ISSUES["double_charge"]:
                second_at = paid_at + timedelta(minutes=2)
                rows.append(_pay(_payment_id(rng), r.order_id, amount, cur, "succeeded", second_at))
        # Orphans: payments the shop has no record of
        for _ in range(int(rng.binomial(len(day_orders), PAYMENT_ISSUES["orphan_payment"]))):
            t = datetime(day.year, day.month, day.day, int(rng.integers(6, 22)), tzinfo=timezone.utc)
            rows.append(_pay(_payment_id(rng),
                             f"KG-{day:%Y%m%d}-9{rng.integers(1000, 9999)}",
                             int(rng.integers(5000, 200000)), "zar", "succeeded", t))
    return pd.DataFrame(rows)


def _payment_id(rng: np.random.Generator) -> str:
    return "pay_" + "".join(rng.choice(list("0123456789abcdef"), 16))


def _pay(pid, order_id, amount, currency, status, created, updated=None):
    updated = updated or created
    return {"payment_id": pid, "order_id": order_id, "amount": amount, "currency": currency,
            "status": status, "created_at": created.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "updated_at": updated.strftime("%Y-%m-%dT%H:%M:%SZ")}


def write_payments_db(payments: pd.DataFrame, path: Path) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.unlink(missing_ok=True)
    # closing(...) really closes the file when done. A plain `with sqlite3.connect()`
    # only commits, and Windows refuses to rename a file that is still open.
    with closing(sqlite3.connect(tmp)) as conn:
        payments.to_sql("payments", conn, index=False)
        conn.execute("CREATE INDEX idx_updated ON payments (updated_at, payment_id)")
        conn.commit()
    tmp.replace(path)   # swap in the finished file in one go


# --- MAIN -----------------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Karoo Goods source data.")
    parser.add_argument("--end", type=date.fromisoformat, default=date.today() - timedelta(days=1),
                        help="last day of data to generate (default: yesterday)")
    args = parser.parse_args()
    end: date = args.end

    orders_dir = DATA / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)

    products = build_products()
    customers = build_customers()
    products.to_csv(DATA / "products.csv", index=False)
    customers[customers["signup_date"] <= end.isoformat()].to_csv(DATA / "customers.csv", index=False)

    # Build every day's true orders, then decide which file each row lands in.
    days = [START_DATE + timedelta(days=i) for i in range((end - START_DATE).days + 1)]
    all_clean, all_updates = [], []
    for d in days:
        result = build_day_orders(d, customers, products)
        all_clean.append(result.clean)
        all_updates.append(result.updates)
    clean = pd.concat(all_clean, ignore_index=True)
    updates = pd.concat(all_updates, ignore_index=True)
    updates["file_day"] = updates["updated_ts"].map(lambda t: t.date())
    clean_by_day = dict(tuple(clean.groupby(clean["order_ts"].map(lambda t: t.date()))))
    updates_by_day = dict(tuple(updates.groupby("file_day")))

    for d in days:
        today = clean_by_day.get(d, clean.iloc[0:0])
        late = updates_by_day.get(d, updates.iloc[0:0]).drop(columns="file_day")
        daily = pd.concat([today, late], ignore_index=True)
        write_order_file(make_messy(daily, d, legacy=d < PLATFORM_MIGRATION), d, orders_dir)

    payments = build_payments(clean, updates, end)
    payments = payments[payments["updated_at"] <= f"{end}T23:59:59Z"]
    write_payments_db(payments, DATA / "payments_api.db")

    print(f"Generated {START_DATE} -> {end} ({len(days)} days)")
    print(f"  products : {len(products):>8,}")
    print(f"  customers: {len(customers[customers['signup_date'] <= end.isoformat()]):>8,}")
    print(f"  orders   : {len(clean):>8,}  (+ {int((updates['file_day'] <= end).sum()):,} late status updates)")
    print(f"  payments : {len(payments):>8,}")
    print(f"  files in : {orders_dir}")


if __name__ == "__main__":
    main()
