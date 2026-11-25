"""
Module 1 worked example: the same messy data, processed two ways, loaded into PostgreSQL.

  ETL: Extract -> Transform (in Python/pandas) -> Load clean rows into Postgres
  ELT: Extract -> Load RAW rows into Postgres -> Transform with SQL inside Postgres

Run:   python module1_etl_vs_elt.py
Needs: pip install pandas sqlalchemy "psycopg[binary]"
       A PostgreSQL database called 'karoo' (create it in pgAdmin first).
"""
import getpass
import io
import os

import pandas as pd
from sqlalchemy import create_engine, text

# --- CONNECTION SETTINGS ------------------------------------------------------
# Change these if your pgAdmin connection uses different values.
DB_USER = "postgres"
DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "karoo"

# Good habit from day one: never type passwords into code that goes on GitHub.
# The script reads the PGPASSWORD environment variable, or asks you for it.
DB_PASSWORD = os.environ.get("PGPASSWORD") or getpass.getpass(f"Password for Postgres user '{DB_USER}': ")


def get_engine():
    url = f"postgresql+psycopg://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(url)


# --- EXTRACT -----------------------------------------------------------------
# In later modules this comes from a real file/API. Here it's inline so the
# script runs anywhere. Notice the mess: duplicate order, mixed-case currency,
# amount stored as text with a thousands separator, a missing amount, and two
# different date formats.
RAW_CSV = """order_id,order_date,customer,currency,amount
1001,2026-11-01,Thabo M,ZAR,"1,250.00"
1002,2026-11-01,Anna K,eur,80.50
1002,2026-11-01,Anna K,eur,80.50
1003,02/11/2026,John D,USD,120
1004,2026-11-03,Lerato P,ZAR,
1005,2026-11-03,Mike S,usd,45.99
1006,2026-11-03,Emma W,GBP,35.00
"""

# Hard-coded FX rates for now (Module 3 replaces this with the Frankfurter API).
FX_TO_ZAR = {"ZAR": 1.0, "USD": 17.40, "EUR": 20.10}


def extract() -> pd.DataFrame:
    # dtype=str: read everything as text so pandas doesn't silently guess types.
    return pd.read_csv(io.StringIO(RAW_CSV), dtype=str)


# --- ETL: transform in Python, then load -------------------------------------
def run_etl(raw: pd.DataFrame, engine) -> None:
    df = raw.copy()
    df = df.drop_duplicates(subset="order_id")                       # dedupe
    df["currency"] = df["currency"].str.strip().str.upper()          # 'eur ' -> 'EUR'
    df["amount"] = pd.to_numeric(df["amount"].str.replace(",", ""), errors="coerce")
    df = df.dropna(subset=["amount"])                                # drop missing amounts (for now!)

    # Parse each known date format explicitly, then combine. Never let pandas "guess".
    iso = pd.to_datetime(df["order_date"], format="%Y-%m-%d", errors="coerce")
    dmy = pd.to_datetime(df["order_date"], format="%d/%m/%Y", errors="coerce")
    df["order_date"] = iso.fillna(dmy).dt.date

    df["amount_zar"] = (df["amount"] * df["currency"].map(FX_TO_ZAR)).round(2)

    df.to_sql("etl_orders", engine, if_exists="replace", index=False)


# --- ELT: load raw, then transform in SQL ------------------------------------
ELT_SQL = """
CREATE TABLE elt_orders AS
WITH deduped AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) AS rn
    FROM raw_orders
),
cleaned AS (
    SELECT
        order_id,
        -- convert both DD/MM/YYYY and ISO YYYY-MM-DD text into a real DATE
        CASE WHEN order_date LIKE '__/__/____'
             THEN TO_DATE(order_date, 'DD/MM/YYYY')
             ELSE TO_DATE(order_date, 'YYYY-MM-DD') END       AS order_date,
        customer,
        UPPER(TRIM(currency))                                  AS currency,
        CAST(REPLACE(amount, ',', '') AS NUMERIC(12, 2))       AS amount
    FROM deduped
    WHERE rn = 1
      AND amount IS NOT NULL AND TRIM(amount) <> ''
)
SELECT c.*,
       ROUND(c.amount * f.rate_to_zar, 2) AS amount_zar
FROM cleaned c
JOIN fx_rates f USING (currency)
"""


def run_elt(raw: pd.DataFrame, engine) -> None:
    raw.to_sql("raw_orders", engine, if_exists="replace", index=False)      # load AS-IS

    fx = pd.DataFrame(FX_TO_ZAR.items(), columns=["currency", "rate_to_zar"])
    fx["rate_to_zar"] = fx["rate_to_zar"].astype(str)                         # stored as text...
    fx.to_sql("fx_rates", engine, if_exists="replace", index=False)
    with engine.begin() as conn:                                              # ...so cast to NUMERIC
        conn.execute(text("ALTER TABLE fx_rates ALTER COLUMN rate_to_zar TYPE NUMERIC(12, 6) "
                          "USING rate_to_zar::NUMERIC"))

    # engine.begin() = one transaction: either every statement succeeds, or none do.
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS elt_orders"))
        conn.execute(text(ELT_SQL))


def main() -> None:
    engine = get_engine()
    raw = extract()
    print(f"Extracted {len(raw)} raw rows\n")

    run_etl(raw, engine)
    run_elt(raw, engine)

    print("ETL result (cleaned in pandas):")
    print(pd.read_sql("SELECT * FROM etl_orders ORDER BY order_id", engine), "\n")
    print("ELT result (cleaned in SQL):")
    print(pd.read_sql("SELECT * FROM elt_orders ORDER BY order_id", engine), "\n")

    raw_count = pd.read_sql("SELECT COUNT(*) AS n FROM raw_orders", engine)["n"][0]
    print("Raw table still has every original row:", raw_count)

    print("\nRevenue in ZAR by day (the question Finance actually asks):")
    print(pd.read_sql("""
        SELECT order_date, SUM(amount_zar) AS revenue_zar, COUNT(*) AS orders
        FROM elt_orders
        GROUP BY order_date
        ORDER BY order_date
    """, engine))

    print("\nDone. Open pgAdmin -> karoo -> Schemas -> public -> Tables (right-click -> Refresh).")


if __name__ == "__main__":
    main()
