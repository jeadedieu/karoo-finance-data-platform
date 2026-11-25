"""
LOAD: write extracted data into PostgreSQL's raw layer - safely.

The golden rule: running a load twice must give the same result as running it once
(IDEMPOTENCY). Different data needs a different strategy to achieve that:

  Strategy          Used for            How it works
  ----------------  ------------------  -------------------------------------------------
  UPSERT            payments, FX rates  insert new keys, update existing ones if newer
  REPLACE PARTITION order files         delete that file's rows, re-insert them
  FULL REFRESH      customers, products empty the table and reload it (small data)

Every load runs inside ONE transaction: it fully succeeds or changes nothing.
Rows are bulk-loaded with PostgreSQL's COPY, far faster than row-by-row INSERTs.
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import Connection, create_engine, text
from sqlalchemy.engine import Engine

from src import config

log = logging.getLogger("load")

SCHEMA_FILE = config.ROOT / "sql" / "001_raw_schema.sql"


# =============================================================================
# Setup
# =============================================================================
def get_engine() -> Engine:
    return create_engine(config.db_url())


def ensure_schema(engine: Engine) -> None:
    """Create the raw/meta schemas and tables if they don't exist yet (safe to repeat)."""
    with engine.begin() as conn:
        conn.exec_driver_sql(SCHEMA_FILE.read_text(encoding="utf-8"))


# =============================================================================
# Watermarks: "where did the last load stop?" - read from the data itself
# =============================================================================
def payments_watermark(engine: Engine, overlap: timedelta = timedelta(hours=1)) -> str:
    """
    Latest updated_at already loaded, minus a safety overlap.

    Why the overlap? A payment updated at 10:00:00 might only become visible in the
    API a moment later. Re-reading the last hour costs a few rows; the upsert makes
    those repeats harmless, and nothing is ever missed.
    """
    with engine.connect() as conn:
        latest = conn.execute(text("SELECT max(updated_at) FROM raw.payments")).scalar()
    if latest is None:
        return "1970-01-01T00:00:00Z"            # first run: take everything
    ts = datetime.strptime(latest, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) - overlap
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def next_order_file_date(engine: Engine, first_day: date) -> date:
    with engine.connect() as conn:
        latest = conn.execute(text("SELECT max(_file_date) FROM raw.orders")).scalar()
    return first_day if latest is None else latest + timedelta(days=1)


def fx_start_date(engine: Engine, first_day: date, lookback_days: int = 7) -> date:
    """Re-read the last week of rates each time (cheap, and catches late corrections)."""
    with engine.connect() as conn:
        latest = conn.execute(text("SELECT max(date) FROM raw.fx_rates")).scalar()
    return first_day if latest is None else date.fromisoformat(latest) - timedelta(days=lookback_days)


# =============================================================================
# Loaders - one per strategy
# =============================================================================
def upsert_payments(engine: Engine, df: pd.DataFrame) -> None:
    cols = ["payment_id", "order_id", "amount", "currency", "status", "created_at", "updated_at",
            "_source", "_extracted_at"]
    _upsert(engine, df, table="raw.payments", cols=cols, key=["payment_id"],
            newer_when="raw.payments.updated_at < EXCLUDED.updated_at", source="payments")


def upsert_fx_rates(engine: Engine, df: pd.DataFrame) -> None:
    cols = ["date", "base", "quote", "rate", "_source", "_extracted_at"]
    _upsert(engine, df, table="raw.fx_rates", cols=cols, key=["date", "base", "quote"],
            newer_when="raw.fx_rates.rate IS DISTINCT FROM EXCLUDED.rate", source="fx_rates")


def replace_order_files(engine: Engine, df: pd.DataFrame) -> None:
    """For every file in df: delete whatever that file loaded before, then insert it fresh."""
    if df.empty:
        log.info("orders: nothing to load")
        return
    df = df.copy()
    df["_file_date"] = df["_source"].str.extract(r"orders_(\d{4}-\d{2}-\d{2})")[0]
    df["_file_row"] = df.groupby("_source").cumcount() + 1
    cols = ["order_id", "order_ts", "updated_ts", "customer_id", "country", "product_id", "quantity",
            "unit_price", "currency", "total_amount", "status", "_file_date", "_file_row",
            "_source", "_extracted_at"]
    file_dates = sorted(df["_file_date"].unique())

    with engine.begin() as conn:
        deleted = conn.execute(text("DELETE FROM raw.orders WHERE _file_date = ANY(:d)"),
                               {"d": [date.fromisoformat(d) for d in file_dates]}).rowcount
        _copy(conn, df, "raw.orders", cols)
        _log(conn, "orders", "replace_partition", len(df), inserted=len(df), updated=None,
             detail=f"{len(file_dates)} file(s) {file_dates[0]}..{file_dates[-1]}, replaced {deleted} old rows")
    log.info("orders: %d rows from %d file(s) (%d old rows replaced)", len(df), len(file_dates), deleted)


def full_refresh(engine: Engine, df: pd.DataFrame, table: str) -> None:
    """TRUNCATE + reload in one transaction: readers see the old data until the new data is complete."""
    cols = [c for c in df.columns]
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE {table}"))
        _copy(conn, df, table, cols)
        _log(conn, table.split(".")[-1], "full_refresh", len(df), inserted=len(df), updated=None)
    log.info("%s: full refresh, %d rows", table, len(df))


# =============================================================================
# Building blocks
# =============================================================================
def _upsert(engine: Engine, df: pd.DataFrame, table: str, cols: list[str], key: list[str],
            newer_when: str, source: str) -> None:
    """
    1. COPY the batch into a temporary table (fast bulk load).
    2. Keep one row per key (the latest), in case the batch itself has repeats.
    3. INSERT ... ON CONFLICT: new keys are inserted; existing keys are updated
       only when the incoming row is newer/different - so re-running changes nothing.
    """
    if df.empty:
        log.info("%s: nothing to load", source)
        return
    tmp = f"tmp_{source}"
    col_list = ", ".join(cols)
    key_list = ", ".join(key)
    updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c not in key)
    order_by = "updated_at DESC" if "updated_at" in cols else "_extracted_at DESC"

    with engine.begin() as conn:
        conn.execute(text(f"CREATE TEMP TABLE {tmp} (LIKE {table} INCLUDING DEFAULTS) ON COMMIT DROP"))
        _copy(conn, df, tmp, cols)
        result = conn.execute(text(f"""
            INSERT INTO {table} ({col_list})
            SELECT DISTINCT ON ({key_list}) {col_list}
            FROM {tmp}
            ORDER BY {key_list}, {order_by}
            ON CONFLICT ({key_list}) DO UPDATE
               SET {updates}, _loaded_at = now()
             WHERE {newer_when}
            RETURNING (xmax = 0) AS inserted
        """)).scalars().all()
        inserted = sum(result)
        updated = len(result) - inserted
        _log(conn, source, "upsert", len(df), inserted=inserted, updated=updated)
    log.info("%s: %d received -> %d inserted, %d updated, %d unchanged",
             source, len(df), inserted, updated, len(df) - inserted - updated)


def _copy(conn: Connection, df: pd.DataFrame, table: str, cols: list[str]) -> None:
    """Bulk-load a DataFrame with PostgreSQL COPY, inside the caller's transaction."""
    raw_conn = conn.connection.driver_connection      # the underlying psycopg connection
    data = df[cols].astype(object).where(df[cols].notna(), None)
    with raw_conn.cursor() as cur:
        with cur.copy(f"COPY {table} ({', '.join(cols)}) FROM STDIN") as copy:
            for row in data.itertuples(index=False, name=None):
                copy.write_row(row)


def _log(conn: Connection, source: str, strategy: str, received: int,
         inserted: int | None, updated: int | None, detail: str | None = None) -> None:
    conn.execute(text("""
        INSERT INTO meta.load_log (source, strategy, rows_received, rows_inserted, rows_updated, detail)
        VALUES (:s, :st, :r, :i, :u, :d)
    """), {"s": source, "st": strategy, "r": received, "i": inserted, "u": updated, "d": detail})
