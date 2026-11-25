"""
Integration tests for the load layer against a REAL PostgreSQL database.

They prove the most important promise of the pipeline: loading the same data twice
changes nothing (idempotency), while genuinely newer data is applied.
SAFETY: these tests EMPTY the raw tables, so they only run against a database whose
name ends in "_test" (e.g. DB_NAME=karoo_test). Against anything else they are skipped,
so they can never wipe the real warehouse by accident.
"""
import os

import pandas as pd
import pytest
from sqlalchemy import text

from src import load

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def engine():
    if not os.environ.get("DB_NAME", "").endswith("_test"):
        pytest.skip("integration tests only run against a *_test database (set DB_NAME=karoo_test)")
    try:
        eng = load.get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"no warehouse database reachable: {exc}")
    load.ensure_schema(eng)
    return eng


@pytest.fixture(autouse=True)
def clean_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE raw.payments, raw.orders, raw.fx_rates, meta.load_log"))
    yield
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE raw.payments, raw.orders, raw.fx_rates, meta.load_log"))


def count(engine, table):
    with engine.connect() as conn:
        return conn.execute(text(f"SELECT count(*) FROM {table}")).scalar()


def payments(status="succeeded", updated="2026-11-01T10:00:00Z"):
    return pd.DataFrame([{
        "payment_id": "pay_1", "order_id": "KG-1", "amount": "12500", "currency": "zar", "status": status,
        "created_at": "2026-11-01T10:00:00Z", "updated_at": updated,
        "_source": "test", "_extracted_at": "2026-11-01T11:00:00Z",
    }])


def test_upserting_the_same_payment_twice_keeps_one_row(engine):
    load.upsert_payments(engine, payments())
    load.upsert_payments(engine, payments())
    assert count(engine, "raw.payments") == 1


def test_newer_payment_version_replaces_the_old_one(engine):
    load.upsert_payments(engine, payments())
    load.upsert_payments(engine, payments(status="refunded", updated="2026-11-05T09:00:00Z"))
    with engine.connect() as conn:
        assert conn.execute(text("SELECT status FROM raw.payments")).scalar() == "refunded"


def test_older_payment_version_is_ignored(engine):
    load.upsert_payments(engine, payments(status="refunded", updated="2026-11-05T09:00:00Z"))
    load.upsert_payments(engine, payments(status="succeeded", updated="2026-11-01T10:00:00Z"))
    with engine.connect() as conn:
        assert conn.execute(text("SELECT status FROM raw.payments")).scalar() == "refunded"


def test_reloading_an_order_file_replaces_instead_of_duplicating(engine):
    rows = pd.DataFrame([{
        "order_id": f"KG-{i}", "order_ts": "2026-11-01 10:00:00", "updated_ts": "2026-11-01 10:00:00",
        "customer_id": "C1", "country": "ZA", "product_id": "P001", "quantity": "1", "unit_price": "10",
        "currency": "ZAR", "total_amount": "10", "status": "completed",
        "_source": "orders_2026-11-01.csv (csv_v1)", "_extracted_at": "x",
    } for i in range(5)])
    load.replace_order_files(engine, rows)
    load.replace_order_files(engine, rows)
    assert count(engine, "raw.orders") == 5


def test_watermark_has_a_safety_overlap(engine):
    load.upsert_payments(engine, payments(updated="2026-11-01T10:00:00Z"))
    assert load.payments_watermark(engine) == "2026-11-01T09:00:00Z"
