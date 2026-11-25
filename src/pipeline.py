"""
The pipeline: Extract -> Load for every source, incremental by default.

  python -m src.pipeline
      Normal daily run: load only what's new since the last run.

  python -m src.pipeline --orders-from 2026-03-01 --orders-to 2026-03-31
      Backfill / reprocess: (re)load order files for a date range.
      Safe to repeat - the files' old rows are replaced, never duplicated.

Each run_* function is one independent step. In Step 6, Airflow calls them as tasks.
"""
from __future__ import annotations

import argparse
import logging
from datetime import date, timedelta

from src import config, extract, load

log = logging.getLogger("pipeline")

FIRST_ORDER_DAY = date(2025, 11, 1)
# 1 Nov 2025 was a Saturday, and weekend orders use Friday's rate -> start FX a week earlier.
FIRST_FX_DAY = FIRST_ORDER_DAY - timedelta(days=7)


def run_reference(engine) -> None:
    load.full_refresh(engine, extract.extract_customers(), "raw.customers")
    load.full_refresh(engine, extract.extract_products(), "raw.products")


def run_fx(engine, end: date | None = None) -> None:
    start = load.fx_start_date(engine, FIRST_FX_DAY)
    end = end or date.today()
    load.upsert_fx_rates(engine, extract.extract_fx_rates(start, end))


def run_payments(engine) -> None:
    since = load.payments_watermark(engine)
    log.info("payments: watermark -> fetching updates since %s", since)
    load.upsert_payments(engine, extract.extract_payments(updated_since=since))


def run_orders(engine, start: date | None = None, end: date | None = None) -> None:
    start = start or load.next_order_file_date(engine, FIRST_ORDER_DAY)
    end = end or latest_order_file_date()
    if start > end:
        log.info("orders: up to date (last file %s)", end)
        return
    load.replace_order_files(engine, extract.extract_orders(start, end))


def latest_order_file_date() -> date:
    names = sorted(p.stem for p in config.ORDERS_DIR.glob("orders_*.*"))
    if not names:
        raise extract.ExtractError(f"No order files in {config.ORDERS_DIR}")
    return date.fromisoformat(names[-1].removeprefix("orders_"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the Karoo Goods pipeline (extract + load).")
    parser.add_argument("--orders-from", type=date.fromisoformat, help="reprocess order files from this date")
    parser.add_argument("--orders-to", type=date.fromisoformat, help="...up to this date (inclusive)")
    args = parser.parse_args()

    config.setup_logging()
    engine = load.get_engine()
    load.ensure_schema(engine)

    run_reference(engine)
    run_fx(engine)
    run_payments(engine)
    run_orders(engine, args.orders_from, args.orders_to)
    log.info("pipeline finished")


if __name__ == "__main__":
    main()
