"""
Karoo Goods daily pipeline - Airflow DAG.

Every morning at 06:00 South African time:

  simulate_shop_export          (stands in for the web shop dropping yesterday's file)
          |
          +--> load_reference ---+
          +--> load_fx_rates ----+
          +--> load_payments ----+--> check_source_freshness --> dbt_build
          +--> load_orders ------+

Each load is its own task: if the payments API is down, only load_payments fails
and retries, the other loads still succeed, and dbt waits until everything is in.

Tasks run our existing CLI commands in the project's own Python environment
(KAROO_PYTHON), so the pipeline's packages (pandas, dbt...) never clash with Airflow's.
"""
from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

KAROO_HOME = os.environ.get("KAROO_HOME", "/opt/karoo")
KAROO_PYTHON = os.environ.get("KAROO_PYTHON", "/opt/karoo-venv/bin/python")


def karoo(task_id: str, args: str, **kwargs) -> BashOperator:
    """A task that runs `python <args>` inside the project folder."""
    return BashOperator(task_id=task_id, bash_command=f'cd "{KAROO_HOME}" && "{KAROO_PYTHON}" {args}', **kwargs)


@dag(
    dag_id="karoo_daily_pipeline",
    description="Extract orders, payments and FX rates, load to the warehouse, build and test with dbt",
    schedule="0 6 * * *",                                         # every day at 06:00 ...
    start_date=pendulum.datetime(2026, 11, 1, tz="Africa/Johannesburg"),  # ... SA time
    catchup=False,                                                # don't replay every missed day on first start
    max_active_runs=1,                                            # never two runs writing at the same time
    default_args={
        "owner": "data-engineering",
        "retries": 2,                                             # Airflow-level retries, on top of the
        "retry_delay": timedelta(minutes=2),                      # HTTP retries inside the extract code
        "execution_timeout": timedelta(minutes=30),               # a stuck task fails instead of hanging
    },
    tags=["karoo", "finance", "elt"],
)
def karoo_daily_pipeline():
    # In real life the shop drops yesterday's export by itself. Here we simulate it.
    # {{ macros.ds_add(ds, -1) }} = the day before this run's date, so backfills generate the right day.
    simulate = karoo("simulate_shop_export", "simulator/generate_data.py --end {{ macros.ds_add(ds, -1) }}")

    loads = [
        karoo("load_reference", "-m src.pipeline --step reference"),
        karoo("load_fx_rates", "-m src.pipeline --step fx"),
        karoo("load_payments", "-m src.pipeline --step payments"),
        karoo("load_orders", "-m src.pipeline --step orders"),
    ]

    # Stop early if any source has gone stale (e.g. no new orders for 36+ hours)
    freshness = karoo("check_source_freshness", "-m src.transform source freshness")

    # Build every model and run every test; failing tests fail the run
    dbt_build = karoo("dbt_build", "-m src.transform build", retries=0)

    simulate >> loads >> freshness >> dbt_build


karoo_daily_pipeline()
