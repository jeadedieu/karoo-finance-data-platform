"""
TRANSFORM: run dbt (the warehouse/ project) with settings from .env.

dbt reads the database login from environment variables. Importing src.config
loads .env into the environment, so the password never appears in any dbt file.

  python -m src.transform build          # run all models + all tests, in dependency order
  python -m src.transform run            # models only
  python -m src.transform test           # tests only
  python -m src.transform source freshness
  python -m src.transform docs generate  # then: python -m src.transform docs serve
"""
from __future__ import annotations

import os
import sys

from dbt.cli.main import dbtRunner

from src import config

DBT_DIR = config.ROOT / "warehouse"


def run_dbt(args: list[str]) -> bool:
    """Run a dbt command; returns True if it succeeded."""
    os.environ.setdefault("DBT_PROFILES_DIR", str(DBT_DIR))
    full_args = [*args, "--project-dir", str(DBT_DIR)]
    if args and args[0] in {"build", "run", "test", "seed"}:
        full_args += ["--profiles-dir", str(DBT_DIR)]
    result = dbtRunner().invoke(full_args)
    return result.success


if __name__ == "__main__":
    ok = run_dbt(sys.argv[1:] or ["build"])
    sys.exit(0 if ok else 1)
