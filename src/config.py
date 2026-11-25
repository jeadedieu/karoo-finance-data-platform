"""
Central settings for the pipeline.

Every other module imports from here, so paths, credentials and logging are
defined in ONE place. Values come from the .env file (never hard-coded).
"""
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]      # the project folder
load_dotenv(ROOT / ".env")

# --- Where the source files live ---------------------------------------------
DATA_DIR = ROOT / "data"
ORDERS_DIR = DATA_DIR / "orders"

# --- APIs ---------------------------------------------------------------------
FX_API_URL = os.environ.get("FX_API_URL", "https://api.frankfurter.dev/v2/rates")
PAYMENTS_API_URL = os.environ.get("PAYMENTS_API_URL", "http://127.0.0.1:8000")
PAYMENTS_API_KEY = os.environ.get("PAYMENTS_API_KEY", "")

# --- Warehouse database (used from Step 4) ------------------------------------
def db_url() -> str:
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]
    host = os.environ.get("DB_HOST", "localhost")
    port = os.environ.get("DB_PORT", "5432")
    name = os.environ.get("DB_NAME", "karoo")
    return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{name}"


# --- Logging --------------------------------------------------------------------
def setup_logging(level: int = logging.INFO) -> None:
    """Timestamped log lines instead of print(): you can see WHEN each step happened."""
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
