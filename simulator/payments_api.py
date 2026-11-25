"""
A fake payment-provider API (think: a tiny Stripe), serving data/payments_api.db.

It behaves like real third-party APIs, including the annoying parts:
  - needs an API key              (Authorization: Bearer <key>)
  - returns data in pages         (?page=1&page_size=500, max 1000)
  - supports incremental pulls    (?updated_since=2026-11-01T00:00:00Z)
  - rate limit: 10 requests per 5 seconds -> HTTP 429 with a Retry-After header
  - random failures: ~4% HTTP 503, ~2% very slow responses (3 seconds)
  - amounts are in MINOR units (cents): 125000 = 1,250.00

Run:   python simulator/payments_api.py
Docs:  http://127.0.0.1:8000/docs   (interactive, try requests in the browser)
Set CHAOS=0 in .env to switch off failures and slowness (useful for tests).
"""
from __future__ import annotations

import os
import random
import sqlite3
from contextlib import closing
import time
from collections import deque
from pathlib import Path

import uvicorn
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.responses import JSONResponse

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

DB_PATH = ROOT / "data" / "payments_api.db"
API_KEY = os.environ.get("PAYMENTS_API_KEY", "dev-secret-key")
CHAOS = os.environ.get("CHAOS", "1") != "0"
RATE_LIMIT, RATE_WINDOW_S = 10, 5.0

app = FastAPI(title="Karoo Payments API (simulated)", version="1.0")
_recent_requests: deque[float] = deque()
# Bearer-token security: adds an "Authorize" button to the /docs page.
bearer = HTTPBearer(auto_error=False, description="Paste the API key (without the word Bearer)")


def _check_rate_limit() -> JSONResponse | None:
    now = time.monotonic()
    while _recent_requests and now - _recent_requests[0] > RATE_WINDOW_S:
        _recent_requests.popleft()
    if len(_recent_requests) >= RATE_LIMIT:
        retry_after = max(1, int(RATE_WINDOW_S - (now - _recent_requests[0])) + 1)
        return JSONResponse(status_code=429, headers={"Retry-After": str(retry_after)},
                            content={"error": "rate_limited", "message": f"Max {RATE_LIMIT} requests per {RATE_WINDOW_S:.0f}s"})
    _recent_requests.append(now)
    return None


@app.get("/health")
def health():
    return {"status": "ok", "database_found": DB_PATH.exists()}


@app.get("/v1/payments")
def list_payments(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer),
    updated_since: str = Query("1970-01-01T00:00:00Z", description="Only payments updated at or after this UTC time"),
    updated_before: str = Query("9999-12-31T23:59:59Z", description="Only payments updated before this UTC time"),
    page: int = Query(1, ge=1),
    page_size: int = Query(100, ge=1, le=1000),
):
    if credentials is None or credentials.credentials != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    if (limited := _check_rate_limit()) is not None:
        return limited
    if CHAOS:
        roll = random.random()
        if roll < 0.04:
            return JSONResponse(status_code=503, content={"error": "service_unavailable", "message": "Try again later"})
        if roll < 0.06:
            time.sleep(3)

    offset = (page - 1) * page_size
    with closing(sqlite3.connect(DB_PATH)) as conn:   # closes the file after each request
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT payment_id, order_id, amount, currency, status, created_at, updated_at
               FROM payments
               WHERE updated_at >= ? AND updated_at < ?
               ORDER BY updated_at, payment_id
               LIMIT ? OFFSET ?""",
            (updated_since, updated_before, page_size + 1, offset),
        ).fetchall()

    has_more = len(rows) > page_size
    return {"data": [dict(r) for r in rows[:page_size]], "page": page, "page_size": page_size, "has_more": has_more}


if __name__ == "__main__":
    print(f"Payments API on http://127.0.0.1:8000  (chaos {'ON' if CHAOS else 'OFF'}) - Ctrl+C to stop")
    uvicorn.run(app, host="127.0.0.1", port=8000, log_level="warning")
