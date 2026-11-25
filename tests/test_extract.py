"""
Unit tests for the extract layer. No network, no database: the HTTP session is faked,
so these run in under a second and test only OUR logic (retries, parsing, schema checks).
"""
import json

import pandas as pd
import pytest
import requests

from src import extract
from src.extract import ExtractError, get_with_retries


# --- a fake HTTP session that replays a scripted list of responses -----------------
class FakeResponse:
    def __init__(self, status: int, body=None, headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}
        self.text = json.dumps(body)

    def json(self):
        return self._body


class FakeSession:
    def __init__(self, script):
        self.script = list(script)
        self.calls = 0

    def get(self, url, params=None, timeout=None):
        self.calls += 1
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


@pytest.fixture(autouse=True)
def no_sleeping(monkeypatch):
    """Record waits instead of actually sleeping, so tests are instant."""
    waits = []
    monkeypatch.setattr(extract.time, "sleep", waits.append)
    return waits


# --- retry behaviour -------------------------------------------------------------------
def test_retries_server_errors_then_succeeds(no_sleeping):
    session = FakeSession([FakeResponse(503), FakeResponse(500), FakeResponse(200, {"ok": True})])
    assert get_with_retries(session, "http://x") == {"ok": True}
    assert session.calls == 3
    assert len(no_sleeping) == 2


def test_rate_limit_waits_as_long_as_retry_after_says(no_sleeping):
    session = FakeSession([FakeResponse(429, headers={"Retry-After": "7"}), FakeResponse(200, [])])
    get_with_retries(session, "http://x")
    assert no_sleeping == [7.0]


def test_timeouts_are_retried():
    session = FakeSession([requests.Timeout("slow"), FakeResponse(200, [])])
    assert get_with_retries(session, "http://x") == []


def test_client_errors_fail_fast_without_retrying():
    session = FakeSession([FakeResponse(401, {"detail": "bad key"})])
    with pytest.raises(ExtractError, match="401"):
        get_with_retries(session, "http://x")
    assert session.calls == 1          # a wrong API key won't fix itself: no retries


def test_gives_up_after_max_attempts():
    session = FakeSession([FakeResponse(503)] * 3)
    with pytest.raises(ExtractError, match="after 3 attempts"):
        get_with_retries(session, "http://x", max_attempts=3)


def test_backoff_grows_exponentially_and_is_capped():
    waits = [extract._backoff(n) for n in (1, 2, 3, 4, 10)]
    assert 1 <= waits[0] < 1.5 and 2 <= waits[1] < 2.5 and 4 <= waits[2] < 4.5
    assert waits[4] <= 30.5


# --- order files --------------------------------------------------------------------
def _write_json(tmp_path, orders):
    path = tmp_path / "orders_2026-06-01.json"
    path.write_text(json.dumps({"export_date": "2026-06-01", "orders": orders}), encoding="utf-8")
    return path


GOOD_JSON_ORDER = {
    "id": "KG-1", "created_at": "2026-06-01T10:00:00+02:00", "updated_at": "2026-06-01T10:00:00+02:00",
    "customer": {"id": "C00001", "country": "ZA"}, "line": {"sku": "P001", "qty": 2, "unit_price": 129.0},
    "currency": "ZAR", "total": 258.0, "status": "completed",
}


def test_json_orders_are_flattened_into_the_common_shape(tmp_path):
    df = extract.read_order_file(_write_json(tmp_path, [GOOD_JSON_ORDER]))
    row = df.iloc[0]
    assert list(df.columns[:11]) == extract.ORDER_COLUMNS
    assert (row.order_id, row.customer_id, row.product_id, row.quantity) == ("KG-1", "C00001", "P001", "2")


def test_json_nan_is_treated_as_missing(tmp_path):
    path = tmp_path / "orders_2026-06-02.json"
    order = {**GOOD_JSON_ORDER, "customer": {"id": float("nan"), "country": "ZA"}}
    path.write_text(json.dumps({"orders": [order]}), encoding="utf-8")   # json writes NaN
    assert extract.read_order_file(path).iloc[0].customer_id is None


def test_json_schema_change_fails_loudly(tmp_path):
    broken = {k: v for k, v in GOOD_JSON_ORDER.items() if k != "currency"}
    with pytest.raises(ExtractError, match="no field currency"):
        extract.read_order_file(_write_json(tmp_path, [broken]))


def test_csv_with_missing_column_fails_loudly(tmp_path):
    path = tmp_path / "orders_2025-11-01.csv"
    pd.DataFrame({"order_id": ["KG-1"], "order_date": ["2025-11-01 10:00:00"]}).to_csv(path, index=False)
    with pytest.raises(ExtractError, match="Schema change"):
        extract.read_order_file(path)


def test_csv_values_stay_as_text(tmp_path):
    path = tmp_path / "orders_2025-11-01.csv"
    cols = sorted(extract.EXPECTED_CSV_COLUMNS)
    row = {c: "x" for c in cols} | {"customer_id": "007", "unit_price": "R1,250.00"}
    pd.DataFrame([row]).to_csv(path, index=False)
    df = extract.read_order_file(path)
    assert df.iloc[0].customer_id == "007"            # leading zeros kept
    assert df.iloc[0].unit_price == "R1,250.00"       # not interpreted during extract


def test_missing_daily_file_stops_the_run(tmp_path, monkeypatch):
    monkeypatch.setattr(extract.config, "ORDERS_DIR", tmp_path)
    from datetime import date
    with pytest.raises(ExtractError, match="Missing order file"):
        extract.extract_orders(date(2026, 1, 1), date(2026, 1, 2))
