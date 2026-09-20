# tests/coal_index/test_cec_scraper.py
"""Tests for the CEC coal index scraper."""
import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

from services.coal_index.cec_scraper import parse_rows, upsert_rows, latest_coal_price, run_daily

_SAMPLE = Path("/tmp/cec_zdlzs_real.json")


def _payload():
    return json.loads(_SAMPLE.read_text())


def test_parse_rows_counts():
    rows = parse_rows(_payload())
    by_index = {}
    for name, d, v in rows:
        by_index.setdefault(name, []).append((d, v))
    assert set(by_index) == {
        "caofeidian_5500", "caofeidian_5000", "caofeidian_4500",
        "ceci_synth_5500", "ceci_synth_5000", "ceci_deal_5500", "ceci_deal_5000",
        "ceci_fob_5500", "ceci_fob_5000", "spec_7000",
        "ceci_composite", "ceci_supply", "ceci_demand", "ceci_inventory",
        "ceci_price", "ceci_shipping",
    }
    assert len(by_index["caofeidian_5500"]) == 1334
    assert len(by_index["ceci_synth_5500"]) == 361
    assert len(by_index["spec_7000"]) == 342
    assert len(by_index["ceci_composite"]) == 347


def test_parse_rows_latest_values():
    rows = parse_rows(_payload())
    latest = {n: (d, v) for n, d, v in rows if d == date(2026, 9, 18)}
    assert latest["caofeidian_5500"] == (date(2026, 9, 18), 977.0)
    assert latest["ceci_synth_5500"] == (date(2026, 9, 18), 799.0)
    assert latest["ceci_deal_5500"] == (date(2026, 9, 18), 981.0)
    assert latest["spec_7000"] == (date(2026, 9, 18), 1209.0)


def test_upsert_rows_idempotent():
    cur = MagicMock()
    rows = [("caofeidian_5500", date(2026, 9, 18), 977.0)]
    n1 = upsert_rows(cur, rows)
    n2 = upsert_rows(cur, rows)  # second run — same SQL, no error
    assert n1 == n2 == 1
    assert cur.execute.call_count == 1 + 1 + 2  # ensure_table x2 + upsert x2
    sql = cur.execute.call_args_list[-1].args[0]
    assert "ON CONFLICT (index_name, date) DO UPDATE" in sql


def test_latest_coal_price():
    eng = MagicMock()
    ctx = eng.connect.return_value.__enter__.return_value
    ctx.execute.return_value.fetchone.return_value = (799.0,)
    assert latest_coal_price(eng) == 799.0
    ctx.execute.return_value.fetchone.return_value = None
    assert latest_coal_price(eng) is None


def test_run_daily_failure_alerts_feishu():
    feishu = MagicMock()
    with patch("services.coal_index.cec_scraper.fetch_index", side_effect=RuntimeError("boom")):
        out = run_daily("postgresql://x", feishu=feishu, owner_open_id="ou_1")
    assert out["ok"] is False and "boom" in out["error"]
    feishu.send_text.assert_called_once()
    assert "抓取失败" in feishu.send_text.call_args.kwargs["text"]
