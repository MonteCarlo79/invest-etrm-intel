# tests/spot_mcp/test_tools.py
"""get_province_fundamentals — Strategist tool over province_fundamentals_monthly.

Spec: .superpowers/sdd/2026-11-02-market-fundamentals-tab/task-9-brief.md
- Thin wrapper over services.province_fundamentals.store.read_series (T1).
- Grounding rule preserved: only table data is returned; an empty province
  yields {"rows": [], "note": "无数据"} — never fabricated numbers.
- Compact 口径 (caliber) hint per section so the agent can cite full/省调/推算.

All DB access is mocked by patching store.read_series; no real DB is touched.
"""
from datetime import date, datetime
from pathlib import Path

import pytest

from services.province_fundamentals import store
from services.spot_mcp import tools


def _row(month, period, section, metric, value, unit="万千瓦",
         source="安徽月报.xlsx"):
    return {"province": "安徽", "month": month, "period": period,
            "section": section, "metric": metric, "value": value,
            "unit": unit, "source": source,
            "created_at": datetime(2026, 2, 10, 8, 0)}


@pytest.fixture
def fake_read(monkeypatch):
    """Patch store.read_series; returns (calls, payloads).

    calls: list of {"province", "section", "months", "pg_url"} per invocation.
    payloads: dict section -> rows served to the tool (mutate per test).
    """
    calls: list[dict] = []
    payloads: dict[str, list] = {}

    def _fake(province, section, metrics=None, *, pg_url, months=24):
        calls.append({"province": province, "section": section,
                      "months": months, "pg_url": pg_url})
        return list(payloads.get(section, []))

    monkeypatch.setattr(store, "read_series", _fake)
    monkeypatch.setenv("PGURL", "postgresql://test")
    return calls, payloads


# ── Step 1 acceptance: empty province -> 无数据, never fabricated ──────────────

def test_empty_province_returns_no_data(fake_read):
    out = tools.get_province_fundamentals("青海")
    assert out["rows"] == []
    assert out["note"] == "无数据"
    assert "error" not in out


# ── single section: serialisation + 口径 hint ──────────────────────────────────

def test_single_section_rows_serialised_compactly(fake_read):
    calls, payloads = fake_read
    payloads["capacity"] = [
        _row(date(2026, 1, 1), "year", "capacity", "cap_total_full", 12635.64),
        _row(date(2026, 1, 1), "month", "capacity", "cap_total", 6831.49),
    ]
    out = tools.get_province_fundamentals("安徽", section="capacity")
    assert calls == [{"province": "安徽", "section": "capacity",
                      "months": 24, "pg_url": "postgresql://test"}]
    assert out["count"] == 2
    r_year, r_month = out["rows"]
    # month serialised as compact YYYY-MM; period separates year vs January rows
    assert r_year["month"] == "2026-01" and r_year["period"] == "year"
    assert r_month["month"] == "2026-01" and r_month["period"] == "month"
    assert r_month["value"] == 6831.49 and isinstance(r_month["value"], float)
    assert r_month["unit"] == "万千瓦" and r_month["source"] == "安徽月报.xlsx"
    # compact rows: no created_at / province noise (province is top-level)
    assert "created_at" not in r_month and "province" not in r_month
    assert out["province"] == "安徽"
    # 口径 hint per section: cap_total_full -> full, cap_total -> base => both cited
    assert out["caliber"]["capacity"] == ["base", "full"]
    assert "口径" in out["caliber_hint"]


def test_none_value_survives_not_zeroed(fake_read):
    """Reported-but-empty cell (value None) must survive as None — a zero would
    be fabrication (不编造)."""
    _, payloads = fake_read
    payloads["demand"] = [
        _row(date(2026, 6, 1), "month", "demand", "demand_total", None,
             unit="亿千瓦时"),
    ]
    out = tools.get_province_fundamentals("安徽", section="demand")
    assert out["rows"][0]["value"] is None


# ── section omitted -> all four canonical sections ─────────────────────────────

def test_all_four_sections_when_omitted(fake_read):
    calls, payloads = fake_read
    payloads["capacity"] = [
        _row(date(2026, 1, 1), "month", "capacity", "cap_total", 6831.49)]
    payloads["demand"] = [
        _row(date(2026, 6, 1), "month", "demand", "demand_total", 288.5,
             unit="亿千瓦时")]
    out = tools.get_province_fundamentals("安徽")
    assert [c["section"] for c in calls] == [
        "capacity", "generation", "util_hours", "demand"]
    assert out["count"] == 2
    assert {r["section"] for r in out["rows"]} == {"capacity", "demand"}
    # caliber hint only for sections that actually returned rows
    assert set(out["caliber"]) == {"capacity", "demand"}
    assert out["caliber"]["capacity"] == ["base"]


# ── caliber classification ─────────────────────────────────────────────────────

def test_caliber_full_only_and_derived(fake_read):
    _, payloads = fake_read
    payloads["util_hours"] = [
        _row(date(2026, 1, 1), "year", "util_hours", "hours_wind", 2100.0,
             unit="小时"),
        _row(date(2026, 1, 1), "year", "util_hours", "hours_wind_yoy", 45.0,
             unit="小时"),
        _row(date(2026, 6, 1), "month", "util_hours", "hours_wind_derived",
             180.0, unit="小时"),
    ]
    out = tools.get_province_fundamentals("安徽", section="util_hours")
    assert out["caliber"]["util_hours"] == ["base", "derived"]


def test_caliber_yoy_suffix_does_not_hide_full(fake_read):
    """cap_total_full_yoy belongs to the 全口径 family, not 省调."""
    _, payloads = fake_read
    payloads["capacity"] = [
        _row(date(2026, 1, 1), "year", "capacity", "cap_total_full", 12635.64),
        _row(date(2026, 1, 1), "year", "capacity", "cap_total_full_yoy", 3.2,
             unit="%"),
    ]
    out = tools.get_province_fundamentals("安徽", section="capacity")
    assert out["caliber"]["capacity"] == ["full"]


# ── input handling ─────────────────────────────────────────────────────────────

def test_invalid_section_returns_note_no_db_call(fake_read):
    calls, _ = fake_read
    out = tools.get_province_fundamentals("安徽", section="bogus")
    assert calls == []                      # never hits the store
    assert out["rows"] == []
    assert "bogus" in out["note"]
    assert out["valid_sections"] == ["capacity", "generation", "util_hours",
                                     "demand"]


def test_english_province_mapped_to_chinese(fake_read):
    calls, _ = fake_read
    tools.get_province_fundamentals("Anhui", section="capacity")
    assert calls[0]["province"] == "安徽"


def test_national_mapped_to_quanguo(fake_read):
    calls, _ = fake_read
    tools.get_province_fundamentals("National", section="demand")
    assert calls[0]["province"] == "全国"


def test_months_passthrough(fake_read):
    calls, _ = fake_read
    tools.get_province_fundamentals("安徽", section="capacity", months=12)
    assert calls[0]["months"] == 12


# ── error path: error != empty (不编造 applies to failures too) ────────────────

def test_db_error_returns_error_dict_not_no_data(fake_read, monkeypatch):
    def _boom(*a, **kw):
        raise RuntimeError("connection refused")
    monkeypatch.setattr(store, "read_series", _boom)
    out = tools.get_province_fundamentals("安徽", section="capacity")
    assert out["rows"] == []
    assert "connection refused" in out["error"]
    assert out.get("note") != "无数据"      # failure must not masquerade as empty


# ── agent registration in apps/spot-market/app.py (source-level sanity) ───────

def test_agent_tool_registered_in_app():
    src = (Path(__file__).resolve().parents[2]
           / "apps" / "spot-market" / "app.py").read_text(encoding="utf-8")
    # tool schema registered in _AGENT_TOOLS
    assert '"name": "get_province_fundamentals"' in src
    # dispatch branch wired
    assert 'name == "get_province_fundamentals"' in src
    # analytical-framework line maps the question types to the tool
    assert "get_province_fundamentals（" in src
    for token in ("装机", "发电量", "利用小时"):
        assert token in src.split("get_province_fundamentals（")[0][-120:]
