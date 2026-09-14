# tests/hermes/test_fuel_fleet_screener.py
from unittest.mock import MagicMock, patch
import services.hermes.fuel_fleet_screener as sc


def test_extract_json_from_fenced_block():
    text = '```json\n{"coal_price_yuan_t": 850, "gas_price_yuan_m3": 3.1, "fleet_segments": []}\n```'
    data = sc._extract_json(text)
    assert data["coal_price_yuan_t"] == 850


def test_screen_upserts_extraction(monkeypatch):
    fake_data = {"coal_price_yuan_t": 850.0, "gas_price_yuan_m3": 3.1,
                 "fleet_segments": [{"fuel": "coal", "capacity_mw": 40000,
                                     "heat_rate_kj_kwh": 8200, "vom_yuan_mwh": 12,
                                     "label": "coal_main"}],
                 "confidence": "high", "source_url": "kb:doc1"}
    monkeypatch.setattr(sc, "_search_kb", lambda *a, **k: [("chunk text", "doc1.pdf")])
    monkeypatch.setattr(sc, "_claude_extract", lambda *a, **k: fake_data)
    captured = {}

    def fake_upsert(rows, pg_url, source):
        captured["rows"] = rows
        return {"upserted": len(rows), "conflicts": 0, "errors": []}

    monkeypatch.setattr(sc, "upsert_fuel_fleet_rows", fake_upsert)
    out = sc.screen_fuel_fleet("postgresql://x", "key", provinces=["山东"])
    assert out["extracted"] == 1 and out["upserted"] == 1
    row = captured["rows"][0]
    assert row["province"] == "山东" and row["coal_price_yuan_t"] == 850.0
    assert row["source"].startswith("kb:") or "doc1" in row["source"]


def test_screen_skips_failed_extraction(monkeypatch):
    monkeypatch.setattr(sc, "_search_kb", lambda *a, **k: [])
    monkeypatch.setattr(sc, "_claude_extract", lambda *a, **k: None)
    monkeypatch.setattr(sc, "upsert_fuel_fleet_rows",
                        lambda *a, **k: {"upserted": 0, "conflicts": 0, "errors": []})
    out = sc.screen_fuel_fleet("postgresql://x", "key", provinces=["山东"])
    assert out["extracted"] == 0 and out["upserted"] == 0
