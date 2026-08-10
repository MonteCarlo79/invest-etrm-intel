import datetime as dt

import pytest

from services.spot_ingest.monthly_report import is_spot_monthly_pdf, infer_report_month


def test_matches_monthly_report():
    assert is_spot_monthly_pdf("电力现货市场价格与运行月报（2026年6月）.pdf") is True


def test_rejects_daily_report():
    # 日报 must NOT match — it has its own pipeline (is_spot_pdf)
    assert is_spot_monthly_pdf("电力现货市场价格与运行日报2026-06-01.pdf") is False


def test_rejects_exchange_monthly():
    # provincial exchange 月报 must NOT match — handled by is_exchange_report
    assert is_spot_monthly_pdf("山东电力交易中心2026年6月月报.pdf") is False


def test_requires_pdf_extension():
    assert is_spot_monthly_pdf("电力现货市场价格与运行月报（2026年6月）.xlsx") is False


def test_infer_month_full_width_parens():
    assert infer_report_month("电力现货市场价格与运行月报（2026年6月）.pdf") == dt.date(2026, 6, 1)


def test_infer_month_zero_padded():
    assert infer_report_month("电力现货市场价格与运行月报（2026年06月）.pdf") == dt.date(2026, 6, 1)


def test_infer_month_yearless_returns_none():
    assert infer_report_month("电力现货市场价格与运行月报（6月）.pdf") is None


from unittest.mock import MagicMock, patch

from services.spot_ingest.monthly_report import extract_monthly_json, validate_monthly_data


def _province(**over):
    base = {
        "province_cn": "山东", "run_status": "正式运行",
        "mlt_volume_yi_kwh": 164.63, "mlt_avg_price": 0.344, "mlt_coverage_pct": 62.58,
        "rt_volume_yi_kwh": None, "rt_avg_price": 0.346, "rt_mom_pct": -12.42,
        "da_volume_yi_kwh": None, "da_avg_price": 0.315, "da_mom_pct": 4.41,
    }
    base.update(over)
    return base


def _data(n=25, **nat_over):
    national = {
        "rt_total_volume_yi_kwh": 4469.26, "rt_avg_price": 0.291,
        "da_total_volume_yi_kwh": 4493.13, "da_avg_price": 0.294,
        "mlt_coverage_volume_yi_kwh": 12951.62, "mlt_coverage_pct": 66.04,
        "mlt_avg_price": 0.313,
    }
    national.update(nat_over)
    return {"national": national, "provinces": [_province() for _ in range(n)]}


def test_validate_clean_data_no_warnings():
    assert validate_monthly_data(_data()) == []


def test_validate_few_provinces_warns():
    warnings = validate_monthly_data(_data(n=5))
    assert any("省份" in w for w in warnings)


def test_validate_zero_provinces_raises():
    with pytest.raises(ValueError):
        validate_monthly_data({"national": {}, "provinces": []})


def test_validate_price_out_of_range_nulled():
    data = _data()
    data["provinces"][0]["rt_avg_price"] = 5.0
    warnings = validate_monthly_data(data)
    assert data["provinces"][0]["rt_avg_price"] is None
    assert any("rt_avg_price" in w for w in warnings)


def test_validate_coverage_pct_out_of_range_nulled():
    data = _data()
    data["provinces"][0]["mlt_coverage_pct"] = 150.0
    warnings = validate_monthly_data(data)
    assert data["provinces"][0]["mlt_coverage_pct"] is None
    assert warnings


def test_validate_unknown_province_dropped():
    data = _data()
    data["provinces"].append(_province(province_cn="亚特兰蒂斯"))
    warnings = validate_monthly_data(data)
    assert len(data["provinces"]) == 25
    assert any("亚特兰蒂斯" in w for w in warnings)


def test_extract_monthly_json_parses_claude_output():
    payload = '{"national": {"rt_avg_price": 0.291}, "provinces": []}'
    fake_resp = MagicMock()
    fake_resp.content = [MagicMock(text=f"Here is the data:\n{payload}")]
    fake_client = MagicMock()
    fake_client.messages.create.return_value = fake_resp
    with patch("shared.anthropic_client.make_client", return_value=fake_client):
        result = extract_monthly_json("page text", dt.date(2026, 6, 1), "key")
    assert result["national"]["rt_avg_price"] == 0.291


def test_extract_monthly_json_raises_on_garbage():
    fake_resp = MagicMock()
    fake_resp.content = [MagicMock(text="no json here")]
    fake_client = MagicMock()
    fake_client.messages.create.return_value = fake_resp
    with patch("shared.anthropic_client.make_client", return_value=fake_client):
        with pytest.raises(ValueError):
            extract_monthly_json("page text", dt.date(2026, 6, 1), "key")


from services.spot_ingest.monthly_report import upsert_monthly_rows


def test_upsert_writes_national_and_provinces():
    data = _data(n=2)
    executed = []

    fake_cur = MagicMock()
    fake_cur.execute.side_effect = lambda sql, params: executed.append(params)
    fake_conn = MagicMock()
    fake_conn.cursor.return_value.__enter__.return_value = fake_cur
    fake_get_conn = MagicMock()
    fake_get_conn.return_value.__enter__.return_value = fake_conn

    with patch("services.knowledge_pool.db.get_conn", fake_get_conn):
        result = upsert_monthly_rows(data["national"], data["provinces"], dt.date(2026, 6, 1), "test.pdf")

    assert result == {"national_written": True, "provinces_upserted": 2}
    assert len(executed) == 3  # 1 national + 2 provinces
    assert executed[0]["report_month"] == dt.date(2026, 6, 1)
    assert executed[0]["source_file"] == "test.pdf"
    assert executed[1]["province_en"] == "Shandong"
    fake_conn.commit.assert_called_once()
