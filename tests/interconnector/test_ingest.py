# tests/interconnector/test_ingest.py
from datetime import date
import pandas as pd
import pytest
from services.interconnector import ingest

@pytest.mark.parametrize("raw,year,exp", [
    ("1月", 2026, (date(2026,1,1), date(2026,1,31))),
    ("6月 ", 2026, (date(2026,6,1), date(2026,6,30))),      # trailing space
    ("3-12月", 2026, (date(2026,3,1), date(2026,12,31))),
    ("5-8月", 2026, (date(2026,5,1), date(2026,8,31))),
    ("9-10月", 2026, (date(2026,9,1), date(2026,10,31))),
    ("1-12月", 2026, (date(2026,1,1), date(2026,12,31))),
    ("2026", 2026, (date(2026,1,1), date(2026,12,31))),
])
def test_normalize_month(raw, year, exp):
    assert ingest.normalize_month(raw, year) == exp

@pytest.mark.parametrize("raw,anchor", [
    ("蒙东&黑龙江&吉林", "蒙东"), ("锡盟", "锡盟"), ("黑龙江/吉林/辽宁/蒙东", "黑龙江"),
    ("华北/冀北/山西/蒙西", "山西"), ("坤渝配套新能源", "重庆"), ("陕武配套新能源", "陕西"),
    ("南方区域", "云南"), ("黑吉辽", "黑龙江"), ("华北锡盟二期冀北河北山西蒙西", "锡盟"),
])
def test_anchor_for(raw, anchor):
    assert ingest.anchor_for(raw) == anchor

def _fixture_xlsx(tmp_path, rows):
    f = tmp_path / "hd.xlsx"
    pd.DataFrame(rows).to_excel(f, index=False)
    return f

def test_parse_huadong_cleans_channels_and_anchors(tmp_path):
    rows = [{"标的月份":"1月","标的期间类型":"月度","华东省份-XX":"江苏","对端送出省份":"蒙东",
             "通道1":"\n鲁固直流","通道2":"雁淮直流","通道3":None,
             "成交电量（校核前）MWh":36443.61,"成交电量（校核后）MWh":36443.61,
             "上网侧均价 元/MWh":124,"落地侧均价 元/MWh":275,
             "景融成交电量 MWh":14410.11,"景融成交均价 元/MWh":275.42,"通道费":151}]
    out = ingest.parse_huadong(_fixture_xlsx(tmp_path, rows), year=2026)
    assert len(out) == 1
    r = out[0]
    assert r["channel_1"] == "鲁固直流" and r["channel_2"] == "雁淮直流" and r["channel_3"] is None
    assert r["send_anchor"] == "蒙东" and r["recv_province"] == "江苏"
    assert r["month_start"] == date(2026,1,1) and r["month_end"] == date(2026,1,31)
    assert r["vol_post_mwh"] == pytest.approx(36443.61)
    assert r["jingrong_price"] == pytest.approx(275.42)

def test_parse_huadong_annual_period_covers_year(tmp_path):
    rows = [{"标的月份":"2026","标的期间类型":"年度","华东省份-XX":"上海","对端送出省份":"四川",
             "通道1":"复奉直流","通道2":None,"通道3":None,
             "成交电量（校核前）MWh":100,"成交电量（校核后）MWh":100,
             "上网侧均价 元/MWh":300,"落地侧均价 元/MWh":400,
             "景融成交电量 MWh":None,"景融成交均价 元/MWh":None,"通道费":80}]
    out = ingest.parse_huadong(_fixture_xlsx(tmp_path, rows), year=2026)
    assert out[0]["month_start"] == date(2026,1,1) and out[0]["month_end"] == date(2026,12,31)

def test_file_fingerprint_format():
    assert ingest.file_fingerprint("华东跨省数据汇总.xlsx", 44092) == "华东跨省数据汇总.xlsx:44092"

class _Cur:
    def __init__(self): self.calls = []
    def execute(self, sql, params=None): self.calls.append((sql, params))
    def executemany(self, sql, seq): self.calls.append((sql, list(seq)))
    def __enter__(self): return self
    def __exit__(self, *exc): return False
class _Conn:
    def __init__(self): self.cur = _Cur(); self.committed = 0
    def cursor(self): return self.cur
    def commit(self): self.committed += 1

def test_replace_trades_deletes_then_inserts():
    conn = _Conn()
    rows = [{"target_month_raw":"1月","period_type":"月度","month_start":date(2026,1,1),
             "month_end":date(2026,1,31),"recv_province":"江苏","send_raw":"蒙东","send_anchor":"蒙东",
             "channel_1":"鲁固直流","channel_2":None,"channel_3":None,
             "vol_pre_mwh":1.0,"vol_post_mwh":1.0,"send_price":1,"land_price":2,
             "jingrong_vol_mwh":None,"jingrong_price":None,"channel_fee":1}]
    n = ingest.replace_trades(conn, rows, "src.xlsx:123")
    assert n == 1 and conn.committed == 1
    assert "DELETE FROM staging.interconnector_trades" in conn.cur.calls[0][0]
    assert "INSERT INTO staging.interconnector_trades" in conn.cur.calls[1][0]
    assert conn.cur.calls[1][1][0]["source_file"] == "src.xlsx:123"
