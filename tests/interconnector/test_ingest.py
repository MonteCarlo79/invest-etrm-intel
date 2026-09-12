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
    assert n == 1 and conn.committed == 0      # explicit COMMIT, not conn.commit()
    assert conn.cur.calls[0][0] == "BEGIN"
    assert "DELETE FROM staging.interconnector_trades" in conn.cur.calls[1][0]
    assert "INSERT INTO staging.interconnector_trades" in conn.cur.calls[2][0]
    assert conn.cur.calls[2][1][0]["source_file"] == "src.xlsx:123"
    assert conn.cur.calls[3][0] == "COMMIT"

def _snapshot_fixture(tmp_path):
    f = tmp_path / "kj.xlsx"
    rows = [
        {"送出区域":"东北","送出省份":"黑龙江","受入省份":"安徽","交易类型2":"其他市场化交易","输电通道":"锡泰直流","落地均价（亿千瓦时）":0.02,"落地均价":322.72},
        {"送出区域":"东北","送出省份":"黑龙江","受入省份":"安徽","交易类型2":"其他市场化交易 汇总","输电通道":"汇总","落地均价（亿千瓦时）":2.03,"落地均价":334.79},
        {"送出区域":"东北","送出省份":"黑龙江 汇总","受入省份":"安徽 汇总","交易类型2":"省间绿电交易（市场化交易）","输电通道":"雁淮直流","落地均价（亿千瓦时）":9.07,"落地均价":376.58},
    ]
    with pd.ExcelWriter(f) as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="中长期", index=False)
    return f

def test_parse_mlt_snapshot_flags_subtotals(tmp_path):
    out = ingest.parse_mlt_snapshot(_snapshot_fixture(tmp_path), sheet="中长期")
    assert len(out) == 3
    assert out[0]["is_subtotal"] is False
    assert out[1]["is_subtotal"] is True            # 交易类型 汇总
    assert out[2]["is_subtotal"] is True            # 省份 汇总
    assert out[0]["volume_100m_kwh"] == pytest.approx(0.02)
    assert out[0]["landing_price"] == pytest.approx(322.72)
    assert out[0]["trade_type"] == "其他市场化交易"

def test_replace_snapshot_deletes_label_then_inserts():
    conn = _Conn()
    rows = [{"sheet":"中长期","send_region":"东北","send_prov":"黑龙江","recv_prov":"安徽",
             "trade_type":"其他市场化交易","channel":"锡泰直流","is_subtotal":False,
             "volume_100m_kwh":0.02,"landing_price":322.72}]
    n = ingest.replace_snapshot(conn, "2025-full", rows)
    assert n == 1
    assert conn.cur.calls[0][0] == "BEGIN"
    assert "DELETE FROM staging.interconnector_mlt_snapshot WHERE snapshot_label" in conn.cur.calls[1][0]
    assert conn.cur.calls[1][1] == ("2025-full",)
    assert conn.cur.calls[2][1][0]["snapshot_label"] == "2025-full"
    assert conn.cur.calls[3][0] == "COMMIT"

def test_upsert_channels_writes_all_fields():
    conn = _Conn()
    from services.interconnector.registry import CHANNELS
    n = ingest.upsert_channels(conn, CHANNELS[:1])
    assert n == 1
    sql, params = conn.cur.calls[0]
    assert "INSERT INTO staging.interconnector_channels" in sql
    assert "ON CONFLICT (name) DO UPDATE" in sql
    assert params[0]["name"] == "锡泰直流" and params[0]["send_lon"] == 116.07

def test_pct_override_roundtrip_sql():
    conn = _Conn()
    ingest.set_pct_override(conn, "蒙西", 85.0)
    sql, params = conn.cur.calls[0]
    assert "interconnector_mlt_pct_override" in sql and params == ("蒙西", 85.0)


class TestSnapshotNaturalKey:
    def test_same_sender_under_two_grid_regions_both_insert(self, tmp_path):
        """锡盟二期 appears as 华北 and 蒙西 region rows — same (send_prov, recv,
        type, channel) but different send_region. Both must survive: the unique
        key includes send_region."""
        f = tmp_path / "kj2.xlsx"
        rows = [
            {"送出区域":"华北","送出省份":"锡盟二期","受入省份":"江苏","交易类型2":"省间绿电交易（市场化交易）","输电通道":"锡泰直流","落地均价（亿千瓦时）":1.98,"落地均价":388.89},
            {"送出区域":"蒙西","送出省份":"锡盟二期","受入省份":"江苏","交易类型2":"省间绿电交易（市场化交易）","输电通道":"锡泰直流","落地均价（亿千瓦时）":0.03,"落地均价":238.75},
        ]
        pd.DataFrame(rows).to_excel(f, sheet_name="中长期", index=False)
        out = ingest.parse_mlt_snapshot(f, sheet="中长期")
        assert len(out) == 2
        conn = _Conn()
        n = ingest.replace_snapshot(conn, "2025-full", out)
        assert n == 2
        params = conn.cur.calls[2][1]  # after BEGIN, DELETE
        regions = {p["send_region"] for p in params}
        assert regions == {"华北", "蒙西"}

    def test_snapshot_unique_key_includes_send_region(self):
        ddl = " ".join(ingest.DDL)
        assert "send_region" in ddl.split("interconnector_mlt_snapshot")[1].split("UNIQUE")[1]
        # migration must fix pre-existing narrow-constraint tables idempotently
        assert "ic_mlt_snapshot_uq" in ingest._MIGRATE_SNAPSHOT_UQ
        assert "DROP CONSTRAINT IF EXISTS" in ingest._MIGRATE_SNAPSHOT_UQ

    def test_ensure_tables_runs_snapshot_uq_migration(self):
        conn = _Conn()
        ingest.ensure_tables(conn)
        sqls = [c[0] for c in conn.cur.calls]
        assert any("ic_mlt_snapshot_uq" in s for s in sqls)


class TestGovAgreements:
    def test_ddl_has_agreements_table(self):
        assert any("interconnector_gov_agreements" in stmt for stmt in ingest.DDL)

    def test_save_agreements_full_replace_in_txn(self):
        conn = _Conn()
        rows = [dict(send_prov="青海", recv_prov="上海", annual_gwh=4000.0, period="2026-2028",
                     channel_hint="青豫/灵绍/庆东", source="人民日报2026-06", note="绿电长协,待核实")]
        n = ingest.save_agreements(conn, rows)
        assert n == 1
        sqls = [c[0] for c in conn.cur.calls]
        assert "BEGIN" in sqls[0]
        assert "DELETE FROM staging.interconnector_gov_agreements" in sqls[1]
        assert "INSERT INTO staging.interconnector_gov_agreements" in sqls[2]
        assert conn.cur.calls[2][1][0]["send_prov"] == "青海"

    def test_seed_agreements_only_when_empty(self):
        class CurCount(_Cur):
            def __init__(self, n): super().__init__(); self._n = n
            def fetchone(self): return (self._n,)
        class ConnCount(_Conn):
            def __init__(self, n): super().__init__(); self.cur = CurCount(n)
        # empty table → seeds and returns >0
        n = ingest.seed_agreements_if_empty(ConnCount(0))
        assert n > 0
        # non-empty → no-op
        assert ingest.seed_agreements_if_empty(ConnCount(5)) == 0

    def test_seed_rows_carry_source_and_unverified_note(self):
        for r in ingest.AGREEMENT_SEED:
            assert r["source"] and r["note"], r
        sends = {(r["send_prov"], r["recv_prov"]) for r in ingest.AGREEMENT_SEED}
        assert ("青海", "上海") in sends
