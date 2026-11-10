# tests/services/retail_risk/test_contracts_crm.py
"""Province 零售签约 sheets -> CONTRACTS_COLS (fixtures mirror real header rows).

Convention: share_ratio = CHANNEL's share of the margin; channel_fee_cny_mwh =
per-MWh channel fee; spread_cny_mwh set for 价差/联动上浮 contracts (price NULL)."""
import datetime
from pathlib import Path

import pandas as pd
import pytest

from services.retail_risk import schemas
from services.retail_risk.parsers import contracts_crm as cc


def _xlsx(path: Path, sheet: str, header: list, rows: list, header_row: int = 0):
    data = [[None] * len(header)] * header_row + [header] + rows
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as w:
        pd.DataFrame(data).to_excel(w, sheet_name=sheet, index=False, header=False)
    return path


def _one(df, name):
    assert list(df.columns) == schemas.CONTRACTS_COLS
    return df[df.customer_name == name].iloc[0]


def test_jinan(tmp_path):
    hdr = ["零售用户名称", "BD", "来源", "绑定开始", "绑定结束", "用户电价类型", "上浮（元/MWh）",
           "是否含绿电", "年度预估电量(MWh）", "合同状态", "渠道费"]
    f = _xlsx(tmp_path / "6 【冀南测算】x【分月分时】.xlsx", "冀南零售签约原表", hdr, [
        ["衡水百纳", "/", "渠道", "2026-01-01", "2026-12-31", "固定价差-混合电价", 4.0, None, 10210.0, "已生效", "80%*上浮价格*年度电量"],
        ["坤天新能源", "供应链", "供应链", "2026-01-01", "2026-12-31", "固定价差-混合电价", -1.3, None, 214500.0, "已生效", "1.5万"]])
    out = cc.parse_contracts(f, "冀南")
    a = _one(out, "衡水百纳")
    assert a.contract_type == "indexed" and a.spread_cny_mwh == 4.0 and pd.isna(a.price_cny_mwh)
    assert a.share_ratio == pytest.approx(0.8) and a.annual_mwh == 10210.0
    assert pd.isna(a.bd_name) and a.customer_source == "渠道" and a.status == "active"
    b = _one(out, "坤天新能源")
    assert b.channel_fee_lump_cny == 15000.0 and pd.isna(b.share_ratio) and b.bd_name == "供应链"


def test_zhejiang(tmp_path):
    hdr = ["零售用户名称", "BD", "用户电价类型", "是否含绿电", "是否封顶", "联动上浮套餐", "上浮金额",
           "固定价格套餐 ", "新套餐价格（元/度）", "年度预估电量(万度）", "签约渠道分成比例", "执行调整后渠道分成比例"]
    f = _xlsx(tmp_path / "7 【浙江测算】x【分月分时】.xlsx", "浙江零售签约原表", hdr, [
        ["乐清华亿", "张笃海", "分时电价", None, None, "联动上浮0.002", 0.002, None, "/", 2.0, 0.805, 0.6],
        ["德爵精酿", "邱磊", "分时电价", None, None, None, None, 0.37, "/", 4.7, 0.85, "/"]], header_row=1)
    out = cc.parse_contracts(f, "浙江")
    a = _one(out, "乐清华亿")
    assert a.contract_type == "indexed" and a.spread_cny_mwh == pytest.approx(2.0)   # 元/度 -> 元/MWh
    assert a.annual_mwh == pytest.approx(20.0) and a.share_ratio == 0.6              # 万度; adjusted share wins
    assert a.bd_name == "张笃海"
    b = _one(out, "德爵精酿")
    assert b.contract_type == "fixed" and b.price_cny_mwh == pytest.approx(370.0) and b.share_ratio == 0.85


def test_guangdong_skips_summary_rows(tmp_path):
    hdr = ["No", "企业名称", "企业\n编号", "销售类型", "代理起始月份", "合同到期日", "预估年度交易电量",
           "合同\n模式", "购电种类", "固定部分占比", "固定部分\n平段价格\n（合同价）", "固定+上浮（合同价）",
           "固定部分\n平段价格\n（裸价）", "固定+上浮\n（裸价）", "联动部分占比",
           "固定部分度电居间费（元/兆瓦时）", "客户来源", "具体名称"]
    f = _xlsx(tmp_path / "1 【广东测算】x【分月-不分月分时】.xlsx", "零售签约原表", hdr, [
        [1, "东莞愉丰", "E1", "渠道", 202604.0, 202612.0, 250, "固定价格+市场联动", "火电", 0.9, 372, 372, 350, 350, 0.1, 22.0, "渠道", "张三"],
        [None, None, None, None, None, None, 22076, "拟签约总电量（万度）", None, None, None, None, None, None, None, None, None, None]])
    out = cc.parse_contracts(f, "广东")
    assert len(out) == 1
    a = _one(out, "东莞愉丰")
    assert a.annual_mwh == pytest.approx(2500.0)                                    # 万度 -> MWh
    assert a.price_cny_mwh == 372 and a.channel_fee_cny_mwh == 22.0
    assert a.start_date == datetime.date(2026, 4, 1) and a.end_date == datetime.date(2026, 12, 31)
    assert a.contract_type == "indexed_band" and a.k1 == 0.9 and a.k2 == pytest.approx(0.1)
    assert a.channel_name == "张三" and a.customer_source == "渠道"


def test_fujian(tmp_path):
    hdr = ["零售用户名称", "BD", "来源", "绑定开始", "绑定结束", "签订日期", "套餐类型", "固定价",
           "度电服务费（元/兆瓦时）", "归用户分享/分摊比例", "绿电环境权益（已签约绿电套餐用户可填）",
           "年度预估电量(MWh）", "合同状态", "客户类型", "备注", "渠道名", "渠道分成比例"]
    f = _xlsx(tmp_path / "3 【福建测算】x【分月不分时】.xlsx", "福建零售签约原表", hdr, [
        ["福州亿利达", "夏世兴XSX", "渠道", "2026-01-01", "2026-12-31", None, "固定价格", 399.0, None, None, None, 127.962, None, None, None, "夏世兴", 0.8],
        ["莆田某厂", "庄小东", "渠道", "2026-01-01", "2026-12-31", None, "不分时段市场均价+服务费", None, 10.0, None, None, 50.0, None, None, None, "庄小东", 0.88]])
    out = cc.parse_contracts(f, "福建")
    a = _one(out, "福州亿利达")
    assert a.contract_type == "fixed" and a.price_cny_mwh == 399.0 and a.share_ratio == 0.8
    assert a.channel_name == "夏世兴" and a.status == "active"
    b = _one(out, "莆田某厂")
    assert b.contract_type == "indexed" and b.spread_cny_mwh == 10.0


def test_shanghai(tmp_path):
    hdr = ["客户名称", "状态", "绑定日期", "Unnamed: 3", "客户联系人", "客户联系方式", "预估年度交易电量/MWH",
           "月均电量/MWH", "绿电需要比例", "优先级", "备注", "能否用年度绿电", "x12", "x13", "是否PPA",
           "渠道名称", "渠道分成模式", "固定价部分政策", "分成部分政策", "合同签订模式", "现货联动比例",
           "偏差考核分摊比例（甲方", "场内价格"]
    f = _xlsx(tmp_path / "5 【上海测算】x【分月不分时】.xlsx", "上海零售总表", hdr, [
        ["德榔能物业", "生效中", None, None, None, None, 2500.0, 208.3, 0.4, None, None, None, None, None, None,
         "萍钛", "利润分成", None, 0.9, "固定价", 0.05, 0.0, 470],
        ["某待审核", "待审核", None, None, None, None, 100.0, 8.3, None, None, None, None, None, None, None,
         "徐总", "固定价+分成", 78.0, 0.5, "固定价", 0.05, 0.0, 477.9]], header_row=1)
    out = cc.parse_contracts(f, "上海")
    a = _one(out, "德榔能物业")
    assert a.price_cny_mwh == 470 and a.share_ratio == 0.9 and a.channel_name == "萍钛"
    assert a.status == "active" and a.start_date == datetime.date(2026, 1, 1)
    assert _one(out, "某待审核").status == "pending"


def test_jiangsu(tmp_path):
    hdr = ["零售用户名称", "预估年度火电电量/万度", "预估年度绿电电量/万度", "绿电零售价（元/度）", "套餐类型",
           "属性", "对应渠道", "备注", "合同开始日期", "合同结束日期", "长协比例（%）", "长协盈利甲方占比（%）",
           "月竞比例（%）", "月度盈利甲方占比（%）", "代购比例（%）", "固定结算价（元度）", "分成基准价（元度）",
           "长协浮动（元度）", "月竞浮动（元度）", "代购浮动（元度）", "零售长协价预估（元/度）",
           "零售月度价预估（元/度）", "零售代购价预估（元/度）", "零售均价预估\n（元/度）"]
    f = _xlsx(tmp_path / "4 【江苏测算-更新】x-汇总.xlsx", "利润测算", hdr, [
        ["云龙餐饮", 70.0, 0.0, None, "固定价格类", "渠道", "南京何总", None, "2025-10-01", "2026-12-31",
         0, 0, 0, 0, 0, 0.39, 0, 0, 0, 0, 0.39, 0.39, 0, 0.39],
        ["过期厂", 10.0, 0.0, None, "价格浮动类", "直销", None, None, "2024-01-01", "2025-06-30",
         100, 0, 0, 0, 0, 0, 0, -0.001, 0, 0, 0.343, 0, 0, 0.343]], header_row=1)
    out = cc.parse_contracts(f, "江苏")
    assert set(out.customer_name) == {"云龙餐饮"}                      # ended before 2026 -> dropped
    a = _one(out, "云龙餐饮")
    assert a.contract_type == "fixed" and a.price_cny_mwh == pytest.approx(390.0)
    assert a.annual_mwh == pytest.approx(700.0) and a.channel_name == "南京何总"
    assert a.customer_source == "渠道"


def test_channel_fee_margin_share_and_fees():
    from services.retail_risk.pnl_bridge import channel_fee
    c = pd.DataFrame([
        # fixed 400 vs benchmark 350, channel 80% of margin, 1200 MWh/yr -> 100/month
        {"contract_type": "fixed", "price_cny_mwh": 400.0, "spread_cny_mwh": None, "share_ratio": 0.8,
         "channel_fee_cny_mwh": None, "channel_fee_lump_cny": None, "annual_mwh": 1200.0, "monthly_forecast": "{}"},
        # spread contract 4/MWh, channel 80%
        {"contract_type": "indexed", "price_cny_mwh": None, "spread_cny_mwh": 4.0, "share_ratio": 0.8,
         "channel_fee_cny_mwh": None, "channel_fee_lump_cny": None, "annual_mwh": 1200.0, "monthly_forecast": "{}"},
        # per-MWh 居间费 22 + annual lump 12,000
        {"contract_type": "indexed_band", "price_cny_mwh": 372.0, "spread_cny_mwh": None, "share_ratio": None,
         "channel_fee_cny_mwh": 22.0, "channel_fee_lump_cny": 12000.0, "annual_mwh": 1200.0, "monthly_forecast": "{}"},
    ])
    fee = channel_fee(c, month=3, benchmark=350.0)
    assert fee == pytest.approx(100 * 0.8 * 50 + 100 * 0.8 * 4 + (100 * 22 + 1000))
    # losses are not shared with the channel
    c2 = c.iloc[[0]].assign(price_cny_mwh=340.0)
    assert channel_fee(c2, month=3, benchmark=350.0) == 0.0
