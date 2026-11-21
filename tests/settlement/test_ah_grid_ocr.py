"""国网安徽 下网 电费明细账 / 电费计算明细 — OCR text → deterministic items (定远 2024 image bills)."""
from pathlib import Path

import pytest

from services.settlement_ingest.parser_ah_grid_ocr import is_ah_grid_charge, parse_ah_grid_charge_text

# OCR lines (repo ocr_text output) of 定远 2024-01 电费明细账, fee block only
JAN = """电费明细账 导出人员：王莉
户号： 3400109729283 户名： 定远智贮能源科技有限公司 电费年月：202401
有功电量：6 6765704 无功电量：6 6833208 力调标准：0.9 功率因数：0.7 参与力调：2951932.63 调整系数：10
阶梯累计电量： 档期： 二档递增电费：0*0.05=0.00 三档递增电费： 0*0.3=0.00 调整电费：295193.27
其中代收政府性 可再生能源：13.38 重大水利基金：2.54 地方库区移民：0.00
基金及附加费： 农维费：0.00 大型库区移民：4.39 差别电价：0.00 代征合计：20.31
代理购电费：0.00 市场化交易电费：2951826.40 输配电费：106.23 上网环节线损费用： ：0.00
结算电费合计： 3247152.55 其中季节性尖峰电费：88*0.072=6.34
结算周期： 20240101-20240201 抄表员：吴兆祥"""

# 2024-03 电费计算明细: OCR noise "市场化交易电费：2 2822166.83", credit 力调, block 合计 rows
MAR = """电费计算明细 导出人员：王莉
电费年月：202403 地址：安徽省滁州市定远县
电费合计：2809587.45 =电度电费+力调电费 有功电量：6468696 无功电量： 2477216
代理购电费： ：0.00 市场化交易电费：2 2822166.83 输配电费：100.74 上网环节线损费用：0.00
工商业 系统运行费用： 交叉补贴新增损益： 0.00 力调电费损益：0.00 抽水蓄能容量电费：0.00
中 合计：0.00 历史偏差电费折价：0.00 绿色发展电价损益：0.00 煤电容量电费： 0.00
代收政府性基金及附加费： 可再生能源：13.23 重大水利基金：2.52 地方库区移民：
合计：20.09 农维费：0.00 大型库区移民：4.34 差别电价：x0.00
力调电费：-12700.21 参与力调金额：2822267.57 力调标准：0.9 功率因数93 调整系数：-0.45
结算周期：20240301-20240401"""

# 2024-05: adds 现货市场平衡/其他费用 block (合计 14125.04)
MAY = """电费计算明细
电费年月：202405
电费合计：1936539.93 =电度电费+力调电费+其他电费 有功电量：| 4411688
代理购电费：0 0.00 市场化交易电费： 1922134.22 输配电费：100.17 网环管线损费用：12.46
其 工商业 系统运行费用： 上网环节线损代理采购损益： -0.07 天然气发电容量电费：2.34
中 合计：38.61 历史偏差电费折价：-2.13 煤电容量电费：16.65
代收政府性基金及附加费： 可再生能源：13.07 重大水利基金：2.50
合计：19.86 农维费：0.00 大型库区移民：4.29
力调电费：109.57 参与力调金额：405.8 力调标准：0.9
现货市 机组限高考核：-71.90 市场平衡费用：14530.06
其他费用及 启动成本补偿：855.71 机组实时发电计划执行偏差收益回收：-1.39
运营费 必开机组补偿：0.00 新能源超额获利回收：-285.28 合计：14125.04
结算周期：20240501-20240601"""

# 2025-09: 分次结算 第1期 + 终期 (终期 re-prices the month → negative energy)
SEP = """电费计算明细
电费年月：202509 电压等级：交流220kV
电费合计：1129978.23 =电度电费 有功电量：2739328 无功电量： 78248
代理购电费：0.00 市场化交易电费：1129896.94 输配电费：49.02 上网环节线损费用：0.00
工商业 系统运行费用： 交叉补贴新增损益： :16. 17 力调电费损益：0.49
中 合计：22.79 绿色发展电价损益：0.00 煤电容量电费：6.26
代收政府性基金及附加费： 可再生能源：5.6
合计：9.48 大型库区移民：2.05 差别电价：0.00
结算周期：20250901-20250915 分次结算第1期 核算包编号：H344130106000011
电费年月：202509 电压等级：交流220kV
电费合计：-303845.96 =电度电费+力调电费 有功电量：2728368 无功电量： 1233368
电费年月：202509 电压等级：交流220kV
代理购电费：0.00 市场化交易电费： -303940.88 输配电费：55.96 上网环节线损费用：4.38
工商业 系统运行费用： 交叉补贴新增损益：18.15 力调电费损益：0.56
合计：25.59 绿色发展电价损益：0.00 煤电容量电费：7.03
代收政府性基金及附加费： 可再生能源：6.2
合计：10.61 大型库区移民：2.29 差别电价：0.00
力调电费：-1.62 参与力调金额：216.02 力调标准：0.9
结算周期：20250915-20251001 分次结算终期"""


def _by_note(items):
    return {i["notes"]: (i["category"], i["amount_cny"], i["volume_mwh"]) for i in items}


def test_detects_both_sheet_titles():
    assert is_ah_grid_charge(JAN) and is_ah_grid_charge(MAR)
    assert not is_ah_grid_charge("国网甘肃 本期电量 1551995千瓦时 本期电费 179574.29元")


def test_jan_mingxizhang_ties_with_adjustment_and_peak():
    got = _by_note(parse_ah_grid_charge_text(JAN))
    assert got["充电结算: 市场化交易电费 [全月]"] == ("charge_energy", -2951826.40, 6765.704)
    assert got["充电结算: 力调电费 [全月]"] == ("basic_fee", -295193.27, None)
    assert got["充电结算: 尖峰加收 [全月]"][1] == -6.34
    assert got["充电结算: 政府性基金及附加 [全月]"][1] == -20.31
    assert round(sum(i["amount_cny"] for i in parse_ah_grid_charge_text(JAN)), 2) == -3247152.55


def test_mar_ocr_noise_and_power_factor_credit():
    items = parse_ah_grid_charge_text(MAR)
    got = _by_note(items)
    assert got["充电结算: 市场化交易电费 [全月]"] == ("charge_energy", -2822166.83, 6468.696)
    assert got["充电结算: 力调电费 [全月]"][1] == 12700.21          # credit → positive
    assert round(sum(i["amount_cny"] for i in items), 2) == -2809587.45


def test_may_other_fees_block():
    items = parse_ah_grid_charge_text(MAY)
    got = _by_note(items)
    assert got["充电结算: 其他费用及运营费用 [全月]"] == ("market_redistribution", -14125.04, None)
    assert got["充电结算: 系统运行费用 [全月]"][1] == -38.61
    assert got["充电结算: 上网环节线损费用 [全月]"][1] == -12.46
    assert round(sum(i["amount_cny"] for i in items), 2) == -1936539.93


def test_sep_split_settlement_sums_both_periods_and_merges_spilled_sheet():
    items = parse_ah_grid_charge_text(SEP)
    energy = [i for i in items if i["category"] == "charge_energy"]
    assert sorted(i["notes"] for i in energy) == ["充电结算: 市场化交易电费 [第1期]", "充电结算: 市场化交易电费 [终期]"]
    assert round(sum(i["volume_mwh"] for i in energy), 3) == 5467.696
    assert round(sum(i["amount_cny"] for i in items), 2) == round(-(1129978.23 - 303845.96), 2)


def test_misread_total_refuses():
    assert parse_ah_grid_charge_text(MAR.replace("2809587.45", "2809587.95")) == []


ROOT = Path("/Users/chenzhuqi/Library/CloudStorage/OneDrive-Personal/ETRM/bess-platform/data/raw/"
            "settlement/invoices/B-3 安徽定远/2024年结算单")


def test_image_bill_routes_to_ocr_parser_end_to_end(tmp_path):
    pytest.importorskip("rapidocr_onnxruntime")
    pytest.importorskip("pymupdf")
    src = ROOT / "2024年5月【B-3-下】定远下网电费.jpg"
    if not src.exists():
        pytest.skip("image bill not present")
    from libs.asset_risk.settlement.ocr import image_to_pdf
    from libs.asset_risk.settlement.router import read_bill, route
    from services.settlement_ingest.scanner import classify_pdf
    pdf = tmp_path / "b.pdf"
    pdf.write_bytes(image_to_pdf(src.read_bytes(), "jpg"))
    bill = read_bill(str(pdf), src.name)
    assert bill.is_scanned
    r = route(bill, classify_pdf(src.name), "scanner")
    assert r is not None and "国网安徽" in r.label, r and r.label
    assert round(sum(i["amount_cny"] for i in r.parse(bill)), 2) == -1936539.93
