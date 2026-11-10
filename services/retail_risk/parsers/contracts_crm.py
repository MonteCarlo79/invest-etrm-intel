# services/retail_risk/parsers/contracts_crm.py
"""Province 零售签约 sheets (台账/mtm/ 测算 workbooks) -> CONTRACTS_COLS.

Conventions shared by every province:
  share_ratio          CHANNEL's share of the retail margin (冀南 '80%*上浮价格',
                       浙江 执行调整后 > 签约 渠道分成比例, 福建 渠道分成比例, 上海 分成部分政策)
  price_cny_mwh        full retail price for fixed-price packages (元/度 x 1000)
  spread_cny_mwh       上浮 / 服务费 for 价差-type packages (price_cny_mwh NULL)
  channel_fee_cny_mwh  per-MWh channel fee (广东 固定部分度电居间费)
  channel_fee_lump_cny annual lump channel fee (冀南 '1.5万')
  annual_mwh           MWh (万度 x 10)
Contracts that ended before 2026 are dropped; missing dates default to 2026.
"""
from __future__ import annotations

import datetime
import json
import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_Y0, _Y1 = datetime.date(2026, 1, 1), datetime.date(2026, 12, 31)

# (filename glob under 台账/mtm, sheet, header row)
SOURCES = {
    "冀南": ("6 【冀南测算】*【分月分时】.xlsx", "冀南零售签约原表", 0),
    "浙江": ("7 【浙江测算】*【分月分时】.xlsx", "浙江零售签约原表", 1),
    "广东": ("1 【广东测算】*【分月-不分月分时】.xlsx", "零售签约原表", 0),
    "福建": ("3 【福建测算】*【分月不分时】.xlsx", "福建零售签约原表", 0),
    "上海": ("5 【上海测算】*【分月不分时】.xlsx", "上海零售总表", 1),
    "江苏": ("4 【江苏测算-更新】*汇总.xlsx", "利润测算", 1),
}


def contract_files(root: str | Path) -> dict[str, Path]:
    out = {}
    for prov, (g, _, _) in SOURCES.items():
        hits = [p for p in sorted((Path(root) / "台账" / "mtm").glob(g))
                if not re.search(r"[+-]10", p.name)]
        if hits:
            out[prov] = hits[0]
    return out


def _f(x) -> float | None:
    try:
        v = float(str(x).replace(",", "").strip())
        return None if pd.isna(v) else v
    except (ValueError, TypeError):
        return None


def _s(x) -> str | None:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    t = str(x).strip()
    return None if t in ("", "/", "nan", "-") else t


def _d(x, default: datetime.date) -> datetime.date:
    if x is None or (isinstance(x, float) and pd.isna(x)) or _s(x) is None:
        return default
    v = _f(x)
    if v is not None and 190001 <= v <= 210012:              # yyyymm (广东)
        y, m = divmod(int(v), 100)
        if default == _Y1:                                   # end-of-month for end dates
            nxt = datetime.date(y + (m == 12), m % 12 + 1, 1)
            return nxt - datetime.timedelta(days=1)
        return datetime.date(y, m, 1)
    try:
        return pd.to_datetime(x).date()
    except (ValueError, TypeError):
        return default


def _status(x) -> str:
    t = _s(x) or ""
    if any(k in t for k in ("解约", "终止")):
        return "terminated"
    if any(k in t for k in ("失效", "过期", "到期")):
        return "expired"
    if any(k in t for k in ("待", "未生效", "审核", "确认", "上传")):
        return "pending"
    return "active"


def _row(name, prov, start, end, **kw) -> dict:
    r = {c: None for c in schemas.CONTRACTS_COLS}
    r.update(customer_name=name, start_date=start, end_date=end, monthly_mwh="{}",
             contract_ref=f"{prov}-{name}-{start.isoformat()}", status="active")
    r.update(kw)
    return r


def _jinan(df: pd.DataFrame) -> list[dict]:
    out = []
    for r in df.to_dict("records"):
        name = _s(r.get("零售用户名称"))
        if not name:
            continue
        fee_txt = _s(r.get("渠道费")) or ""
        m_share = re.match(r"(\d+(?:\.\d+)?)%\s*\*\s*上浮", fee_txt)
        m_lump = re.fullmatch(r"(\d+(?:\.\d+)?)万", fee_txt)
        ptype = _s(r.get("用户电价类型")) or ""
        out.append(_row(
            name, "冀南", _d(r.get("绑定开始"), _Y0), _d(r.get("绑定结束"), _Y1),
            package_name=ptype, package_class=ptype,
            contract_type="indexed" if "价差" in ptype else "indexed_band",
            spread_cny_mwh=_f(r.get("上浮（元/MWh）")),
            share_ratio=float(m_share.group(1)) / 100 if m_share else None,
            channel_fee_lump_cny=float(m_lump.group(1)) * 10_000 if m_lump else None,
            annual_mwh=_f(r.get("年度预估电量(MWh）")), status=_status(r.get("合同状态")),
            bd_name=_s(r.get("BD")), customer_source=_s(r.get("来源"))))
    return out


def _zhejiang(df: pd.DataFrame) -> list[dict]:
    out = []
    fixed_col = next((c for c in df.columns if str(c).strip() == "固定价格套餐"), None)
    for r in df.to_dict("records"):
        name = _s(r.get("零售用户名称"))
        if not name:
            continue
        spread, fixed = _f(r.get("上浮金额")), _f(r.get(fixed_col)) if fixed_col else None
        share = _f(r.get("执行调整后渠道分成比例"))
        share = share if share is not None else _f(r.get("签约渠道分成比例"))
        vol = _f(r.get("年度预估电量(万度）"))
        out.append(_row(
            name, "浙江", _Y0, _Y1,
            package_name=_s(r.get("联动上浮套餐")) or ("固定价格" if fixed else None),
            package_class=_s(r.get("用户电价类型")),
            contract_type="fixed" if fixed is not None and spread is None else "indexed",
            price_cny_mwh=fixed * 1000 if fixed is not None and spread is None else None,
            spread_cny_mwh=spread * 1000 if spread is not None else None,
            share_ratio=share, annual_mwh=vol * 10 if vol is not None else None,
            bd_name=_s(r.get("BD"))))
    return out


def _guangdong(df: pd.DataFrame) -> list[dict]:
    df = df.rename(columns=lambda c: re.sub(r"\s+", "", str(c)))
    out = []
    for r in df.to_dict("records"):
        name, mode = _s(r.get("企业名称")), _s(r.get("合同模式")) or ""
        if not name or "固定" not in mode:
            continue                                   # summary block rows below the list
        k1, k2 = _f(r.get("固定部分占比")), _f(r.get("联动部分占比"))
        if k2 is None and k1 is not None:
            k2 = round(1 - k1, 6)
        vol = _f(r.get("预估年度交易电量"))
        ctype = "fixed" if k1 == 1 else "indexed" if k1 == 0 else "indexed_band"
        out.append(_row(
            name, "广东", _d(r.get("代理起始月份"), _Y0), _d(r.get("合同到期日"), _Y1),
            package_name=mode, package_class=_s(r.get("购电种类")), contract_type=ctype,
            price_cny_mwh=_f(r.get("固定+上浮（合同价）")),
            channel_fee_cny_mwh=_f(r.get("固定部分度电居间费（元/兆瓦时）")),
            annual_mwh=vol * 10 if vol is not None else None, k1=k1, k2=k2,
            customer_source=_s(r.get("客户来源")) or _s(r.get("销售类型")),
            channel_name=_s(r.get("具体名称"))))
    return out


def _fujian(df: pd.DataFrame) -> list[dict]:
    out = []
    for r in df.to_dict("records"):
        name = _s(r.get("零售用户名称"))
        if not name:
            continue
        ptype = _s(r.get("套餐类型")) or ""
        fixed = "固定" in ptype
        out.append(_row(
            name, "福建", _d(r.get("绑定开始"), _Y0), _d(r.get("绑定结束"), _Y1),
            package_name=ptype, package_class=ptype,
            contract_type="fixed" if fixed else "indexed",
            price_cny_mwh=_f(r.get("固定价")) if fixed else None,
            spread_cny_mwh=None if fixed else _f(r.get("度电服务费（元/兆瓦时）")),
            share_ratio=_f(r.get("渠道分成比例")), annual_mwh=_f(r.get("年度预估电量(MWh）")),
            status=_status(r.get("合同状态")), bd_name=_s(r.get("BD")),
            customer_source=_s(r.get("来源")), channel_name=_s(r.get("渠道名"))))
    return out


def _shanghai(df: pd.DataFrame) -> list[dict]:
    out = []
    for r in df.to_dict("records"):
        name = _s(r.get("客户名称"))
        if not name:
            continue
        mode = _s(r.get("合同签订模式")) or ""
        out.append(_row(
            name, "上海", _d(r.get("绑定日期"), _Y0), _Y1,
            package_name=_s(r.get("渠道分成模式")), package_class=mode,
            contract_type="fixed" if "固定" in mode else "indexed",
            price_cny_mwh=_f(r.get("场内价格")), share_ratio=_f(r.get("分成部分政策")),
            annual_mwh=_f(r.get("预估年度交易电量/MWH")), status=_status(r.get("状态")),
            channel_name=_s(r.get("渠道名称"))))
    return out


def _jiangsu(df: pd.DataFrame) -> list[dict]:
    df = df.rename(columns=lambda c: re.sub(r"\s+", "", str(c)))
    out = []
    for r in df.to_dict("records"):
        name = _s(r.get("零售用户名称"))
        if not name:
            continue
        ptype = _s(r.get("套餐类型")) or ""
        vol = (_f(r.get("预估年度火电电量/万度")) or 0.0) + (_f(r.get("预估年度绿电电量/万度")) or 0.0)
        avg = _f(r.get("零售均价预估（元/度）"))
        ctype = ("fixed" if "固定" in ptype else "indexed" if "浮动" in ptype else "indexed_band")
        pct = lambda c: (_f(r.get(c)) or 0.0) / 100.0
        out.append(_row(
            name, "江苏", _d(r.get("合同开始日期"), _Y0), _d(r.get("合同结束日期"), _Y1),
            package_name=ptype, package_class=ptype, contract_type=ctype,
            price_cny_mwh=avg * 1000 if avg else None,       # expected retail avg (预估)
            annual_mwh=vol * 10, k1=pct("长协比例（%）"), k2=pct("月竞比例（%）"),
            k3=pct("代购比例（%）"), customer_source=_s(r.get("属性")),
            channel_name=_s(r.get("对应渠道"))))
    return out


_PARSERS = {"冀南": _jinan, "浙江": _zhejiang, "广东": _guangdong, "福建": _fujian,
            "上海": _shanghai, "江苏": _jiangsu}


def parse_contracts(path: str | Path, province: str) -> pd.DataFrame:
    _, sheet, header = SOURCES[province]
    df = pd.read_excel(path, sheet_name=sheet, header=header)
    rows = [r for r in _PARSERS[province](df) if r["end_date"] >= _Y0]
    out = pd.DataFrame(rows, columns=schemas.CONTRACTS_COLS)
    return out.drop_duplicates("contract_ref", keep="last").reset_index(drop=True)


def price_formula(r) -> str:
    """JSON for rm_customer_contracts.price_formula (no dedicated columns)."""
    keys = ("spread_cny_mwh", "channel_fee_cny_mwh", "channel_fee_lump_cny")
    d = {k: getattr(r, k) for k in keys
         if getattr(r, k, None) is not None and not pd.isna(getattr(r, k))}
    return json.dumps(d)
