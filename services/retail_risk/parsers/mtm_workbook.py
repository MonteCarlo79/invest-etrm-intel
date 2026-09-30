# services/retail_risk/parsers/mtm_workbook.py
"""MTM 测算 workbooks (台账/mtm/): forward curves, retail contracts, hourly ratios."""
from __future__ import annotations

import calendar
import json
import re
from pathlib import Path

import pandas as pd

from services.retail_risk import schemas

_PROVINCE_RE = re.compile(r"【(.+?)测算")
_SCENARIO_SUFFIX = {"-10": "spot_m10", "+10": "spot_p10"}


def scenario_for_filename(name: str) -> str:
    stem = name.rsplit(".", 1)[0]
    for suffix, product in _SCENARIO_SUFFIX.items():
        if stem.endswith(suffix + "】") or stem.endswith(suffix):
            return product
    return "spot_base"


def province_for_filename(name: str) -> str:
    m = _PROVINCE_RE.search(name)
    if not m:
        raise ValueError(f"Cannot parse province from {name}")
    return m.group(1)


def mtm_files(root: str | Path) -> list[Path]:
    d = Path(root) / "台账" / "mtm"
    return [p for p in sorted(d.glob("*.xlsx"))
            if "测算" in p.name and "信息汇总" not in p.name]


def _find_sheet(xl: pd.ExcelFile, *keywords: str) -> str | None:
    for s in xl.sheet_names:
        if all(k in s for k in keywords):
            return s
    return None


def _month_hour_grid(df: pd.DataFrame) -> pd.DataFrame:
    """[month label col0, hour cols 1..24] -> long frame (month, hour, value)."""
    df = df.rename(columns={df.columns[0]: "月份"})
    df = df[df["月份"].astype(str).str.contains("月", na=False)]
    df["month"] = df["月份"].astype(str).str.extract(r"(\d{1,2})").astype(int)
    hour_cols = [c for c in df.columns if c not in ("月份", "month")]
    long = df.melt(id_vars=["month"], value_vars=hour_cols, var_name="hour", value_name="value")
    long["hour"] = long["hour"].astype(int)
    return long.dropna(subset=["value"])


def parse_mtm_workbook(path: str | Path) -> dict:
    path = Path(path)
    province = province_for_filename(path.name)
    product = scenario_for_filename(path.name)
    xl = pd.ExcelFile(path)
    curve_date = pd.Timestamp(path.stat().st_mtime, unit="s").date()

    curves = pd.DataFrame(columns=schemas.CURVES_COLS)
    price_sheet = _find_sheet(xl, "模型价格预测") or _find_sheet(xl, "价格预测")
    if price_sheet:
        grid = _month_hour_grid(xl.parse(price_sheet))
        rows = []
        for r in grid.itertuples(index=False):
            ndays = calendar.monthrange(2026, r.month)[1]
            for day in range(1, ndays + 1):
                rows.append([province, product, f"2026-{r.month:02d}-{day:02d}",
                             int(r.hour), float(r.value) / 1000.0, curve_date])
        curves = pd.DataFrame(rows, columns=schemas.CURVES_COLS)

    contracts = pd.DataFrame(columns=schemas.CONTRACTS_COLS)
    c_sheet = _find_sheet(xl, "合约", "总表")
    if c_sheet and product == "spot_base":   # contracts identical across scenarios
        cdf = xl.parse(c_sheet)
        month_cols = [c for c in cdf.columns if re.fullmatch(r"\d{1,2}月(/\d{1,2}月)?电量", str(c))]
        annual_col = next((c for c in cdf.columns if "年度电量" in str(c)), None)
        rows = []
        for pos, r in enumerate(cdf.itertuples(index=False)):
            if pd.isna(getattr(r, "零售用户名称", None)):
                continue
            monthly = {}
            for c in month_cols:
                mnum = re.match(r"(\d{1,2})月", str(c)).group(1)
                v = cdf.iloc[pos][c]
                if pd.notna(v):
                    monthly[mnum] = float(v) * 10.0      # 万度 -> MWh
            annual_v = cdf.iloc[pos][annual_col] if annual_col else None
            rows.append([
                str(r.零售用户名称), str(r.序号), str(getattr(r, "套餐名称", "")),
                str(getattr(r, "套餐类别", "")),
                schemas.contract_type_for(str(getattr(r, "套餐类别", ""))),
                float(r.套餐价格) if pd.notna(getattr(r, "套餐价格", None)) else None,
                float(r.渠道分成比例) if pd.notna(getattr(r, "渠道分成比例", None)) else None,
                pd.to_datetime(r.生效时间).date(), pd.to_datetime(r.失效时间).date(),
                float(annual_v) * 10.0 if annual_v is not None and pd.notna(annual_v) else None,
                json.dumps(monthly),
            ])
        contracts = pd.DataFrame(rows, columns=schemas.CONTRACTS_COLS)

    return {"province": province, "product": product,
            "curves": curves, "contracts": contracts}


def load_hourly_ratios(path: str | Path) -> pd.DataFrame:
    """分月分时比例 sheet -> (month, hour, ratio), normalised to sum 1 per month."""
    path = Path(path)
    xl = pd.ExcelFile(path)
    sheet = _find_sheet(xl, "分月分时比例")
    if sheet is None:
        return pd.DataFrame(columns=["month", "hour", "ratio"])
    grid = _month_hour_grid(xl.parse(sheet))
    grid["ratio"] = grid["value"] / grid.groupby("month")["value"].transform("sum")
    return grid[["month", "hour", "ratio"]]
