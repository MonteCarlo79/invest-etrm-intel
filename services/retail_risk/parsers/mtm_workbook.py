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


def _hour_col_map(columns) -> dict:
    """Map hour columns -> int hour (0-23). Accepts 0..23, '0'..'23', 0.0..23.0,
    'h00'..'h23'. Skips everything else ('Unnamed: N', notes, duplicate '0.1')."""
    out = {}
    for c in columns:
        s = str(c).strip()
        h = None
        m = re.fullmatch(r"h(\d{2})", s)
        if m:
            h = int(m.group(1))
        else:
            try:
                f = float(s)
                if f == int(f):
                    h = int(f)
            except ValueError:
                pass
        if h is not None and 0 <= h <= 23:
            out[c] = h
    return out


def _month_hour_grid(df: pd.DataFrame) -> pd.DataFrame:
    """[month label col0, hour cols 1..24] -> long frame (month, hour, value).
    Month labels: pure 'N月' or pure int 1-12. Junk labels/values/columns dropped."""
    df = df.rename(columns={df.columns[0]: "月份"})
    labels = df["月份"].astype(str).str.strip()
    month_num = labels.str.extract(r"^(\d{1,2})月$", expand=False)
    int_labels = labels.str.fullmatch(r"\d{1,2}(\.0)?")
    month_num = month_num.fillna(labels[int_labels].apply(lambda s: str(int(float(s)))))
    df["month"] = pd.to_numeric(month_num, errors="coerce")
    df = df.dropna(subset=["month"])
    df = df[df["month"].between(1, 12)]
    df["month"] = df["month"].astype(int)
    col_map = _hour_col_map([c for c in df.columns if c not in ("月份", "month")])
    long = df.melt(id_vars=["month"], value_vars=list(col_map), var_name="hour", value_name="value")
    long["hour"] = long["hour"].map(col_map)
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    return long.dropna(subset=["value", "hour"])


def _expand_month_hour(province: str, product: str, month: int, hour: int,
                       price_mwh: float, curve_date) -> list:
    ndays = calendar.monthrange(2026, month)[1]
    return [[province, product, f"2026-{month:02d}-{day:02d}", hour,
             price_mwh / 1000.0, curve_date] for day in range(1, ndays + 1)]


def _curves_from_grid(xl: pd.ExcelFile, sheet: str, province: str, product: str,
                      curve_date) -> list:
    """Standard month(N月) x hour grid （分时 workbooks: 山东/安徽/冀南/浙江/上海)."""
    grid = _month_hour_grid(xl.parse(sheet))
    rows = []
    for r in grid.itertuples(index=False):
        rows.extend(_expand_month_hour(province, product, r.month, int(r.hour),
                                       float(r.value), curve_date))
    return rows


def _curves_from_transposed(xl: pd.ExcelFile, sheet: str, province: str, product: str,
                            curve_date) -> list:
    """广东/福建 layout: price series in ROWS x month numbers in COLUMNS.
    Uses the 实时 spot series when present, else 综合价/日前. Monthly flat -> 24h."""
    df = xl.parse(sheet, header=None)
    series_row = None
    for i in range(len(df)):
        label = str(df.iloc[i, 0])
        if "实时" in label and "现货" in label:
            series_row = i
            break
        if series_row is None and ("综合价" in label or ("现货" in label and "日前" in label)):
            series_row = i
    if series_row is None:
        return []
    rows = []
    for j in range(1, df.shape[1]):
        try:
            month = int(float(df.iloc[0, j]))
            price = float(df.iloc[series_row, j])
        except (ValueError, TypeError):
            continue
        if not (1 <= month <= 12) or pd.isna(price):
            continue
        for h in range(24):
            rows.extend(_expand_month_hour(province, product, month, h, price, curve_date))
    return rows


def _curves_from_flat_param(xl: pd.ExcelFile, province: str, product: str,
                            curve_date) -> list:
    """江苏 layout: 参数 sheet holds a scalar 预估现货均价 (CNY/kWh) -> flat curve."""
    sheet = _find_sheet(xl, "参数")
    if sheet is None:
        return []
    df = xl.parse(sheet, header=None)
    price_kwh = None
    for i in range(len(df)):
        if "预估现货均价" in str(df.iloc[i, 0]):
            try:
                price_kwh = float(df.iloc[i, 1])
            except (ValueError, TypeError):
                pass
            break
    if price_kwh is None:
        return []
    rows = []
    for month in range(1, 13):
        for h in range(24):
            rows.extend(_expand_month_hour(province, product, month, h,
                                           price_kwh * 1000.0, curve_date))
    return rows


def _is_transposed_layout(xl: pd.ExcelFile, sheet: str) -> bool:
    """Transposed sheets put month NUMBERS 1-12 in the header row and series names
    in col 0. Hour grids (headers 0..23) are NOT transposed: they contain 0 and
    values > 12."""
    df = xl.parse(sheet, header=None, nrows=2)
    if df.empty:
        return False
    first_col_label = str(df.iloc[0, 0])
    label_ok = not any(k in first_col_label for k in ("月", "价格", "假设"))
    nums = pd.to_numeric(df.iloc[0, 1:], errors="coerce").dropna()
    return (label_ok and len(nums) >= 6
            and nums.between(1, 12).all() and nums.min() >= 1)


def parse_mtm_workbook(path: str | Path) -> dict:
    path = Path(path)
    province = province_for_filename(path.name)
    product = scenario_for_filename(path.name)
    xl = pd.ExcelFile(path)
    curve_date = pd.Timestamp(path.stat().st_mtime, unit="s").date()

    curve_rows: list = []
    price_sheet = _find_sheet(xl, "模型价格预测") or _find_sheet(xl, "价格预测")
    if price_sheet:
        if _is_transposed_layout(xl, price_sheet):
            curve_rows = _curves_from_transposed(xl, price_sheet, province, product, curve_date)
        else:
            curve_rows = _curves_from_grid(xl, price_sheet, province, product, curve_date)
    else:
        curve_rows = _curves_from_flat_param(xl, province, product, curve_date)
    curves = pd.DataFrame(curve_rows, columns=schemas.CURVES_COLS)

    if "不分时" in path.name and not curves.empty:
        # monthly-flat workbooks （上海 grid has only col-0 populated): broadcast each
        # (province, product, delivery_date)'s price across all 24 hours.
        daily = curves.drop_duplicates(subset=["province", "product", "delivery_date"])
        rows = []
        for r in daily.itertuples(index=False):
            for h in range(24):
                rows.append([r.province, r.product, r.delivery_date, h,
                             r.price_cny_kwh, r.curve_date])
        curves = pd.DataFrame(rows, columns=schemas.CURVES_COLS)

    contracts = pd.DataFrame(columns=schemas.CONTRACTS_COLS)
    c_sheet = (_find_sheet(xl, "合约", "总表") or _find_sheet(xl, "零售签约原表")
               or _find_sheet(xl, "零售总表"))
    if c_sheet and product == "spot_base":   # contracts identical across scenarios
        cdf = xl.parse(c_sheet)
        if not {"序号", "零售用户名称"}.issubset(cdf.columns):
            c_sheet = None                 # different contract schema (P2 scope) — skip
    if c_sheet and product == "spot_base":
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
