"""Settlement analytics: pure/DB-read helpers shared by the Settlement and P&L tabs."""
from __future__ import annotations

import calendar

import pandas as pd
from sqlalchemy import text


CATEGORY_CN = {
    "charge_energy": "充电电费",
    "discharge_energy": "放电收入",
    "capacity_compensation": "容量补偿/非市场化",
    "transmission": "输配电费",
    "system_operation": "上网线损费",
    "coal_capacity_charge": "系统运行费",
    "basic_fee": "基本电费/力调",
    "govt_surcharges": "政府基金及附加",
    "frequency": "调频",
    "penalty": "偏差费用",
    "subsidy": "补贴",
    "rebate": "返还",
    "generation_revenue": "发电收入",
    "other": "其他",
}

_REVENUE_CATS = ("discharge_energy", "generation_revenue")

ASSET_CLASS_OPTIONS = ["wind", "solar", "bess", "thermal", "load",
                       "wind+bess", "wind+bess+load", "solar+bess+load"]


def book_matches_classes(asset_type: str | None, book_type: str | None, sel_classes: list[str]) -> bool:
    """True if a book matches any of the selected asset classes.

    Single classes match rm_assets.asset_type; 'load' matches rm_books.book_type;
    composites are unions (e.g. 'wind+bess+load' = wind/bess assets or load books).
    """
    t = asset_type or ""
    bt = book_type or ""
    for c in sel_classes:
        if c == t:
            return True
        if c == "load" and bt == "load":
            return True
        if c == "wind+bess" and t in ("wind", "bess"):
            return True
        if c == "wind+bess+load" and (t in ("wind", "bess") or bt == "load"):
            return True
        if c == "solar+bess+load" and (t in ("solar", "bess") or bt == "load"):
            return True
    return False


def _portfolio_matrix(items_df: pd.DataFrame) -> pd.DataFrame:
    """Month × asset net-profit matrix with 组合合计 column and 资产合计 row."""
    df = items_df.copy()
    df["month"] = pd.to_datetime(df["settlement_month"]).dt.strftime("%Y-%m")
    mat = df.pivot_table(index="month", columns="asset", values="amount_cny",
                         aggfunc="sum", fill_value=0)
    mat = mat.sort_index()
    mat["组合合计"] = mat.sum(axis=1)
    total_row = mat.sum(axis=0)
    total_row.name = "资产合计"
    return pd.concat([mat, total_row.to_frame().T])


def _days_in_months(month_series) -> int:
    """Total calendar days across the distinct months present in the series."""
    months = pd.to_datetime(month_series).dt.to_period("M").unique()
    return sum(calendar.monthrange(p.year, p.month)[1] for p in months)


def _load_settlement_items(engine, book_id: int, date_range=None) -> pd.DataFrame:
    """Category totals for one book, optionally restricted to a settlement_month range.

    date_range is the st.date_input return: empty tuple while unselected, or
    (start, end) once both dates are picked. Only a complete pair filters
    (reversed picks are normalized); anything else returns all months.
    """
    clause = ""
    params = {"bid": book_id}
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        start, end = sorted(str(d) for d in date_range)
        clause = " AND s.settlement_month >= :d_start AND s.settlement_month <= :d_end"
        params["d_start"], params["d_end"] = start, end
    with engine.connect() as conn:
        return pd.read_sql(text(f"""
            SELECT si.category, SUM(si.amount_cny) as total
            FROM marketdata.rm_settlement_items si
            JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
            WHERE s.book_id = :bid{clause}
            GROUP BY si.category
            ORDER BY total DESC
        """), conn, params=params)


def _asset_summary(items_df: pd.DataFrame) -> pd.DataFrame:
    """Per-asset realised P&L summary.

    Returns DataFrame with columns: asset, net_profit, discharge_mwh,
    charge_mwh, arb_income, arb_spread (arb_income / discharge_mwh; None when
    no discharge volume), cycles_per_day (charge_mwh / energy_per_cycle / days;
    None when capacity data or days are unavailable).
    """
    rows = []
    for asset, g in items_df.groupby("asset"):
        dis_vol = float(g.loc[g["category"].isin(_REVENUE_CATS), "volume_mwh"].sum())
        chg_vol = float(g.loc[g["category"] == "charge_energy", "volume_mwh"].sum())
        rev = float(g.loc[g["category"].isin(_REVENUE_CATS), "amount_cny"].sum())
        cost = float(g.loc[g["category"] == "charge_energy", "amount_cny"].sum())
        arb_income = rev + cost

        # Daily average cycles over the months present (needs capacity x duration)
        energy = None
        if "capacity_mw" in g.columns:
            cap = g["capacity_mw"].iloc[0]
            dur = g["bess_duration_h"].iloc[0] if "bess_duration_h" in g.columns else None
            if pd.notna(cap) and cap:
                dur_val = float(dur) if (dur is not None and pd.notna(dur) and dur) else 4.0
                energy = float(cap) * dur_val
        days = _days_in_months(g["settlement_month"])
        cycles = (chg_vol / energy / days) if (energy and days) else None

        rows.append({
            "asset": asset,
            "net_profit": float(g["amount_cny"].sum()),
            "discharge_mwh": dis_vol,
            "charge_mwh": chg_vol,
            "arb_income": arb_income,
            "arb_spread": arb_income / dis_vol if dis_vol else None,
            "cycles_per_day": cycles,
        })
    return pd.DataFrame(rows)


def _asset_month_metric(items_df: pd.DataFrame, metric: str) -> pd.DataFrame:
    """Month × asset matrix for a per-unit metric, with per-year YTD rows.

    metric: "容量补偿价差" (capcomp ¥/MWh), "套利价差" (arb ¥/MWh),
            "日均充放次数" (cycles/day via capacity × duration and calendar days),
            "转化率" (round-trip efficiency = discharge / charge).

    YTD rows are volume-weighted aggregates (sum of numerators ÷ sum of
    denominators over the year's months), NOT averages of monthly ratios.
    """
    df = items_df.copy()
    df["month"] = pd.to_datetime(df["settlement_month"]).dt.strftime("%Y-%m")
    rows = []
    ytd = {}  # (asset, year) -> accumulators

    for (asset, month), g in df.groupby(["asset", "month"]):
        dis_vol = float(g.loc[g["category"].isin(_REVENUE_CATS), "volume_mwh"].sum())
        chg_vol = float(g.loc[g["category"] == "charge_energy", "volume_mwh"].sum())
        cap_amt = float(g.loc[g["category"] == "capacity_compensation", "amount_cny"].sum())
        rev = float(g.loc[g["category"].isin(_REVENUE_CATS), "amount_cny"].sum())
        cost = float(g.loc[g["category"] == "charge_energy", "amount_cny"].sum())

        cap = g["capacity_mw"].iloc[0] if "capacity_mw" in g.columns else None
        dur = g["bess_duration_h"].iloc[0] if "bess_duration_h" in g.columns else None
        atype = g["asset_type"].iloc[0] if "asset_type" in g.columns else None
        energy = None
        if pd.notna(cap) and cap:
            energy = float(cap) * (float(dur) if (dur is not None and pd.notna(dur) and dur) else 4.0)
        days = calendar.monthrange(int(month[:4]), int(month[5:7]))[1]

        # Monthly value
        if metric == "容量补偿价差":
            val = cap_amt / dis_vol if dis_vol else None
        elif metric == "套利价差":
            val = (rev + cost) / dis_vol if dis_vol else None
        elif metric == "转化率":
            # Wind: 发电小时数 = generation / installed capacity (RTE is meaningless)
            val = (dis_vol / float(cap) if (atype == "wind" and pd.notna(cap) and cap)
                   else (dis_vol / chg_vol if chg_vol else None))
        else:  # 日均充放次数
            val = chg_vol / energy / days if (energy and days) else None
        rows.append({"asset": asset, "month": month, "value": val})

        # Accumulate for YTD
        acc = ytd.setdefault((asset, month[:4]), {
            "dis_vol": 0.0, "chg_vol": 0.0, "cap_amt": 0.0,
            "rev": 0.0, "cost": 0.0, "days": 0, "energy": energy,
            "atype": atype, "cap": float(cap) if (pd.notna(cap) and cap) else None,
        })
        acc["dis_vol"] += dis_vol
        acc["chg_vol"] += chg_vol
        acc["cap_amt"] += cap_amt
        acc["rev"] += rev
        acc["cost"] += cost
        acc["days"] += days

    mat = pd.DataFrame(rows).pivot_table(index="month", columns="asset",
                                         values="value", aggfunc="first")
    if mat.empty:
        return mat

    # YTD rows: sum of numerators ÷ sum of denominators
    ytd_rows = {}
    for (asset, year), acc in ytd.items():
        if metric == "容量补偿价差":
            val = acc["cap_amt"] / acc["dis_vol"] if acc["dis_vol"] else None
        elif metric == "套利价差":
            val = (acc["rev"] + acc["cost"]) / acc["dis_vol"] if acc["dis_vol"] else None
        elif metric == "转化率":
            if acc.get("atype") == "wind":
                val = acc["dis_vol"] / acc["cap"] if acc.get("cap") else None
            else:
                val = acc["dis_vol"] / acc["chg_vol"] if acc["chg_vol"] else None
        else:  # 日均充放次数
            val = (acc["chg_vol"] / acc["energy"] / acc["days"]
                   if (acc["energy"] and acc["days"]) else None)
        if val is not None:
            ytd_rows.setdefault(f"{year} YTD", {})[asset] = val

    # Assemble: each year's months followed by its YTD row
    years = sorted({m[:4] for m in mat.index})
    parts = []
    for year in years:
        months = [m for m in mat.index if m.startswith(year)]
        parts.append(mat.loc[months])
        label = f"{year} YTD"
        if label in ytd_rows and ytd_rows[label]:
            parts.append(pd.DataFrame([ytd_rows[label]], index=[label]))
    return pd.concat(parts)


def _year_subtotal_row(pivot, monthly, year, days_in_month, energy_per_cycle_mwh,
                       discharge_col, charge_col, cap_col, is_wind=False, capacity_mw=None):
    """Build one year's YTD subtotal row with per-unit metrics recalculated
    from that year's own volumes. Returns a Series named "{year} YTD",
    or None if the year has no months in pivot."""
    months = [m for m in pivot.index if m.startswith(year)]
    if not months:
        return None
    row = pivot.loc[months].sum(axis=0)
    row.name = f"{year} YTD"

    yr_monthly = monthly[monthly["month"].str.startswith(year)]
    total_discharge_vol = yr_monthly[yr_monthly["category_cn"] == "放电收入"]["volume"].sum()
    total_charge_vol = yr_monthly[yr_monthly["category_cn"] == "充电电费"]["volume"].sum()
    yr_days = sum(d for m, d in zip(pivot.index, days_in_month) if m.startswith(year))
    if total_discharge_vol > 0 and "价差收入" in pivot.columns:
        # 价差收入 = 放电 + 充电 (不含容量补偿); 度电总价差 = (价差收入 + 容量补偿 + 调频) / 放电量
        cap_total = row[cap_col] if (cap_col and cap_col in pivot.columns) else 0
        freq_total = row["调频"] if "调频" in pivot.columns else 0
        row["度电总价差"] = (row["价差收入"] + cap_total + freq_total) / total_discharge_vol
        if cap_col and "容量补偿价差" in pivot.columns:
            row["容量补偿价差"] = cap_total / total_discharge_vol
        if "套利价差" in pivot.columns:
            row["套利价差"] = row["价差收入"] / total_discharge_vol
    if is_wind and capacity_mw and capacity_mw > 0:
        row["发电小时数"] = round(total_discharge_vol / capacity_mw, 1)
        row["LF"] = total_discharge_vol / (capacity_mw * yr_days * 24) if yr_days > 0 else 0
    if energy_per_cycle_mwh and energy_per_cycle_mwh > 0 and "日均充放次数" in pivot.columns:
        row["日均充放次数"] = round(total_charge_vol / energy_per_cycle_mwh / yr_days, 2) if yr_days > 0 else 0
    if total_charge_vol > 0 and not is_wind:
        row["转化率"] = total_discharge_vol / total_charge_vol
    return row


def _insert_year_subtotals(pivot, monthly, days_in_month, energy_per_cycle_mwh,
                           discharge_col, charge_col, cap_col, is_wind=False, capacity_mw=None):
    """Insert a "YYYY YTD" subtotal row after each year's last month row.
    Extends automatically: any year present in the data gets a subtotal."""
    years = sorted({m[:4] for m in pivot.index})
    parts = []
    for year in years:
        months = [m for m in pivot.index if m.startswith(year)]
        parts.append(pivot.loc[months])
        row = _year_subtotal_row(pivot, monthly, year, days_in_month, energy_per_cycle_mwh,
                                 discharge_col, charge_col, cap_col, is_wind, capacity_mw)
        if row is not None:
            parts.append(row.to_frame().T)
    return pd.concat(parts)


# Notes markers written by the exchange-settlement parsers (安徽统推 / 山东 SDTC).
# The 售电侧合计 row is the NET settlement (not gross discharge); 购电侧合计 is
# its charge component. Keeping them alongside the grid 电费账单/下网明细
# double-counts the same physical energy (定远 2026: 净利润 inflated by the
# 统推净额, e.g. Sep +586,750 on 3,006,518).
_EXCHANGE_SELL_NOTE = "放电结算: 交易中心售电侧合计"
_EXCHANGE_BUY_NOTE = "充电结算: 交易中心购电侧合计"


def prefer_canonical_settlements(items_df):
    """Drop exchange-settlement rows when a canonical twin exists for the month.

    Per month: canonical discharge rows exist (电费账单 etc.) → drop 售电侧合计;
    canonical charge rows exist (下网明细 etc.) → drop 购电侧合计. Months where
    the exchange settlement is the ONLY source keep their rows.
    Returns (kept_df, exchange_df) — exchange_df feeds the 口径校验 panel.
    """
    sell = items_df["notes"] == _EXCHANGE_SELL_NOTE
    buy = items_df["notes"] == _EXCHANGE_BUY_NOTE
    is_dis = items_df["category"].isin(["discharge_energy", "generation_revenue"])
    is_chg = items_df["category"] == "charge_energy"
    months_dis = set(items_df.loc[is_dis & ~sell, "month"])
    months_chg = set(items_df.loc[is_chg & ~buy, "month"])
    drop = (sell & items_df["month"].isin(months_dis)) | (
        buy & items_df["month"].isin(months_chg))
    return items_df.loc[~drop].copy(), items_df.loc[drop].copy()


# ── Merchant Exposure (市场化收入占比) ────────────────────────────────────────

_MERCHANT_CATS = ("discharge_energy", "generation_revenue")
_CONTRACT_CATS = ("capacity_compensation", "subsidy", "rebate", "other")
_SERVICE_CATS = ("frequency",)
_COST_CATS = ("charge_energy", "transmission", "system_operation",
              "coal_capacity_charge", "basic_fee", "govt_surcharges", "penalty")


def _bucket_of(category: str) -> str:
    """Map a settlement category to a merchant-exposure bucket."""
    if category in _MERCHANT_CATS:
        return "merchant"
    if category in _CONTRACT_CATS:
        return "contract"
    if category in _SERVICE_CATS:
        return "services"
    return "cost"


def compute_merchant_shares(items_df):
    """Per-month bucket sums + merchant share of gross inflows.

    items_df columns: category, month (YYYY-MM), amount_cny.
    merchant share = merchant / (merchant + contract + services) bucket sums,
    where bucket sums are netted within the bucket for the month and floored at 0
    for the inflow denominator. Returns (monthly_df, latest_month_df or None).
    """
    if items_df.empty:
        return pd.DataFrame(), None
    df = items_df.copy()
    df["bucket"] = df["category"].map(_bucket_of)
    monthly = (
        df[df["bucket"].isin(["merchant", "contract", "services"])]
        .groupby(["month", "bucket"], as_index=False)["amount_cny"].sum()
        .pivot(index="month", columns="bucket", values="amount_cny")
        .fillna(0.0)
    )
    for col in ("merchant", "contract", "services"):
        if col not in monthly.columns:
            monthly[col] = 0.0
    monthly["gross"] = (monthly["merchant"].clip(lower=0)
                        + monthly["contract"].clip(lower=0)
                        + monthly["services"].clip(lower=0))
    monthly["merchant_share"] = monthly.apply(
        lambda r: (r["merchant"] / r["gross"]) if r["gross"] > 1e-9 else None,
        axis=1,
    )
    monthly = monthly.reset_index().sort_values("month")
    latest = monthly.dropna(subset=["merchant_share"])
    return monthly, (latest.iloc[-1] if not latest.empty else None)
