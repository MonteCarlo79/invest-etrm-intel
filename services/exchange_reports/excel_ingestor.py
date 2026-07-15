"""
services/exchange_reports/excel_ingestor.py

Parsers for vendor-curated Excel databases of provincial power exchange monthly data.
Each parser returns a list of monthly dicts with standardized field names.

Units after normalization:
  - capacity: MW
  - volumes:  GWh (亿kWh)
  - prices:   yuan/MWh
  - costs:    million yuan (百万元 / 100万元)
"""
from __future__ import annotations

import logging
import re
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_col(df: pd.DataFrame, keywords: list[str], exclude: list[str] | None = None) -> Optional[str]:
    """Return first column whose name contains ALL keywords (and none of the excludes).
    Strips newlines and extra whitespace from column names before matching."""
    for col in df.columns:
        s = str(col).replace('\n', ' ').replace('\r', ' ').strip()
        if all(k in s for k in keywords):
            if exclude and any(e in s for e in exclude):
                continue
            return col
    return None


def _wan_kw_to_mw(val) -> Optional[float]:
    """万kW → MW  (1 万kW = 10 MW)."""
    try:
        v = float(val)
        return round(v * 10, 2) if pd.notna(v) else None
    except (TypeError, ValueError):
        return None


def _qian_kw_to_mw(val) -> Optional[float]:
    """千kW → MW  (1 千kW = 1 MW, since 千kW = 1000 kW = 1 MW)."""
    try:
        v = float(val)
        return round(v, 2) if pd.notna(v) else None
    except (TypeError, ValueError):
        return None


def _to_date(val) -> Optional[date]:
    """Parse YYYY-MM string, timestamp, or (year, month) pair → date."""
    if isinstance(val, (pd.Timestamp, date)):
        return val.date() if isinstance(val, pd.Timestamp) else val
    s = str(val).strip()
    # YYYY-MM
    m = re.match(r'^(\d{4})-(\d{1,2})$', s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 1)
    # YYYY年M月
    m = re.match(r'^(\d{4})年(\d{1,2})月?$', s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 1)
    # YYYY-MM-DD timestamp
    m = re.match(r'^(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 1)
    return None


def _safe(val) -> Optional[float]:
    try:
        v = float(val)
        return v if pd.notna(v) else None
    except (TypeError, ValueError):
        return None


def _safe_int(val) -> Optional[int]:
    try:
        v = float(val)
        return int(v) if pd.notna(v) else None
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Province parsers — each returns list[dict] with standardised fields
# ---------------------------------------------------------------------------

def _parse_mengxi(path: Path) -> list[dict]:
    """蒙西 — 蒙西信息披露月报数据汇总 + 蒙西结算数据."""
    rows: dict[date, dict] = {}

    # ── Capacity (sheet 0) ─────────────────────────────────────────────────
    xl = pd.ExcelFile(path)
    df_cap = xl.parse(0, header=1)
    for _, r in df_cap.iterrows():
        d = _to_date(r.iloc[0])
        if not d:
            continue
        rows.setdefault(d, {"province": "蒙西", "report_month": d, "source_file": path.name})
        rows[d].update({
            "total_capacity_mw":   _qian_kw_to_mw(r.iloc[1]),
            "thermal_capacity_mw": _qian_kw_to_mw(r.iloc[2]),
            "hydro_capacity_mw":   _qian_kw_to_mw(r.iloc[3]),
            "wind_capacity_mw":    _qian_kw_to_mw(r.iloc[4]),
            "solar_capacity_mw":   _qian_kw_to_mw(r.iloc[5]),
            "bess_capacity_mw":    _qian_kw_to_mw(r.iloc[6]),
        })

    # ── Spot market (sheet 5) ──────────────────────────────────────────────
    try:
        df_spot = xl.parse(5, header=1)
        for _, r in df_spot.iterrows():
            d = _to_date(r.iloc[0])
            if not d or d not in rows:
                continue
            rows[d].update({
                "spot_traded_gwh": _safe(r.iloc[1]),
                "spot_avg_price":  _safe(r.iloc[7]) or _safe(r.iloc[4]),
            })
    except Exception:
        pass

    # ── Retail/participants (sheet 9) ─────────────────────────────────────
    try:
        df_retail = xl.parse(9, header=1)
        for _, r in df_retail.iterrows():
            d = _to_date(r.iloc[0])
            if not d or d not in rows:
                continue
            rows[d].update({
                "retailers":                   _safe_int(r.iloc[1]),
                "retailer_traded_gwh":         _safe(r.iloc[4]),
                "retailer_settlement_price":   _safe(r.iloc[5]),
                "retailer_service_fee_million_yuan": _safe(r.iloc[3]) and round(_safe(r.iloc[3]) / 100, 4),
            })
    except Exception:
        pass

    # ── Settlement data (separate file if available) ───────────────────────
    settle_path = path.parent / "内蒙古（蒙西）电力多边交易市场结算数据-2025至2026年5月.xlsx"
    if settle_path.exists():
        try:
            xl2 = pd.ExcelFile(settle_path)
            df_s = xl2.parse(0, header=0)
            # Col layout: year, month, ?, total_volume, ?, contract_vol, ..., thermal_vol, wind_vol...
            for _, r in df_s.iterrows():
                try:
                    yr, mo = int(r.iloc[0]), int(r.iloc[1])
                    d = date(yr, mo, 1)
                except (ValueError, TypeError):
                    continue
                rows.setdefault(d, {"province": "蒙西", "report_month": d, "source_file": path.name})
                rows[d].update({
                    "total_traded_gwh":    _safe(r.iloc[3]),
                    "thermal_generation_gwh": _safe(r.iloc[8]),
                    "wind_generation_gwh": _safe(r.iloc[9]),
                    "avg_settlement_price": _safe(r.iloc[4]) or None,
                })
        except Exception as e:
            logger.debug("蒙西 settlement parse error: %s", e)

    return list(rows.values())


def _parse_shandong(path: Path) -> list[dict]:
    """山东 — 山东电力市场信息披露月报."""
    rows: dict[date, dict] = {}
    xl = pd.ExcelFile(path)

    # ── Capacity (sheet 9, 万kW) ───────────────────────────────────────────
    try:
        df_cap = xl.parse(9, header=1)
        for _, r in df_cap.iterrows():
            d = _to_date(r.iloc[0])
            if not d:
                continue
            rows.setdefault(d, {"province": "山东", "report_month": d, "source_file": path.name})
            rows[d].update({
                "thermal_capacity_mw": _wan_kw_to_mw(r.iloc[1]),
                "wind_capacity_mw":    _wan_kw_to_mw(r.iloc[5]),
                "solar_capacity_mw":   _wan_kw_to_mw(r.iloc[9]),
                "hydro_capacity_mw":   _wan_kw_to_mw(r.iloc[16]),
                "nuclear_capacity_mw": _wan_kw_to_mw(r.iloc[20]),
                "thermal_generation_gwh": _safe(r.iloc[3]),
                "wind_generation_gwh":    _safe(r.iloc[7]),
                "solar_generation_gwh":   _safe(r.iloc[11]),
                "hydro_generation_gwh":   _safe(r.iloc[18]),
            })
    except Exception as e:
        logger.debug("山东 cap error: %s", e)

    # ── Market participants (sheet 1) ──────────────────────────────────────
    try:
        df_p = xl.parse(1, header=1)
        for _, r in df_p.iterrows():
            d = _to_date(r.iloc[0])
            if not d or d not in rows:
                continue
            rows[d].update({
                "market_participants_total": _safe_int(r.iloc[9]),
                "retailers":   _safe_int(r.iloc[4]),
                "generators":  _safe_int(r.iloc[5]),
                "bess_participants": _safe_int(r.iloc[6]),
            })
    except Exception as e:
        logger.debug("山东 participants error: %s", e)

    # ── Settlement/retail (sheet 6) ────────────────────────────────────────
    try:
        df_ret = xl.parse(6, header=1)
        for _, r in df_ret.iterrows():
            d = _to_date(r.iloc[0])
            if not d:
                continue
            rows.setdefault(d, {"province": "山东", "report_month": d, "source_file": path.name})
            rows[d].update({
                "total_traded_gwh":        _safe(r.iloc[1]),
                "avg_settlement_price":    _safe(r.iloc[3]),
                "retailer_settlement_price": _safe(r.iloc[5]),
                "retailer_traded_gwh":       _safe(r.iloc[4]),
            })
    except Exception as e:
        logger.debug("山东 retail error: %s", e)

    # ── Spot market (sheet 3) ──────────────────────────────────────────────
    try:
        df_spot = xl.parse(3, header=1)
        for _, r in df_spot.iterrows():
            d = _to_date(r.iloc[0])
            if not d or d not in rows:
                continue
            rows[d].update({
                "spot_traded_gwh": _safe(r.iloc[1]),
                "spot_avg_price":  _safe(r.iloc[3]),
            })
    except Exception as e:
        logger.debug("山东 spot error: %s", e)

    # ── FR/ancillary market (sheet 7) ─────────────────────────────────────
    try:
        df_fr = xl.parse(7, header=1)
        for _, r in df_fr.iterrows():
            d = _to_date(r.iloc[0])
            if not d or d not in rows:
                continue
            # cols: 调频费用, 调峰补贴, 爬坡考核, 新能源偏差, 考核扣费...
            fr = _safe(r.iloc[1])
            peak = _safe(r.iloc[2])
            anc = _safe(r.iloc[3])
            rows[d].update({
                "fr_pool_million_yuan":               round(fr / 100, 4) if fr else None,
                "peak_shaving_million_yuan":          round(peak / 100, 4) if peak else None,
                "renewable_deviation_million_yuan":   round(anc / 100, 4) if anc else None,
            })
            # total_ancillary = sum of all cost columns
            totals = [_safe(r.iloc[i]) for i in range(1, min(8, len(r))) if _safe(r.iloc[i]) is not None]
            if totals:
                rows[d]["total_ancillary_million_yuan"] = round(sum(totals) / 100, 4)
    except Exception as e:
        logger.debug("山东 FR error: %s", e)

    return list(rows.values())


def _parse_anhui(path: Path) -> list[dict]:
    """安徽 — 安徽电力市场信息披露月报 (万千瓦 = 万kW)."""
    rows: dict[date, dict] = {}
    xl = pd.ExcelFile(path)

    # Sheet 0: capacity
    try:
        df = xl.parse(0, header=1)
        for _, r in df.iterrows():
            d = _to_date(r.iloc[0])
            if not d:
                continue
            rows.setdefault(d, {"province": "安徽", "report_month": d, "source_file": path.name})
            rows[d].update({
                "total_capacity_mw":   _wan_kw_to_mw(r.iloc[1]),
                "thermal_capacity_mw": _wan_kw_to_mw(r.iloc[2]),
                "hydro_capacity_mw":   _wan_kw_to_mw(r.iloc[3]),
                "wind_capacity_mw":    _wan_kw_to_mw(r.iloc[4]),
                "solar_capacity_mw":   _wan_kw_to_mw(r.iloc[5]),
                "bess_capacity_mw":    _wan_kw_to_mw(r.iloc[6]),
                "total_generation_gwh": _safe(r.iloc[7]),
                "thermal_generation_gwh": _safe(r.iloc[9]),
                "hydro_generation_gwh":   _safe(r.iloc[12]),
                "wind_generation_gwh":    _safe(r.iloc[14]),
                "solar_generation_gwh":   _safe(r.iloc[16]),
            })
    except Exception as e:
        logger.debug("安徽 cap error: %s", e)

    return list(rows.values())


def _parse_ningxia(path: Path) -> list[dict]:
    """宁夏 — 宁夏信息披露月报 (万kW)."""
    rows: dict[date, dict] = {}
    xl = pd.ExcelFile(path)

    # Sheet 0: capacity (header row 2)
    try:
        df = xl.parse(0, header=2)
        for _, r in df.iterrows():
            d = _to_date(r.iloc[0])
            if not d:
                continue
            rows.setdefault(d, {"province": "宁夏", "report_month": d, "source_file": path.name})
            rows[d].update({
                "total_capacity_mw":   _wan_kw_to_mw(r.iloc[1]),
                "thermal_capacity_mw": _wan_kw_to_mw(r.iloc[2]),
                "hydro_capacity_mw":   _wan_kw_to_mw(r.iloc[3]),
                "wind_capacity_mw":    _wan_kw_to_mw(r.iloc[4]),
                "solar_capacity_mw":   _wan_kw_to_mw(r.iloc[5]),
                "bess_capacity_mw":    _wan_kw_to_mw(r.iloc[7]),  # 储能容量(万kW)
                "wind_generation_gwh": _safe(r.iloc[15]),
                "solar_generation_gwh":_safe(r.iloc[16]),
                "max_load_mw":         _wan_kw_to_mw(r.iloc[17]),
                "total_generation_gwh":_safe(r.iloc[12]),
            })
    except Exception as e:
        logger.debug("宁夏 cap error: %s", e)

    # Sheet 1: settlement volumes by participant type
    try:
        df_s = xl.parse(1, header=1)
        for _, r in df_s.iterrows():
            d = _to_date(r.iloc[0])
            if not d or d not in rows:
                continue
            # col[1]=用户合计量, col[2]=用户合计价
            rows[d].update({
                "total_traded_gwh":     _safe(r.iloc[1]),
                "avg_settlement_price": _safe(r.iloc[2]),
                "bess_settlement_price": _safe(r.iloc[22]) if len(r) > 22 else None,
            })
    except Exception as e:
        logger.debug("宁夏 settlement error: %s", e)

    return list(rows.values())


def _parse_guangxi(path: Path) -> list[dict]:
    """广西 — 广西-信息披露 (万kW for capacity)."""
    rows: dict[date, dict] = {}
    xl = pd.ExcelFile(path)

    # Sheet 2: end-of-month capacity (year, month, 水电, 燃煤, 燃气, 核电, 风电, 光伏, 生物质, 储能, 合计)
    try:
        df_cap = xl.parse(2, header=1)
        for _, r in df_cap.iterrows():
            try:
                yr, mo = int(float(r.iloc[0])), int(float(r.iloc[1]))
                d = date(yr, mo, 1)
            except (ValueError, TypeError):
                continue
            rows.setdefault(d, {"province": "广西", "report_month": d, "source_file": path.name})
            rows[d].update({
                "hydro_capacity_mw":   _wan_kw_to_mw(r.iloc[2]),
                "thermal_capacity_mw": _wan_kw_to_mw(r.iloc[3]),  # 燃煤
                "nuclear_capacity_mw": _wan_kw_to_mw(r.iloc[5]),
                "wind_capacity_mw":    _wan_kw_to_mw(r.iloc[6]),
                "solar_capacity_mw":   _wan_kw_to_mw(r.iloc[7]),
                "bess_capacity_mw":    _wan_kw_to_mw(r.iloc[9]),
                "total_capacity_mw":   _wan_kw_to_mw(r.iloc[10]),
            })
    except Exception as e:
        logger.debug("广西 cap error: %s", e)

    # Sheet 0: 数据汇总 — monthly generation volumes and settlement info
    try:
        df_s = xl.parse(0, header=2)
        # structure is nested, try to find a monthly summary row
        month_col = _find_col(df_s, ['月份']) or df_s.columns[0]
        vol_col = _find_col(df_s, ['成交量']) or _find_col(df_s, ['交易量'])
        price_col = _find_col(df_s, ['均价']) or _find_col(df_s, ['结算价'])
        for _, r in df_s.iterrows():
            d = _to_date(r[month_col]) if month_col else None
            if not d:
                continue
            rows.setdefault(d, {"province": "广西", "report_month": d, "source_file": path.name})
            if vol_col:
                rows[d]["total_traded_gwh"] = _safe(r[vol_col])
            if price_col:
                rows[d]["avg_settlement_price"] = _safe(r[price_col])
    except Exception as e:
        logger.debug("广西 summary error: %s", e)

    # Sheet 5: monthly settlement 成交-原始 (month, type, vol, price rows)
    try:
        df_settle = xl.parse(5, header=1)
        for _, r in df_settle.iterrows():
            d = _to_date(r.iloc[0])
            if not d:
                continue
            rows.setdefault(d, {"province": "广西", "report_month": d, "source_file": path.name})
            label = str(r.iloc[1]) if len(r) > 1 else ""
            if "合计" in label or "总计" in label:
                rows[d]["total_traded_gwh"] = _safe(r.iloc[2]) if not rows[d].get("total_traded_gwh") else rows[d]["total_traded_gwh"]
                rows[d]["avg_settlement_price"] = _safe(r.iloc[3]) if not rows[d].get("avg_settlement_price") else rows[d]["avg_settlement_price"]
    except Exception:
        pass

    return list(rows.values())


def _parse_mengdong(path: Path) -> list[dict]:
    """蒙东 — 蒙东-电力交易市场信息披露 (万kW)."""
    rows: dict[date, dict] = {}
    xl = pd.ExcelFile(path)

    # Sheet 0: year, month, total, fire, renewables, hydro, bess, ...
    try:
        df = xl.parse(0, header=0)
        for _, r in df.iterrows():
            try:
                yr, mo = int(float(r.iloc[0])), int(float(r.iloc[1]))
                d = date(yr, mo, 1)
            except (ValueError, TypeError):
                continue
            rows.setdefault(d, {"province": "蒙东", "report_month": d, "source_file": path.name})
            rows[d].update({
                "total_capacity_mw":   _wan_kw_to_mw(r.iloc[2]),
                "thermal_capacity_mw": _wan_kw_to_mw(r.iloc[3]),
                "bess_capacity_mw":    _wan_kw_to_mw(r.iloc[6]) if _safe(r.iloc[6]) else None,
                "market_participants_total": _safe_int(r.iloc[7]) if len(r) > 7 else None,
                "generators": _safe_int(r.iloc[10]) if len(r) > 10 else None,
            })
    except Exception as e:
        logger.debug("蒙东 cap error: %s", e)

    # Settlement file
    settle_path = path.parent / "蒙东结算情况及分类构成表数据汇总-2024-07至2026-05.xlsx"
    if settle_path.exists():
        try:
            xl2 = pd.ExcelFile(settle_path)
            df_s = xl2.parse(0, header=1)
            month_col = _find_col(df_s, ['月份']) or df_s.columns[0]
            for _, r in df_s.iterrows():
                d = _to_date(r[month_col])
                if not d or d not in rows:
                    continue
                vol_col = _find_col(df_s, ['成交量']) or _find_col(df_s, ['结算量'])
                price_col = _find_col(df_s, ['均价']) or _find_col(df_s, ['结算价'])
                if vol_col:
                    rows[d]["total_traded_gwh"] = _safe(r[vol_col])
                if price_col:
                    rows[d]["avg_settlement_price"] = _safe(r[price_col])
        except Exception as e:
            logger.debug("蒙东 settlement error: %s", e)

    return list(rows.values())


def _parse_shanxi(path: Path) -> list[dict]:
    """山西 — 山西信息披露月报数据 (千kW, no header)."""
    rows: dict[date, dict] = {}
    xl = pd.ExcelFile(path)

    # Sheet 0: col0=timestamp, col1=total, col2=fire, col3=hydro, col4=nuclear, col5=wind, col6=solar, col7=bess?
    try:
        df = xl.parse(0, header=0)
        for _, r in df.iterrows():
            d = _to_date(r.iloc[0])
            if not d:
                continue
            rows.setdefault(d, {"province": "山西", "report_month": d, "source_file": path.name})
            rows[d].update({
                "total_capacity_mw":   _qian_kw_to_mw(r.iloc[1]),
                "thermal_capacity_mw": _qian_kw_to_mw(r.iloc[2]),
                "hydro_capacity_mw":   _qian_kw_to_mw(r.iloc[3]),
                "nuclear_capacity_mw": _qian_kw_to_mw(r.iloc[4]),
                "wind_capacity_mw":    _qian_kw_to_mw(r.iloc[5]),
                "solar_capacity_mw":   _qian_kw_to_mw(r.iloc[6]),
                "bess_capacity_mw":    _qian_kw_to_mw(r.iloc[7]) if len(r) > 7 else None,
            })
    except Exception as e:
        logger.debug("山西 cap error: %s", e)

    return list(rows.values())


def _parse_xinjiang(path: Path) -> list[dict]:
    """新疆 — 新疆信息披露月报数据汇总.

    Sheet[0] = 目录 (TOC) — skip.
    Sheet[1] = 装机情况 (capacity in 千kW, header=0):
        col[0]=月份, col[1]=总装机, col[3]=煤电, col[5]=水电, col[7]=风电, col[9]=光电
    Sheet[5] = 省内交易 (volumes/prices, two-row header; use header=1):
        col[0]=月份, col[1]=年度成交量(亿kWh), col[3]=均价(元/MWh)
    """
    rows: dict[date, dict] = {}
    xl = pd.ExcelFile(path)

    # ── Capacity (sheet 1, 千kW, header=0) ──────────────────────────────────
    try:
        df_cap = xl.parse(1, header=0)
        for _, r in df_cap.iterrows():
            d = _to_date(r.iloc[0])
            if not d:
                continue
            rows.setdefault(d, {"province": "新疆", "report_month": d, "source_file": path.name})
            rows[d].update({
                "total_capacity_mw":   _qian_kw_to_mw(r.iloc[1]),
                "thermal_capacity_mw": _qian_kw_to_mw(r.iloc[3]) if len(r) > 3 else None,
                "hydro_capacity_mw":   _qian_kw_to_mw(r.iloc[5]) if len(r) > 5 else None,
                "wind_capacity_mw":    _qian_kw_to_mw(r.iloc[7]) if len(r) > 7 else None,
                "solar_capacity_mw":   _qian_kw_to_mw(r.iloc[9]) if len(r) > 9 else None,
            })
    except Exception as e:
        logger.debug("新疆 cap error: %s", e)

    # ── Provincial trading volumes + prices (sheet 5, header=1) ─────────────
    # Row 0 is a merged header group; row 1 contains actual column names.
    # After header=1: col[0]=月份, col[1]=年度成交量(亿kWh), col[3]=均价(元/MWh)
    try:
        df_vol = xl.parse(5, header=1)
        for _, r in df_vol.iterrows():
            d = _to_date(r.iloc[0])
            if not d:
                continue
            rows.setdefault(d, {"province": "新疆", "report_month": d, "source_file": path.name})
            if not rows[d].get("total_traded_gwh"):
                rows[d]["total_traded_gwh"] = _safe(r.iloc[1])
            if not rows[d].get("avg_settlement_price"):
                rows[d]["avg_settlement_price"] = _safe(r.iloc[3]) if len(r) > 3 else None
    except Exception as e:
        logger.debug("新疆 vol error: %s", e)

    return list(rows.values())


def _normalize_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns by stripping newlines/whitespace so _find_col can match them."""
    mapping = {c: str(c).replace('\n', ' ').replace('\r', ' ').strip() for c in df.columns}
    # avoid duplicate column names after normalisation
    seen: dict[str, int] = {}
    deduped = {}
    for orig, new in mapping.items():
        if new in seen:
            seen[new] += 1
            new = f"{new}_{seen[new]}"
        else:
            seen[new] = 0
        deduped[orig] = new
    return df.rename(columns=deduped)


def _detect_month_col(df: pd.DataFrame) -> tuple[Optional[str], Optional[str]]:
    """
    Detect the column that holds dates.
    Returns (month_col, year_col):
      - If single YYYY-MM / Chinese date col found: (month_col, None)
      - If year+month paired int cols found: (None, year_col)  ← use both cols[i] + cols[i+1]
      - If neither: (None, None)
    """
    cols = list(df.columns)
    for i, c in enumerate(cols):
        cs = str(c).replace('\n', ' ').strip()
        # If col name itself is a parseable date string → first real data row became header
        # In that case look in VALUES of that column
        sample = df[c].dropna().head(5)
        date_values = [v for v in sample if _to_date(v) is not None]
        if date_values:
            return c, None
        # Year+month pair pattern: col name = '年份'/'年', next = '月份'/'月'
        if ('年' in cs) and i + 1 < len(cols):
            nc = str(cols[i + 1]).replace('\n', ' ').strip()
            if '月' in nc:
                # Verify values look like years (2020–2030)
                yr_vals = df[c].dropna().head(5)
                if any(str(v)[:4].isdigit() and 2020 <= int(float(str(v)[:4])) <= 2030
                       for v in yr_vals):
                    return None, c
    return None, None


def _row_to_date(r, month_col: Optional[str], year_col: Optional[str],
                 df_cols: list) -> Optional[date]:
    """Extract a date from a row given the column strategy."""
    if month_col is not None:
        return _to_date(r[month_col])
    if year_col is not None:
        try:
            yr = int(float(r[year_col]))
            mo_col = df_cols[df_cols.index(year_col) + 1]
            mo_raw = str(r[mo_col]).replace('月', '').strip()
            mo = int(float(mo_raw))
            return date(yr, mo, 1)
        except Exception:
            return None
    return None


def _parse_generic_info_monthly(path: Path, province: str) -> list[dict]:
    """
    Generic parser for 信息披露月报 files.
    Handles multiple header styles:
      - header=1 with YYYY-MM or Chinese date col
      - header=0 when row 0 contains column labels
      - header=1 with year+month as separate int columns
    """
    rows: dict[date, dict] = {}
    xl = pd.ExcelFile(path)

    for sheet_idx in range(min(8, len(xl.sheet_names))):
        for hdr in [1, 0]:
            try:
                df_raw = xl.parse(sheet_idx, header=hdr)
                if len(df_raw.columns) < 2 or len(df_raw) < 2:
                    continue

                df = _normalize_cols(df_raw)
                month_col, year_col = _detect_month_col(df)
                if month_col is None and year_col is None:
                    continue

                cap_col   = (_find_col(df, ['总装机'])
                             or _find_col(df, ['装机容量'])
                             or _find_col(df, ['全网装机']))
                wind_col  = _find_col(df, ['风电', '装机'])
                solar_col = (_find_col(df, ['光伏', '装机'])
                             or _find_col(df, ['太阳能', '装机']))
                fire_col  = (_find_col(df, ['火电', '装机'])
                             or _find_col(df, ['燃煤', '装机'])
                             or _find_col(df, ['热电', '装机']))
                bess_col  = (_find_col(df, ['储能', '装机'])
                             or _find_col(df, ['储能容量'])
                             or _find_col(df, ['独立储能']))
                vol_col   = (_find_col(df, ['成交量', '合计'])
                             or _find_col(df, ['成交', '合计'])
                             or _find_col(df, ['结算电量', '合计'])
                             or _find_col(df, ['用电量', '合计'])
                             or _find_col(df, ['上网电量', '合计'])
                             or _find_col(df, ['成交', '总']))
                price_col = (_find_col(df, ['均价'], exclude=['同比', '占比', '涨跌'])
                             or _find_col(df, ['结算价'], exclude=['同比']))
                spot_vol  = (_find_col(df, ['现货', '电量'])
                             or _find_col(df, ['现货', '成交'])
                             or _find_col(df, ['日前', '电量']))
                spot_px   = (_find_col(df, ['现货', '价格'])
                             or _find_col(df, ['现货', '均价'])
                             or _find_col(df, ['日前', '均价']))
                in_vol    = (_find_col(df, ['外来电'])
                             or _find_col(df, ['购入', '省间'])
                             or _find_col(df, ['省间', '购']))
                out_vol   = (_find_col(df, ['外送电'])
                             or _find_col(df, ['省间', '销'])
                             or _find_col(df, ['送出', '省间']))
                max_load  = (_find_col(df, ['最大', '负荷'])
                             or _find_col(df, ['最高', '负荷'])
                             or _find_col(df, ['最大负荷']))

                # Need at least one useful column
                if not any([cap_col, vol_col, wind_col, solar_col, bess_col, price_col]):
                    continue

                df_cols = list(df.columns)
                for _, r in df.iterrows():
                    d = _row_to_date(r, month_col, year_col, df_cols)
                    if not d:
                        continue
                    rows.setdefault(d, {"province": province, "report_month": d,
                                        "source_file": path.name})

                    def cap_val(col):
                        if col is None:
                            return None
                        v = _safe(r[col])
                        if v is None:
                            return None
                        cap_total = _safe(r[cap_col]) if cap_col else None
                        if cap_total and cap_total > 200:
                            return round(v * 10, 2)   # 万kW → MW
                        return round(v, 2)              # 千kW → MW

                    if cap_col and not rows[d].get("total_capacity_mw"):
                        rows[d]["total_capacity_mw"] = cap_val(cap_col)
                    if wind_col and not rows[d].get("wind_capacity_mw"):
                        rows[d]["wind_capacity_mw"] = cap_val(wind_col)
                    if solar_col and not rows[d].get("solar_capacity_mw"):
                        rows[d]["solar_capacity_mw"] = cap_val(solar_col)
                    if fire_col and not rows[d].get("thermal_capacity_mw"):
                        rows[d]["thermal_capacity_mw"] = cap_val(fire_col)
                    if bess_col and not rows[d].get("bess_capacity_mw"):
                        rows[d]["bess_capacity_mw"] = cap_val(bess_col)
                    if vol_col and not rows[d].get("total_traded_gwh"):
                        rows[d]["total_traded_gwh"] = _safe(r[vol_col])
                    if price_col and not rows[d].get("avg_settlement_price"):
                        rows[d]["avg_settlement_price"] = _safe(r[price_col])
                    if spot_vol and not rows[d].get("spot_traded_gwh"):
                        rows[d]["spot_traded_gwh"] = _safe(r[spot_vol])
                    if spot_px and not rows[d].get("spot_avg_price"):
                        rows[d]["spot_avg_price"] = _safe(r[spot_px])
                    if in_vol and not rows[d].get("incoming_gwh"):
                        rows[d]["incoming_gwh"] = _safe(r[in_vol])
                    if out_vol and not rows[d].get("outgoing_gwh"):
                        rows[d]["outgoing_gwh"] = _safe(r[out_vol])
                    if max_load and not rows[d].get("max_load_mw"):
                        v = _safe(r[max_load])
                        if v and v > 200:
                            rows[d]["max_load_mw"] = round(v * 10, 2)
                        elif v:
                            rows[d]["max_load_mw"] = round(v, 2)

                break  # found a working header, skip trying the other

            except Exception as e:
                logger.debug("%s sheet[%d] hdr=%d error: %s", province, sheet_idx, hdr, e)

    return list(rows.values())


def _parse_jinan_info(path: Path) -> list[dict]:
    """冀南 — 河北南网电力市场信息报告数据汇总 (综合概况 sheet).

    Sheet[0] has two title/blank rows before the header:
      row 0: title string
      row 1: blank
      row 2: 月份, 注册用量, 工商企业, 居民用户, 售电企业,
              中长期合同量(亿kWh), 月度交易合计(亿kWh), 省内交易合计(亿kWh),
              日前现货量(亿kWh), 实时现货量(亿kWh), 总结算量(亿kWh),
              结算金额(亿元), 售电公司量(亿kWh), 售电公司价(亿元)
    """
    rows: dict[date, dict] = {}
    try:
        xl = pd.ExcelFile(path)
        df = xl.parse(0, header=2)
        for _, r in df.iterrows():
            d = _to_date(r.iloc[0])
            if not d:
                continue
            rows.setdefault(d, {"province": "冀南", "report_month": d, "source_file": path.name})
            rows[d].update({
                "market_participants_total": _safe_int(r.iloc[1]),
                "retailers":                 _safe_int(r.iloc[4]) if len(r) > 4 else None,
                "contract_traded_gwh":       _safe(r.iloc[5])  if len(r) > 5 else None,
                "spot_traded_gwh":           _safe(r.iloc[8])  if len(r) > 8 else None,
                "total_traded_gwh":          _safe(r.iloc[10]) if len(r) > 10 else None,
                "retailer_traded_gwh":       _safe(r.iloc[12]) if len(r) > 12 else None,
            })
    except Exception as e:
        logger.debug("冀南 info error: %s", e)
    return list(rows.values())


def _parse_guangdong(path: Path) -> list[dict]:
    """广东 — 广东电力市场结算数据_宽窄表 (wide format sheet 1 = 宽表)."""
    rows: dict[date, dict] = {}
    try:
        xl = pd.ExcelFile(path)
        # Wide sheet: province, month, year, month_num, category, subcategory, row_num, sub_row_num, metric, value
        df = xl.parse(1, header=0)
        month_col = _find_col(df, ['月份']) or df.columns[1]
        for _, r in df.iterrows():
            d = _to_date(r[month_col])
            if not d:
                continue
            rows.setdefault(d, {"province": "广东", "report_month": d, "source_file": path.name})
            cat = str(r.get('数据分类', '') or '')
            metric = str(r.get('指标') or r.get('指标名称') or '')
            val_col = '数值' if '数值' in df.columns else df.columns[-1]
            v = _safe(r[val_col])
            if v is None:
                continue
            if '总成交' in metric or ('成交量' in metric and '合计' in cat):
                rows[d].setdefault("total_traded_gwh", v)
            elif '均价' in metric and '合计' in cat:
                rows[d].setdefault("avg_settlement_price", v)
    except Exception as e:
        logger.debug("广东 error: %s", e)

    # Also check the market stats file
    stats_path = path.parent / next(
        (f for f in ["广东电力市场运营简报_2026年1-5月.xlsx"] if (path.parent / f).exists()), ""
    )
    if stats_path.exists():
        try:
            xl2 = pd.ExcelFile(stats_path)
            for si in range(min(5, len(xl2.sheet_names))):
                df2 = xl2.parse(si, header=1)
                month_col = _find_col(df2, ['月份']) or df2.columns[0]
                for _, r in df2.iterrows():
                    d = _to_date(r[month_col])
                    if not d:
                        continue
                    rows.setdefault(d, {"province": "广东", "report_month": d, "source_file": path.name})
        except Exception:
            pass

    return list(rows.values())


def _parse_jinan(path: Path, province: str) -> list[dict]:
    """冀南/冀北 — settlement by generation type (year, month, type, volume, price)."""
    rows: dict[date, dict] = {}
    try:
        xl = pd.ExcelFile(path)
        df = xl.parse(0, header=0)
        for _, r in df.iterrows():
            try:
                yr, mo = int(float(r.iloc[0])), int(float(r.iloc[1]))
                d = date(yr, mo, 1)
            except (ValueError, TypeError):
                continue
            label = str(r.iloc[2]) if len(r) > 2 else ""
            vol = _safe(r.iloc[3])
            price = _safe(r.iloc[4])
            rows.setdefault(d, {"province": province, "report_month": d, "source_file": path.name})
            if "合计" in label:
                rows[d]["total_traded_gwh"] = vol
                rows[d]["avg_settlement_price"] = price
            elif "储能" in label:
                rows[d]["bess_settlement_price"] = price
                rows[d]["bess_traded_volume"] = vol
            elif "风" in label:
                rows[d]["wind_settlement_price"] = price
            elif "光" in label or "太阳" in label:
                rows[d]["solar_settlement_price"] = price
            elif "火" in label or "煤" in label:
                rows[d]["thermal_settlement_price"] = price
    except Exception as e:
        logger.debug("%s parse error: %s", province, e)
    return list(rows.values())


def _parse_yunnan(path: Path) -> list[dict]:
    """云南 — 云南电力交易月报数据库."""
    rows: dict[date, dict] = {}
    xl = pd.ExcelFile(path)

    # Sheet 2: 市场化交易汇总 (header=0)
    # col[0]=数据月份, col[1]=清洁能源量(亿kWh), col[2]=清洁能源均价(元/kWh),
    # col[3]=燃煤量, col[5]=跨省量, col[6]=跨省均价(元/kWh), col[7]=省内合计量(亿kWh)
    try:
        df = xl.parse(2, header=0)
        for _, r in df.iterrows():
            d = _to_date(r.iloc[0])
            if not d:
                continue
            rows.setdefault(d, {"province": "云南", "report_month": d, "source_file": path.name})
            px = _safe(r.iloc[2])  # yuan/kWh → yuan/MWh
            rows[d].update({
                "total_traded_gwh":     _safe(r.iloc[7]),
                "avg_settlement_price": round(px * 1000, 2) if px else None,
                "incoming_gwh":         _safe(r.iloc[5]),
            })
    except Exception as e:
        logger.debug("云南 error: %s", e)

    return list(rows.values())


# ---------------------------------------------------------------------------
# Province-file dispatch table
# ---------------------------------------------------------------------------

# Map: (folder_name_pattern, file_name_pattern) → (province_label, parser_fn)
_DISPATCH = [
    ("蒙西月报", "蒙西信息披露月报数据汇总",  "蒙西",   _parse_mengxi),
    ("山东月报", "山东电力市场信息披露月报",   "山东",   _parse_shandong),
    ("安徽月报", "安徽电力市场信息披露月报",   "安徽",   _parse_anhui),
    ("宁夏月报", "宁夏信息披露月报",          "宁夏",   _parse_ningxia),
    ("广西月报", "广西-信息披露",             "广西",   _parse_guangxi),
    ("蒙东月报", "蒙东-电力交易市场信息披露",  "蒙东",   _parse_mengdong),
    ("山西月报", "山西信息披露月报数据",       "山西",   _parse_shanxi),
    ("广东月报", "广东电力市场结算数据",       "广东",   _parse_guangdong),
    ("云南月报", "云南电力交易月报数据库",     "云南",   _parse_yunnan),
    # 冀南 — uses jinan parser
    ("冀南月报", "河北南网-市场化交易结算",    "冀南",   lambda p: _parse_jinan(p, "冀南")),
    ("冀北月报", "冀北2024年以来电力市场",     "冀北",   lambda p: _parse_jinan(p, "冀北")),
    # 冀南 综合概况 (info report summary — header at row 2)
    ("冀南月报",  "河北南网-电力市场信息报告数据汇总", "冀南", _parse_jinan_info),
    # Generic info-monthly parser for remaining provinces
    ("冀北月报",  "冀北-信息披露月报",    "冀北",  lambda p: _parse_generic_info_monthly(p, "冀北")),
    ("冀南月报",  "河北南网-电力市场信息", "冀南",  lambda p: _parse_generic_info_monthly(p, "冀南")),
    ("吉林月报",  "吉林-信息披露月报",    "吉林",  lambda p: _parse_generic_info_monthly(p, "吉林")),
    ("天津月报",  "天津电力市场",        "天津",  lambda p: _parse_generic_info_monthly(p, "天津")),
    ("河南月报",  "河南信息披露月报",     "河南",  lambda p: _parse_generic_info_monthly(p, "河南")),
    ("湖南月报",  "湖南-信息披露",       "湖南",  lambda p: _parse_generic_info_monthly(p, "湖南")),
    ("湖北月报",  "湖北-信息披露",       "湖北",  lambda p: _parse_generic_info_monthly(p, "湖北")),
    ("新疆月报",  "新疆信息披露",        "新疆",  _parse_xinjiang),
    ("甘肃月报",  "甘肃-信息披露",       "甘肃",  lambda p: _parse_generic_info_monthly(p, "甘肃")),
    ("辽宁月报",  "辽宁-信息披露",       "辽宁",  lambda p: _parse_generic_info_monthly(p, "辽宁")),
    ("陕西月报",  "陕西-信息披露",       "陕西",  lambda p: _parse_generic_info_monthly(p, "陕西")),
    ("贵州月报",  "贵州",               "贵州",  lambda p: _parse_generic_info_monthly(p, "贵州")),
    ("黑龙江月报","黑龙江-披露",         "黑龙江",lambda p: _parse_generic_info_monthly(p, "黑龙江")),
    ("海南月报",  "海南",               "海南",  lambda p: _parse_generic_info_monthly(p, "海南")),
    ("青海月报",  "青海",               "青海",  lambda p: _parse_generic_info_monthly(p, "青海")),
    # 江苏 primary monthly file
    ("江苏月报",  "江苏-信息披露月报数据", "江苏", lambda p: _parse_generic_info_monthly(p, "江苏")),
    # 天津settlement breakdown
    ("天津月报",  "天津结算总体情况及分类构成", "天津", lambda p: _parse_generic_info_monthly(p, "天津")),
    # 宁夏settlement DB
    ("宁夏月报",  "宁夏电力市场结算数据库",    "宁夏", lambda p: _parse_generic_info_monthly(p, "宁夏")),
    # 河南settlement breakdown
    ("河南月报",  "河南结算情况及分类构成",    "河南", lambda p: _parse_generic_info_monthly(p, "河南")),
    # 陕西settlement
    ("陕西月报",  "陕西结算及分类构成",        "陕西", lambda p: _parse_generic_info_monthly(p, "陕西")),
    # 黑龙江settlement
    ("黑龙江月报","黑龙江结算及分类构成",      "黑龙江", lambda p: _parse_generic_info_monthly(p, "黑龙江")),
    # 贵州settlement
    ("贵州月报",  "贵州结算情况及分类构成",    "贵州",  lambda p: _parse_generic_info_monthly(p, "贵州")),
    # 青海settlement
    ("青海月报",  "青海电力市场结算总体情况",  "青海",  lambda p: _parse_generic_info_monthly(p, "青海")),
    # 云南secondary
    ("云南月报",  "云南电力交易结算情况报告",  "云南",  lambda p: _parse_generic_info_monthly(p, "云南")),
    # 全国 / 北京 (in 全国月报 folder)
    ("全国月报",  "北京电网市场交易信息",      "北京",  lambda p: _parse_generic_info_monthly(p, "北京")),
]


def parse_excel_file(path: Path) -> tuple[str, list[dict]]:
    """
    Dispatch to the correct province parser based on folder/file name.
    Returns (province_label, list_of_monthly_dicts).
    """
    folder = path.parent.name
    stem = path.stem
    for folder_pat, file_pat, province, parser_fn in _DISPATCH:
        if folder_pat in folder and file_pat in stem:
            try:
                records = parser_fn(path)
                # Filter out months with no useful data
                useful = [r for r in records if any(
                    r.get(k) is not None for k in [
                        "total_capacity_mw", "bess_capacity_mw", "total_traded_gwh",
                        "avg_settlement_price", "wind_capacity_mw",
                    ]
                )]
                return province, useful
            except Exception as e:
                logger.error("Parser failed for %s: %s", path.name, e)
                return province, []
    logger.debug("No parser matched for %s / %s", folder, stem)
    return "", []


# ---------------------------------------------------------------------------
# DB upsert
# ---------------------------------------------------------------------------

def upsert_excel_metrics(records: list[dict], pg_url: str) -> int:
    """
    Upsert a list of monthly metric dicts into staging.exchange_excel_metrics.
    Returns the number of rows upserted.
    """
    import psycopg2

    _COLS = [
        "province", "report_month", "source_file",
        "total_capacity_mw", "thermal_capacity_mw", "hydro_capacity_mw",
        "nuclear_capacity_mw", "wind_capacity_mw", "solar_capacity_mw",
        "bess_capacity_mw", "other_capacity_mw",
        "total_generation_gwh", "thermal_generation_gwh", "hydro_generation_gwh",
        "wind_generation_gwh", "solar_generation_gwh",
        "total_traded_gwh", "spot_traded_gwh", "contract_traded_gwh",
        "avg_settlement_price", "spot_avg_price", "contract_avg_price",
        "thermal_settlement_price", "wind_settlement_price", "solar_settlement_price",
        "bess_settlement_price", "retailer_settlement_price",
        "market_participants_total", "retailers", "generators", "bess_participants",
        "incoming_gwh", "outgoing_gwh", "max_load_mw",
        "fr_pool_million_yuan", "peak_shaving_million_yuan",
        "renewable_deviation_million_yuan", "total_ancillary_million_yuan",
        "retailer_traded_gwh", "retailer_service_fee_million_yuan",
    ]

    set_clause = ", ".join(
        f"{c} = EXCLUDED.{c}" for c in _COLS
        if c not in ("province", "report_month")
    )
    sql = f"""
        INSERT INTO staging.exchange_excel_metrics ({", ".join(_COLS)})
        VALUES ({", ".join(["%s"] * len(_COLS))})
        ON CONFLICT (province, report_month)
        DO UPDATE SET {set_clause}, ingested_at = NOW()
    """

    conn = psycopg2.connect(pg_url)
    count = 0
    try:
        with conn.cursor() as cur:
            for rec in records:
                vals = [rec.get(c) for c in _COLS]
                cur.execute(sql, vals)
                count += 1
        conn.commit()
    finally:
        conn.close()
    return count


# ---------------------------------------------------------------------------
# KB text export
# ---------------------------------------------------------------------------

def excel_to_kb_text(path: Path, province: str) -> str:
    """Convert Excel file sheets to plain text for KB ingestion."""
    import warnings
    warnings.filterwarnings("ignore")
    lines = [f"# {province} 电力交易月报数据 — {path.name}\n"]
    try:
        xl = pd.ExcelFile(path)
        for sn in xl.sheet_names[:12]:
            try:
                df = xl.parse(sn, header=1, nrows=60)
                if len(df) < 2 or len(df.columns) < 2:
                    continue
                lines.append(f"\n## {sn}\n")
                lines.append(df.dropna(how="all").to_string(index=False, max_rows=50))
                lines.append("\n")
            except Exception:
                continue
    except Exception:
        pass
    return "\n".join(lines)
