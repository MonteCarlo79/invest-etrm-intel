# services/retail_risk/schemas.py
"""Canonical frames and vocabulary maps for retail-risk ingestion.

Every parser emits plain DataFrames with these columns; loader.py is the only DB writer.
"""
from __future__ import annotations

import datetime
from dataclasses import dataclass, field
from typing import TypedDict

LOAD_BOOK_PROVINCES = ["冀南", "浙江", "山东", "安徽", "福建", "广东", "江苏", "上海"]

_SPOT_ALIAS = {"冀南": "河北南网"}


def book_name(province: str) -> str:
    return f"景融售电-{province}"


def spot_province(province: str) -> str:
    """Province name as used in marketdata.spot_prices_hourly."""
    return _SPOT_ALIAS.get(province, province)


# Province term -> (rm_positions.channel, rm_positions.instrument_type)
TERM_CHANNEL_INSTRUMENT: dict[str, tuple[str, str]] = {
    "年度双边": ("annual", "bilateral"),
    "年度竞价": ("annual", "forward"),
    "年度挂牌": ("annual", "forward"),
    "月度双边": ("monthly_auction", "bilateral"),
    "月度竞价": ("monthly_auction", "forward"),
    "月度挂牌": ("monthly_listed", "forward"),
    "日滚动": ("intramonth_match", "forward"),
    "滚撮": ("intramonth_match", "forward"),
    "月内": ("intramonth_match", "forward"),
    "10日竞价": ("intramonth_match", "forward"),
    "绿电": ("annual", "forward"),  # tenor overridden by source; counterparty='绿电'
}

TRADES_COLS = ["delivery_date", "hour", "channel", "instrument_type",
               "direction", "volume_mwh", "price_cny_mwh",
               "counterparty", "source_term", "source_file"]

VOLUMES_COLS = ["delivery_date", "hour", "channel", "volume_mwh",
                "vwap_cny_mwh", "nominated_mwh", "settled_mwh", "estimated"]

CURVES_COLS = ["province", "product", "delivery_date", "delivery_hour",
               "price_cny_kwh", "curve_date"]

CONTRACTS_COLS = ["customer_name", "contract_ref", "package_name", "package_class",
                  "contract_type", "price_cny_mwh", "share_ratio",
                  "start_date", "end_date", "annual_mwh", "monthly_mwh"]

BENCH_COLS = ["province", "channel", "month", "avg_price_cny_mwh", "volume_mwh",
              "source", "source_file"]

# Benchmark (exchange-stat) channel -> ours, for trader-alpha joins
BENCH_CHANNEL_MAP = {
    "双边协商交易": ["annual", "monthly_auction"],   # bilateral side joins annual + monthly bilateral rows
    "集中竞价交易": ["monthly_auction"],
    "挂牌交易": ["monthly_listed"],
    "月内集中竞价": ["intramonth_match"],
}


class InvoiceItem(TypedDict, total=False):
    category: str
    label_cn: str
    volume_mwh: float | None
    price_cny_mwh: float | None
    amount_cny: float
    delivery_date: datetime.date | None
    notes: str | None


@dataclass
class InvoiceDoc:
    settlement_month: datetime.date          # 1st of month
    items: list[InvoiceItem] = field(default_factory=list)
    total_amount_cny: float | None = None    # printed 售电公司收益/合计, for cross-check


# --- Settlement subject-code -> rm_settlement_items.category -----------------
# Ordered longest-prefix-first matching via category_for_code(). Codes come from
# each exchange's 结算科目编码 hierarchy (see data/exchange-annual-reports/2026年政策/).

JINAN_CATEGORY_RULES: list[tuple[str, str]] = [  # 河北 HEPX
    ("0101", "midlong_energy"),
    ("0102", "spot_energy"),
    ("0201", "green_premium"),
    ("0202030001", "imbalance"), ("0202030002", "imbalance"), ("0202030003", "imbalance"),
    ("0202030010", "market_redistribution"),
    ("0202", "market_redistribution"),
    ("0204", "imbalance"),
    ("021103", "rule_charges"),
]

ZHEJIANG_CATEGORY_RULES: list[tuple[str, str]] = [
    ("0101", "midlong_energy"),
    ("0102", "spot_energy"),
    ("0201", "green_premium"),
    ("02020300", "imbalance"),
    ("0202", "market_redistribution"),
    ("0204", "imbalance"),
    ("021103", "rule_charges"),
]

ANHUI_CATEGORY_RULES: list[tuple[str, str]] = [
    ("01010201", "midlong_energy"),
    ("0101", "midlong_energy"),
    ("0102", "spot_energy"),
    ("0201", "green_premium"),
    ("02020300", "imbalance"),
    ("0202", "market_redistribution"),
    ("0204", "imbalance"),
    ("021103", "rule_charges"),
]

PACKAGE_CLASS_TO_CONTRACT_TYPE = {
    "联动": "indexed",
    "固定": "fixed",
    "分时": "peak_offpeak",
    "峰谷": "peak_offpeak",
}


def contract_type_for(package_class: str) -> str:
    for key, ctype in PACKAGE_CLASS_TO_CONTRACT_TYPE.items():
        if key in (package_class or ""):
            return ctype
    return "indexed_band"


def category_for_code(code: str, rules: list[tuple[str, str]]) -> str:
    """Longest matching subject-code prefix wins; default 'other'."""
    best, best_len = "other", -1
    for prefix, cat in rules:
        if code.startswith(prefix) and len(prefix) > best_len:
            best, best_len = cat, len(prefix)
    return best
