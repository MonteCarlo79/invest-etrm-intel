"""
China Spot Market Price Cockpit
Visualises daily DA / RT clearing prices from spot_daily.

Run:
    py -m streamlit run apps/spot-market/app.py
"""
from __future__ import annotations

import os
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import json
import requests

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.font_manager as _mfm
from matplotlib.patches import Polygon as MplPolygon

# Pick the first CJK-capable font available on this system
_CJK_FONTS = ["Microsoft YaHei", "SimHei", "SimSun", "STHeiti",
               "WenQuanYi Micro Hei", "Noto Sans CJK SC", "Arial Unicode MS"]
_CJK_FONT: str | None = None
for _f in _CJK_FONTS:
    try:
        if _mfm.findfont(_mfm.FontProperties(family=_f), fallback_to_default=False):
            _CJK_FONT = _f
            break
    except (ValueError, OSError):
        pass
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import psycopg2
import streamlit as st
from dotenv import load_dotenv

# ── path / env setup ─────────────────────────────────────────────────────────
_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

for _env in [_REPO / "config" / ".env", _REPO / ".env"]:
    if _env.exists():
        load_dotenv(_env)
_spot_env = _REPO / "apps" / "spot-agent" / ".env"
if _spot_env.exists():
    load_dotenv(_spot_env)

# ── translations ──────────────────────────────────────────────────────────────
_T: dict[str, dict[str, str]] = {
    "en": {
        # app
        "app_title":            "⚡ China Spot Market Price Cockpit",
        # sidebar
        "lang_label":           "🌐 Language",
        "filters":              "Filters",
        "date_range":           "Date range",
        "provinces":            "Provinces (multi-select)",
        "show_band":            "Show min/max band",
        "filter_bad_data":      "Filter bad data",
        "filter_bad_data_help": "Exclude rows where avg is outside [min, max] bounds — caused by early-Jan PDF format differences",
        "data_caption":         "Data: spot_daily · units: ¥/kWh",
        "select_prov_info":     "Select at least one province in the sidebar.",
        # KPIs
        "latest_date":          "Latest Date",
        "dates_in_db":          "Dates in DB",
        "provinces_kpi":        "Provinces",
        "complete_rows":        "Complete Rows",
        "coverage":             "Coverage",
        # tabs
        "tab_overview":         "Overview",
        "tab_spread":           "DA−RT Spread",
        "tab_heatmap":          "Heatmap",
        "tab_province":         "Province Deep-Dive",
        "tab_dist":             "Distributions",
        "tab_geo":              "Geo Map",
        "tab_interprov":        "Inter-Provincial Flow",
        "tab_mgmt":             "Data Management",
        # overview
        "latest_prices":        "Latest prices",
        "col_province":         "Province",
        "col_province_cn":      "CN Name",
        "col_date":             "Date",
        # spread
        "spread_stats":         "Spread statistics (¥/kWh)",
        "col_mean":             "Mean",
        "col_std":              "Std",
        "col_min":              "Min",
        "col_max":              "Max",
        "col_da_gt_rt":         "DA > RT (%)",
        "col_days":             "Days",
        # heatmap
        "metric_label":         "Metric",
        # province
        "select_province":      "Select province",
        "raw_data":             "raw data",
        # distributions
        "market_label":         "Market",
        "hist_bins":            "Histogram bins",
        "kde_label":            "Overlay KDE curve",
        "both_label":           "Both",
        "desc_stats":           "Descriptive statistics (¥/kWh)",
        "col_n":                "N",
        "col_median":           "Median",
        "col_p10":              "P10",
        "col_p25":              "P25",
        "col_p75":              "P75",
        "col_p90":              "P90",
        # geo
        "avg_by_province":      "Average Prices by Province",
        "geo_color_caption":    "Green = Low (<0.20 ¥/kWh) · Yellow = Medium (0.20–0.30) · Red = High (>0.30)",
        "geo_maps_title":       "Geographic Price Maps",
        "geo_color_scale":      "Color scale: 🟢 **< 0.20 ¥/kWh** (low) · 🟡 **0.20–0.30** (medium) · 🔴 **> 0.30 ¥/kWh** (high)",
        "geo_unavailable":      "Province boundaries unavailable — showing bubble fallback.",
        "da_caption":           "Day-Ahead (DA)",
        "rt_caption":           "Real-Time (RT)",
        "col_avg_da":           "Avg DA (¥/kWh)",
        "col_da_level":         "DA Level",
        "col_avg_rt":           "Avg RT (¥/kWh)",
        "col_rt_level":         "RT Level",
        "col_days_da":          "Days (DA)",
        "col_days_rt":          "Days (RT)",
        "level_low":            "Low",
        "level_medium":         "Medium",
        "level_high":           "High",
        # inter-provincial flow
        "interprov_title":      "Inter-Provincial Spot Trading (省间现货交易)",
        "interprov_no_data":    "No inter-provincial data for the selected period.",
        "interprov_price_trend":"Inter-Provincial Clearing Price Trend (¥/kWh)",
        "interprov_vol_trend":  "Total Inter-Provincial Volume (亿kWh)",
        "direction_export":     "Exporting (送端)",
        "direction_import":     "Importing (受端)",
        "col_direction":        "Direction",
        "col_metric_type":      "Metric",
        "col_share":            "Share (%)",
        "col_price_kwh":        "Price (¥/kWh)",
        "col_price_chg":        "Day-on-day (%)",
        "col_time_period":      "Active period",
        "col_volume_gwh":       "Volume (亿kWh)",
        "col_source":           "Source PDF",
        # province summaries
        "summaries_title":      "Market Summaries",
        "summaries_no_data":    "No summaries available for this period.",
        "summary_label":        "{date}",
        # data management
        "data_mgmt_title":      "Data Management",
        "report_year":          "Report year",
        "mode_label":           "Mode",
        "mode_fill_gaps":       "Fill gaps (ingest missing dates only)",
        "mode_backfill":        "Backfill date range (ingest all PDFs covering the range)",
        "additional_steps":     "Additional steps",
        "chk_interprov":        "Parse 省间现货交易 data",
        "chk_interprov_help":   "Extract inter-provincial trading data and save to staging.spot_interprov_flow",
        "chk_ai":               "Generate AI summaries",
        "chk_ai_help":          "Generate Claude daily market summaries (requires ANTHROPIC_API_KEY)",
        "start_date":           "Start date",
        "end_date":             "End date",
        "col_pdf":              "PDF",
        "col_covers":           "Covers",
        "col_dates_range":      "Dates in range",
        "col_missing":          "Missing from DB",
        "col_partial":          "Partial (DA or RT=0)",
        "col_status":           "Status",
        "status_missing":       "Missing",
        "status_partial":       "Partial",
        "status_ok":            "OK",
        "btn_fill_gaps":        "Backfill {n} PDF(s) with missing dates",
        "btn_reingest":         "Re-ingest all {n} PDF(s) in range",
        "warn_partial":         "{n} PDF(s) have partial data (DA or RT missing). Switch to 'Backfill date range' mode to re-ingest them.",
        "all_present":          "All dates in range are present in DB.",
        "no_pdfs":              "No PDFs found in the selected date range.",
        "prog_starting":        "Starting…",
        "prog_parsing":         "Parsing {fname}…",
        "prog_interprov":       "省间 data: {fname}…",
        "prog_ai":              "AI summary {rdate}…",
        "prog_done":            "Done.",
        "backfill_complete":    "Backfill complete — processed {n} PDF(s).",
        "col_dates":            "Dates",
        "col_rows":             "Rows upserted",
        "col_interprov":        "Interprov rows",
        "col_ai":               "AI summaries",
        "col_error":            "Error",
        # chart labels
        "da_label":             "Day-Ahead (DA)",
        "rt_label":             "Real-Time (RT)",
        "da_avg_label":         "DA avg",
        "rt_avg_label":         "RT avg",
        "price_unit":           "¥/kWh",
        "prob_density":         "Probability density",
        "price_axis":           "Price (¥/kWh)",
        "spread_title":         "DA − RT Spread  (¥/kWh)  |  +ve = DA premium, −ve = RT spike",
        "da_clearing":          "Day-Ahead (DA) Clearing Price  (¥/kWh)",
        "rt_clearing":          "Real-Time (RT) Clearing Price  (¥/kWh)",
        "da_dist_title":        "Day-Ahead (DA) Price Distribution  (¥/kWh)",
        "rt_dist_title":        "Real-Time (RT) Price Distribution  (¥/kWh)",
        "da_violin_title":      "Day-Ahead (DA) — Violin / Box Plot  (¥/kWh)",
        "rt_violin_title":      "Real-Time (RT) — Violin / Box Plot  (¥/kWh)",
        "da_heatmap_title":     "Day-Ahead Average Clearing Price — Province × Date Heatmap",
        "rt_heatmap_title":     "Real-Time Average Clearing Price — Province × Date Heatmap",
        "geo_title_da":         "Day-Ahead (DA) — Average Price by Province (¥/kWh)",
        "geo_title_rt":         "Real-Time (RT) — Average Price by Province (¥/kWh)",
        # geo animation
        "anim_title":           "Monthly RT Price Animation",
        "anim_range":           "Animation period",
        "anim_start_year":      "Start year",
        "anim_start_month":     "Start month",
        "anim_end_year":        "End year",
        "anim_end_month":       "End month",
        "anim_play":            "▶ Play",
        "anim_pause":           "⏸ Pause",
        "anim_speed":           "Seconds per frame",
        "anim_slider":          "Select month",
        "anim_no_data":         "No data for this month.",
        "anim_map_title":       "RT Avg Price — {month}",
        # geo comparison
        "cmp_title":            "Period Comparison",
        "cmp_metric":           "Metric",
        "cmp_period_a":         "Period A",
        "cmp_period_b":         "Period B",
        "cmp_start":            "Start",
        "cmp_end":              "End",
        "cmp_no_data":          "No data for this period.",
        "cmp_map_title":        "{metric} Avg — {start} → {end}",
    },
    "zh": {
        # app
        "app_title":            "⚡ 中国电力现货市场价格驾驶舱",
        # sidebar
        "lang_label":           "🌐 语言",
        "filters":              "筛选条件",
        "date_range":           "日期范围",
        "provinces":            "省份（多选）",
        "show_band":            "显示最大/最小区间",
        "filter_bad_data":      "过滤异常数据",
        "filter_bad_data_help": "排除均值不在最大/最小区间内的行——由1月初PDF格式差异引起",
        "data_caption":         "数据来源：spot_daily · 单位：元/千瓦时",
        "select_prov_info":     "请在侧边栏选择至少一个省份。",
        # KPIs
        "latest_date":          "最新日期",
        "dates_in_db":          "数据库日期数",
        "provinces_kpi":        "省份数",
        "complete_rows":        "完整行数",
        "coverage":             "覆盖率",
        # tabs
        "tab_overview":         "总览",
        "tab_spread":           "日前-实时价差",
        "tab_heatmap":          "热力图",
        "tab_province":         "省份深度分析",
        "tab_dist":             "价格分布",
        "tab_geo":              "地理分布图",
        "tab_interprov":        "省间现货交易",
        "tab_mgmt":             "数据管理",
        # overview
        "latest_prices":        "最新价格",
        "col_province":         "省份",
        "col_province_cn":      "中文名",
        "col_date":             "日期",
        # spread
        "spread_stats":         "价差统计（元/千瓦时）",
        "col_mean":             "均值",
        "col_std":              "标准差",
        "col_min":              "最小值",
        "col_max":              "最大值",
        "col_da_gt_rt":         "日前>实时（%）",
        "col_days":             "天数",
        # heatmap
        "metric_label":         "指标",
        # province
        "select_province":      "选择省份",
        "raw_data":             "原始数据",
        # distributions
        "market_label":         "市场",
        "hist_bins":            "直方图组数",
        "kde_label":            "叠加KDE曲线",
        "both_label":           "两者",
        "desc_stats":           "描述性统计（元/千瓦时）",
        "col_n":                "N",
        "col_median":           "中位数",
        "col_p10":              "P10",
        "col_p25":              "P25",
        "col_p75":              "P75",
        "col_p90":              "P90",
        # geo
        "avg_by_province":      "各省平均价格",
        "geo_color_caption":    "绿色 = 低价（<0.20元/千瓦时）· 黄色 = 中等（0.20–0.30）· 红色 = 高价（>0.30）",
        "geo_maps_title":       "地理价格分布图",
        "geo_color_scale":      "色阶：🟢 **< 0.20 元/千瓦时**（低）· 🟡 **0.20–0.30**（中）· 🔴 **> 0.30 元/千瓦时**（高）",
        "geo_unavailable":      "省级边界数据不可用——显示气泡图替代。",
        "da_caption":           "日前（DA）",
        "rt_caption":           "实时（RT）",
        "col_avg_da":           "日前均价（元/千瓦时）",
        "col_da_level":         "日前价格水平",
        "col_avg_rt":           "实时均价（元/千瓦时）",
        "col_rt_level":         "实时价格水平",
        "col_days_da":          "日前天数",
        "col_days_rt":          "实时天数",
        "level_low":            "低",
        "level_medium":         "中",
        "level_high":           "高",
        # inter-provincial flow
        "interprov_title":      "省间现货交易情况",
        "interprov_no_data":    "所选时段内无省间交易数据。",
        "interprov_price_trend":"省间出清价格走势（元/千瓦时）",
        "interprov_vol_trend":  "省间总交易量（亿kWh）",
        "direction_export":     "送端（出力）",
        "direction_import":     "受端（受入）",
        "col_direction":        "方向",
        "col_metric_type":      "指标类型",
        "col_share":            "占比（%）",
        "col_price_kwh":        "价格（元/千瓦时）",
        "col_price_chg":        "日环比（%）",
        "col_time_period":      "活跃时段",
        "col_volume_gwh":       "电量（亿kWh）",
        "col_source":           "数据来源",
        # province summaries
        "summaries_title":      "市场日报摘要",
        "summaries_no_data":    "所选时段内暂无市场摘要。",
        "summary_label":        "{date}",
        # data management
        "data_mgmt_title":      "数据管理",
        "report_year":          "报告年份",
        "mode_label":           "模式",
        "mode_fill_gaps":       "补全缺口（仅录入缺失日期）",
        "mode_backfill":        "回填日期范围（录入覆盖该范围的所有PDF）",
        "additional_steps":     "附加步骤",
        "chk_interprov":        "解析省间现货交易数据",
        "chk_interprov_help":   "提取省间交易数据并保存至 staging.spot_interprov_flow",
        "chk_ai":               "生成AI摘要",
        "chk_ai_help":          "生成Claude每日市场摘要（需设置 ANTHROPIC_API_KEY）",
        "start_date":           "开始日期",
        "end_date":             "结束日期",
        "col_pdf":              "PDF文件",
        "col_covers":           "覆盖日期",
        "col_dates_range":      "范围内日期数",
        "col_missing":          "数据库缺失",
        "col_partial":          "部分缺失（日前或实时=0）",
        "col_status":           "状态",
        "status_missing":       "缺失",
        "status_partial":       "部分",
        "status_ok":            "正常",
        "btn_fill_gaps":        "回填 {n} 个PDF（含缺失日期）",
        "btn_reingest":         "重新录入范围内全部 {n} 个PDF",
        "warn_partial":         "{n} 个PDF存在部分数据（日前或实时缺失）。切换至「回填日期范围」模式可重新录入。",
        "all_present":          "所选范围内所有日期均已存在于数据库中。",
        "no_pdfs":              "所选日期范围内未找到PDF文件。",
        "prog_starting":        "启动中…",
        "prog_parsing":         "解析 {fname}…",
        "prog_interprov":       "省间数据：{fname}…",
        "prog_ai":              "AI摘要 {rdate}…",
        "prog_done":            "完成。",
        "backfill_complete":    "回填完成——已处理 {n} 个PDF。",
        "col_dates":            "日期",
        "col_rows":             "已写入行数",
        "col_interprov":        "省间行数",
        "col_ai":               "AI摘要数",
        "col_error":            "错误",
        # chart labels
        "da_label":             "日前（DA）",
        "rt_label":             "实时（RT）",
        "da_avg_label":         "日前均价",
        "rt_avg_label":         "实时均价",
        "price_unit":           "元/千瓦时",
        "prob_density":         "概率密度",
        "price_axis":           "价格（元/千瓦时）",
        "spread_title":         "日前−实时价差（元/千瓦时）| 正值=日前溢价，负值=实时峰值",
        "da_clearing":          "日前（DA）出清价格（元/千瓦时）",
        "rt_clearing":          "实时（RT）出清价格（元/千瓦时）",
        "da_dist_title":        "日前（DA）价格分布（元/千瓦时）",
        "rt_dist_title":        "实时（RT）价格分布（元/千瓦时）",
        "da_violin_title":      "日前（DA）— 小提琴/箱线图（元/千瓦时）",
        "rt_violin_title":      "实时（RT）— 小提琴/箱线图（元/千瓦时）",
        "da_heatmap_title":     "日前平均出清价格 — 省份 × 日期热力图",
        "rt_heatmap_title":     "实时平均出清价格 — 省份 × 日期热力图",
        "geo_title_da":         "日前（DA）— 各省平均价格（元/千瓦时）",
        "geo_title_rt":         "实时（RT）— 各省平均价格（元/千瓦时）",
        # geo animation
        "anim_title":           "各月实时价格动画",
        "anim_range":           "动画时段",
        "anim_start_year":      "起始年",
        "anim_start_month":     "起始月",
        "anim_end_year":        "结束年",
        "anim_end_month":       "结束月",
        "anim_play":            "▶ 播放",
        "anim_pause":           "⏸ 暂停",
        "anim_speed":           "每帧秒数",
        "anim_slider":          "选择月份",
        "anim_no_data":         "该月无数据。",
        "anim_map_title":       "实时均价 — {month}",
        # geo comparison
        "cmp_title":            "时段对比",
        "cmp_metric":           "指标",
        "cmp_period_a":         "时段 A",
        "cmp_period_b":         "时段 B",
        "cmp_start":            "开始日期",
        "cmp_end":              "结束日期",
        "cmp_no_data":          "该时段无数据。",
        "cmp_map_title":        "{metric} 均价 — {start} → {end}",
    },
}


def _t(key: str, **kwargs) -> str:
    """Return translated string for the current language selection."""
    lang = "zh" if st.session_state.get("lang_radio") == "中文" else "en"
    s = _T[lang].get(key, _T["en"].get(key, key))
    return s.format(**kwargs) if kwargs else s


# ── page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Spot Market Cockpit",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── DB connection ─────────────────────────────────────────────────────────────
@st.cache_resource
def _get_conn():
    url = (
        os.environ.get("PGURL")
        or os.environ.get("DATABASE_URL")
        or os.environ.get("DB_URL")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    return psycopg2.connect(url, keepalives=1, keepalives_idle=60,
                            keepalives_interval=10, keepalives_count=5)

def _conn():
    conn = _get_conn()
    try:
        conn.cursor().execute("SELECT 1")
    except Exception:
        _get_conn.clear()
        conn = _get_conn()
    return conn

# ── data quality filter ───────────────────────────────────────────────────────
def _apply_quality_filter(df: pd.DataFrame) -> pd.DataFrame:
    mask = pd.Series(True, index=df.index)
    for m in ("da", "rt"):
        avg, mx, mn = f"{m}_avg", f"{m}_max", f"{m}_min"
        bad_lo = df[avg].notna() & df[mn].notna() & (df[avg] < df[mn] - 0.001)
        bad_hi = df[avg].notna() & df[mx].notna() & (df[avg] > df[mx] + 0.001)
        bad_range = df[avg].notna() & ((df[avg] < -0.5) | (df[avg] > 2.0))
        mask &= ~(bad_lo | bad_hi | bad_range)
    df = df[mask].copy()
    for m in ("da", "rt"):
        for col in (f"{m}_max", f"{m}_min"):
            df.loc[df[col].notna() & ((df[col] > 2.0) | (df[col] < -1.0)), col] = None
    return df

# ── data loaders ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=120, show_spinner=False)
def load_all(start: date, end: date, quality_filter: bool) -> pd.DataFrame:
    q = """
        SELECT report_date::date AS report_date,
               province_en, province_cn,
               da_avg, da_max, da_min,
               rt_avg, rt_max, rt_min
        FROM spot_daily
        WHERE report_date BETWEEN %s AND %s
          AND (da_avg IS NOT NULL OR rt_avg IS NOT NULL)
        ORDER BY report_date, province_en
    """
    df = pd.read_sql(q, _conn(), params=(start, end), parse_dates=["report_date"])
    for c in ["da_avg","da_max","da_min","rt_avg","rt_max","rt_min"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if quality_filter:
        df = _apply_quality_filter(df)
    return df


@st.cache_data(ttl=120, show_spinner=False)
def load_provinces() -> list[str]:
    cur = _conn().cursor()
    cur.execute(
        "SELECT DISTINCT province_en FROM spot_daily "
        "WHERE report_date >= '2026-01-01' ORDER BY 1"
    )
    return [r[0] for r in cur.fetchall()]


@st.cache_data(ttl=60, show_spinner=False)
def load_kpis(quality_filter: bool) -> dict:
    cur = _conn().cursor()
    cur.execute("""
        SELECT
            MAX(report_date)                              AS latest_date,
            COUNT(DISTINCT report_date)                   AS total_dates,
            COUNT(DISTINCT province_en)                   AS total_provinces,
            SUM(CASE WHEN da_avg IS NOT NULL AND rt_avg IS NOT NULL THEN 1 ELSE 0 END) AS complete_rows,
            COUNT(*)                                      AS total_rows
        FROM spot_daily
        WHERE report_date >= '2026-01-01'
    """)
    r = cur.fetchone()
    return {
        "latest_date":     r[0],
        "total_dates":     r[1],
        "total_provinces": r[2],
        "complete_rows":   r[3],
        "total_rows":      r[4],
    }


@st.cache_data(ttl=60, show_spinner=False)
def load_interprov(start: date, end: date) -> pd.DataFrame:
    try:
        cur = _conn().cursor()
        cur.execute("""
            SELECT report_date::date, direction, metric_type,
                   province_cn, province_share,
                   price_yuan_kwh, price_chg_pct,
                   time_period, total_vol_100gwh, source_pdf
            FROM staging.spot_interprov_flow
            WHERE report_date BETWEEN %s AND %s
            ORDER BY report_date, direction, metric_type
        """, (start, end))
        cols = [d[0] for d in cur.description]
        df = pd.DataFrame(cur.fetchall(), columns=cols)
        for c in ["price_yuan_kwh", "price_chg_pct", "total_vol_100gwh", "province_share"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=60, show_spinner=False)
def load_summaries(start: date, end: date) -> pd.DataFrame:
    try:
        cur = _conn().cursor()
        cur.execute("""
            SELECT report_date::date, summary_text, model, source_pdf
            FROM staging.spot_report_summaries
            WHERE report_date BETWEEN %s AND %s
            ORDER BY report_date DESC
        """, (start, end))
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)
    except Exception:
        return pd.DataFrame()


# ── colour helpers ────────────────────────────────────────────────────────────
_PALETTE = px.colors.qualitative.Plotly + px.colors.qualitative.Dark24

def _prov_colour(provinces: list[str]) -> dict[str, str]:
    return {p: _PALETTE[i % len(_PALETTE)] for i, p in enumerate(sorted(provinces))}


# ── chart builders ────────────────────────────────────────────────────────────
def chart_timeseries(df: pd.DataFrame, provinces: list[str],
                     metric: str, show_band: bool) -> go.Figure:
    fig = go.Figure()
    colours = _prov_colour(provinces)
    avg_col, max_col, min_col = f"{metric}_avg", f"{metric}_max", f"{metric}_min"

    for prov in sorted(provinces):
        sub = df[df["province_en"] == prov].sort_values("report_date")
        if sub.empty or sub[avg_col].isna().all():
            continue
        col = colours[prov]

        sub_band = sub[sub[avg_col].notna()]
        if show_band and sub_band[max_col].notna().any():
            x_band = pd.concat([sub_band["report_date"], sub_band["report_date"].iloc[::-1]])
            y_band = pd.concat([sub_band[max_col], sub_band[min_col].iloc[::-1]])
            fig.add_trace(go.Scatter(
                x=x_band, y=y_band,
                fill="toself", fillcolor=col, opacity=0.10,
                line=dict(width=0), showlegend=False, hoverinfo="skip",
            ))

        fig.add_trace(go.Scatter(
            x=sub["report_date"], y=sub[avg_col],
            name=prov, mode="lines+markers",
            line=dict(color=col, width=1.8), marker=dict(size=4),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.4f} ¥/kWh<extra>" + prov + "</extra>",
        ))

    title_key = "da_clearing" if metric == "da" else "rt_clearing"
    fig.update_layout(
        height=430,
        title=dict(text=_t(title_key), font=dict(size=14)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11)),
        xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
    )
    return fig


def chart_da_rt_overlay(df: pd.DataFrame, province: str) -> go.Figure:
    sub = df[df["province_en"] == province].sort_values("report_date")
    fig = go.Figure()

    for metric, label_key, colour in [
        ("da", "da_avg_label", "#1f77b4"),
        ("rt", "rt_avg_label", "#ff7f0e"),
    ]:
        avg_col, max_col, min_col = f"{metric}_avg", f"{metric}_max", f"{metric}_min"
        if sub[avg_col].isna().all():
            continue
        label = _t(label_key)
        if sub[max_col].notna().any():
            fig.add_trace(go.Scatter(
                x=pd.concat([sub["report_date"], sub["report_date"].iloc[::-1]]),
                y=pd.concat([sub[max_col], sub[min_col].iloc[::-1]]),
                fill="toself", fillcolor=colour, opacity=0.12,
                line=dict(width=0), showlegend=False, hoverinfo="skip",
            ))
        fig.add_trace(go.Scatter(
            x=sub["report_date"], y=sub[avg_col],
            name=label, mode="lines+markers",
            line=dict(color=colour, width=2), marker=dict(size=4),
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.4f} ¥/kWh<extra>" + label + "</extra>",
        ))

    fig.update_layout(
        height=390,
        title=dict(text=f"{province} — {_t('da_avg_label')} vs {_t('rt_avg_label')}  ({_t('price_unit')})",
                   font=dict(size=13)),
        margin=dict(l=10, r=10, t=45, b=60),
        legend=dict(orientation="h", yanchor="top", y=-0.15,
                    xanchor="center", x=0.5, font=dict(size=11)),
        xaxis=dict(showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
    )
    return fig


def chart_spread(df: pd.DataFrame, provinces: list[str]) -> go.Figure:
    fig = go.Figure()
    colours = _prov_colour(provinces)

    for prov in sorted(provinces):
        sub = df[df["province_en"] == prov].dropna(subset=["da_avg", "rt_avg"]).copy()
        if sub.empty:
            continue
        sub["spread"] = sub["da_avg"] - sub["rt_avg"]
        fig.add_trace(go.Bar(
            x=sub["report_date"], y=sub["spread"],
            name=prov, marker_color=colours[prov], opacity=0.8,
            hovertemplate="%{x|%Y-%m-%d}<br>Spread: %{y:.4f} ¥/kWh<extra>" + prov + "</extra>",
        ))

    fig.add_hline(y=0, line_width=1, line_color="black", opacity=0.5)
    fig.update_layout(
        height=360, barmode="group",
        title=dict(text=_t("spread_title"), font=dict(size=13)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.22,
                    xanchor="center", x=0.5, font=dict(size=11)),
        xaxis=dict(showgrid=False),
        yaxis=dict(showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
        hovermode="x unified",
    )
    return fig


def chart_heatmap(df: pd.DataFrame, metric: str) -> go.Figure:
    avg_col = f"{metric}_avg"
    pivot = (
        df[["report_date", "province_en", avg_col]]
        .dropna(subset=[avg_col])
        .pivot_table(index="province_en", columns="report_date", values=avg_col)
    )
    if pivot.empty:
        return go.Figure()

    title_key = "da_heatmap_title" if metric == "da" else "rt_heatmap_title"
    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=pivot.columns.strftime("%m-%d"),
        y=pivot.index.tolist(),
        colorscale="RdYlGn_r",
        colorbar=dict(title=_t("price_unit"), thickness=12),
        hoverongaps=False,
        hovertemplate="Date: %{x}<br>Province: %{y}<br>Price: %{z:.4f} ¥/kWh<extra></extra>",
    ))
    fig.update_layout(
        height=max(350, len(pivot) * 24),
        title=dict(text=_t(title_key), font=dict(size=13)),
        margin=dict(l=120, r=20, t=45, b=60),
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        yaxis=dict(tickfont=dict(size=11)),
    )
    return fig


def chart_distributions(df: pd.DataFrame, provinces: list[str],
                         metric: str, nbins: int, show_kde: bool) -> go.Figure:
    avg_col = f"{metric}_avg"
    colours = _prov_colour(provinces)
    fig = go.Figure()

    for prov in sorted(provinces):
        vals = df[df["province_en"] == prov][avg_col].dropna().values
        if len(vals) < 2:
            continue
        col = colours[prov]

        fig.add_trace(go.Histogram(
            x=vals,
            name=prov,
            nbinsx=nbins,
            marker_color=col,
            opacity=0.45,
            histnorm="probability density",
            hovertemplate="Price: %{x:.4f} ¥/kWh<br>Density: %{y:.3f}<extra>" + prov + "</extra>",
        ))

        if show_kde and len(vals) >= 5:
            std = vals.std()
            if std > 0:
                bw = 1.06 * std * len(vals) ** (-0.2)
                x_grid = np.linspace(vals.min() - 2 * bw, vals.max() + 2 * bw, 300)
                kde = np.zeros_like(x_grid)
                for v in vals:
                    kde += np.exp(-0.5 * ((x_grid - v) / bw) ** 2)
                kde /= len(vals) * bw * np.sqrt(2 * np.pi)
                fig.add_trace(go.Scatter(
                    x=x_grid, y=kde,
                    name=f"{prov} KDE",
                    mode="lines",
                    line=dict(color=col, width=2, dash="solid"),
                    showlegend=False,
                    hovertemplate="%{x:.4f} ¥/kWh<br>KDE: %{y:.3f}<extra>" + prov + "</extra>",
                ))

    title_key = "da_dist_title" if metric == "da" else "rt_dist_title"
    fig.update_layout(
        height=430,
        barmode="overlay",
        title=dict(text=_t(title_key), font=dict(size=14)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11)),
        xaxis=dict(title=_t("price_axis"), showgrid=True, gridcolor="#f0f0f0"),
        yaxis=dict(title=_t("prob_density"), showgrid=True, gridcolor="#f0f0f0"),
        plot_bgcolor="white", paper_bgcolor="white",
    )
    return fig


def chart_violin(df: pd.DataFrame, provinces: list[str], metric: str) -> go.Figure:
    avg_col = f"{metric}_avg"
    colours = _prov_colour(provinces)
    fig = go.Figure()

    for prov in sorted(provinces):
        vals = df[df["province_en"] == prov][avg_col].dropna().values
        if len(vals) < 3:
            continue
        fig.add_trace(go.Violin(
            y=vals, name=prov,
            box_visible=True,
            meanline_visible=True,
            fillcolor=colours[prov],
            opacity=0.65,
            line_color=colours[prov],
            hoverinfo="y+name",
        ))

    title_key = "da_violin_title" if metric == "da" else "rt_violin_title"
    fig.update_layout(
        height=430,
        title=dict(text=_t(title_key), font=dict(size=14)),
        margin=dict(l=10, r=10, t=45, b=90),
        legend=dict(orientation="h", yanchor="top", y=-0.18,
                    xanchor="center", x=0.5, font=dict(size=11)),
        yaxis=dict(title=_t("price_axis"), showgrid=True, gridcolor="#f0f0f0", tickformat=".3f"),
        plot_bgcolor="white", paper_bgcolor="white",
        violinmode="group",
    )
    return fig


def _dist_stats(df: pd.DataFrame, provinces: list[str], metric: str) -> pd.DataFrame:
    avg_col = f"{metric}_avg"
    rows = []
    for prov in sorted(provinces):
        vals = df[df["province_en"] == prov][avg_col].dropna()
        if vals.empty:
            continue
        rows.append({
            _t("col_province"): prov,
            _t("col_n"):        len(vals),
            _t("col_mean"):     f"{vals.mean():.4f}",
            _t("col_median"):   f"{vals.median():.4f}",
            _t("col_std"):      f"{vals.std():.4f}",
            _t("col_p10"):      f"{vals.quantile(0.10):.4f}",
            _t("col_p25"):      f"{vals.quantile(0.25):.4f}",
            _t("col_p75"):      f"{vals.quantile(0.75):.4f}",
            _t("col_p90"):      f"{vals.quantile(0.90):.4f}",
            _t("col_min"):      f"{vals.min():.4f}",
            _t("col_max"):      f"{vals.max():.4f}",
        })
    return pd.DataFrame(rows)


# ── Geo map helpers ───────────────────────────────────────────────────────────

_PROV_ADCODE: dict[str, str] = {
    "Beijing":      "110000", "Tianjin":     "120000",
    "Hebei":        "130000", "Hebei-North": "130000", "Hebei-South": "130000",
    "Shanxi":       "140000",
    "Mengxi":       "150000", "Mengdong":    "150000",
    "Liaoning":     "210000", "Jilin":       "220000", "Heilongjiang": "230000",
    "Shanghai":     "310000", "Jiangsu":     "320000", "Zhejiang":     "330000",
    "Anhui":        "340000", "Fujian":      "350000", "Jiangxi":      "360000",
    "Shandong":     "370000", "Henan":       "410000", "Hubei":        "420000",
    "Hunan":        "430000", "Guangdong":   "440000", "Guangxi":      "450000",
    "Hainan":       "460000", "Chongqing":   "500000", "Sichuan":      "510000",
    "Guizhou":      "520000", "Yunnan":      "530000",
    "Shaanxi":      "610000", "Gansu":       "620000", "Qinghai":      "630000",
    "Ningxia":      "640000", "Xinjiang":    "650000",
}

_PROV_CENTROIDS: dict[str, tuple[float, float]] = {
    "110000": (39.90, 116.40), "120000": (39.13, 117.20),
    "130000": (38.04, 114.47), "140000": (37.87, 112.56),
    "150000": (44.09, 113.09), "210000": (41.80, 123.43),
    "220000": (43.89, 125.32), "230000": (47.85, 127.57),
    "310000": (31.23, 121.47), "320000": (32.06, 119.59),
    "330000": (30.27, 120.15), "340000": (31.86, 117.29),
    "350000": (26.10, 118.31), "360000": (27.62, 115.70),
    "370000": (36.67, 117.02), "410000": (34.76, 113.75),
    "420000": (30.60, 114.30), "430000": (28.23, 112.94),
    "440000": (23.37, 113.50), "450000": (23.73, 108.38),
    "460000": (20.02, 110.35), "500000": (29.56, 106.54),
    "510000": (30.57, 103.99), "520000": (26.82, 106.83),
    "530000": (25.05, 101.71), "610000": (34.27, 108.95),
    "620000": (36.06, 103.83), "630000": (36.62, 101.74),
    "640000": (38.47, 106.26), "650000": (41.17,  85.29),
}

_ADCODE_LABEL: dict[str, str] = {
    "110000": "Beijing",        "120000": "Tianjin",
    "130000": "Hebei",          "140000": "Shanxi",
    "150000": "Inner Mongolia", "210000": "Liaoning",
    "220000": "Jilin",          "230000": "Heilongjiang",
    "310000": "Shanghai",       "320000": "Jiangsu",
    "330000": "Zhejiang",       "340000": "Anhui",
    "350000": "Fujian",         "360000": "Jiangxi",
    "370000": "Shandong",       "410000": "Henan",
    "420000": "Hubei",          "430000": "Hunan",
    "440000": "Guangdong",      "450000": "Guangxi",
    "460000": "Hainan",         "500000": "Chongqing",
    "510000": "Sichuan",        "520000": "Guizhou",
    "530000": "Yunnan",         "610000": "Shaanxi",
    "620000": "Gansu",          "630000": "Qinghai",
    "640000": "Ningxia",        "650000": "Xinjiang",
}

_LOW_PRICE  = 0.20
_HIGH_PRICE = 0.30

_GEO_FILE = Path(__file__).parent / "data" / "china_provinces.geojson"

_GEO_COLORSCALE = [
    [0.00, "#00aa44"],
    [0.40, "#ffe000"],
    [0.60, "#ff6600"],
    [1.00, "#cc0000"],
]
_GEO_ZMIN, _GEO_ZMAX = 0.0, 0.5


def _price_level(v: float | None) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "—"
    if v < _LOW_PRICE:
        return _t("level_low")
    if v <= _HIGH_PRICE:
        return _t("level_medium")
    return _t("level_high")


def _level_bg(level: str) -> str:
    return {
        "Low": "#d4edda", "低": "#d4edda",
        "Medium": "#fff3cd", "中": "#fff3cd",
        "High": "#ffe0e0", "高": "#ffe0e0",
    }.get(level, "")


@st.cache_data(ttl=None, show_spinner=False)
def _load_china_geojson() -> tuple[dict | None, str | None]:
    if _GEO_FILE.exists():
        try:
            return json.loads(_GEO_FILE.read_text(encoding="utf-8")), None
        except Exception:
            pass

    try:
        resp = requests.get(
            "https://geo.datav.aliyun.com/areas_v3/bound/100000_full.json",
            timeout=20,
        )
        resp.raise_for_status()
        gj = resp.json()
        if len(gj.get("features", [])) < 10:
            return None, "GeoJSON has too few features — unexpected format"
        _GEO_FILE.parent.mkdir(parents=True, exist_ok=True)
        _GEO_FILE.write_text(json.dumps(gj), encoding="utf-8")
        return gj, None
    except Exception as exc:
        return None, str(exc)


def _geo_agg(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    avg_col = f"{metric}_avg"
    df2 = df.copy()
    df2["adcode"] = df2["province_en"].map(_PROV_ADCODE)
    df2 = df2.dropna(subset=["adcode", avg_col])
    if df2.empty:
        return pd.DataFrame(columns=["adcode", "avg", "label", "price_str"])
    agg = df2.groupby("adcode", as_index=False)[avg_col].mean()
    agg.columns = ["adcode", "avg"]
    agg["label"]     = agg["adcode"].map(_ADCODE_LABEL)
    agg["price_str"] = agg["avg"].map(lambda v: f"{v:.2f}")
    return agg


def _make_china_cmap() -> mcolors.LinearSegmentedColormap:
    stops = [(pos, mcolors.to_rgb(hex_col)) for pos, hex_col in _GEO_COLORSCALE]
    return mcolors.LinearSegmentedColormap.from_list("china_price", stops)


def chart_geo_map(df: pd.DataFrame, metric: str, geojson: dict | None,
                  title: str | None = None) -> plt.Figure:
    agg = _geo_agg(df, metric)
    title_key = "geo_title_da" if metric == "da" else "geo_title_rt"
    display_title = title if title is not None else _t(title_key)

    # Use a CJK-capable font for Chinese labels when one is available
    _lang = st.session_state.get("lang_radio", "English")
    _rc_font = ({"font.family": _CJK_FONT} if _lang == "中文" and _CJK_FONT else {})

    cmap = _make_china_cmap()
    norm = mcolors.Normalize(vmin=_GEO_ZMIN, vmax=_GEO_ZMAX)

    price_map: dict[int, float] = {}
    if not agg.empty:
        for _, row in agg.iterrows():
            try:
                price_map[int(row["adcode"])] = float(row["avg"])
            except (ValueError, TypeError):
                pass

    with plt.rc_context(_rc_font):
        fig, ax = plt.subplots(figsize=(9, 6), facecolor="white")
        ax.set_facecolor("#b8d4f0")

        if geojson:
            for feat in geojson.get("features", []):
                adcode_int = feat.get("properties", {}).get("adcode")
                price = price_map.get(adcode_int)
                fc = cmap(norm(price)) if price is not None else "#d0d0d0"

                geom = feat.get("geometry", {})
                rings: list = []
                if geom.get("type") == "Polygon":
                    rings = [geom["coordinates"][0]]
                elif geom.get("type") == "MultiPolygon":
                    rings = [p[0] for p in geom["coordinates"]]

                for ring in rings:
                    coords = np.array(ring)
                    ax.add_patch(MplPolygon(
                        coords, closed=True,
                        facecolor=fc, edgecolor="white", linewidth=0.8,
                    ))

        if not agg.empty:
            for _, row in agg.iterrows():
                coord = _PROV_CENTROIDS.get(row["adcode"])
                if coord:
                    lat, lon = coord
                    ax.text(lon, lat, row["price_str"], ha="center", va="center",
                            fontsize=7, fontweight="bold", color="black")

        ax.set_xlim(72, 137)
        ax.set_ylim(16, 54)
        ax.set_aspect("equal")
        ax.axis("off")

        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = fig.colorbar(sm, ax=ax, orientation="vertical", fraction=0.025, pad=0.01, aspect=25)
        cbar.set_label(_t("price_unit"), fontsize=9)
        cbar.set_ticks([0.0, 0.1, 0.2, 0.3, 0.4, 0.5])
        cbar.set_ticklabels(["0.0", "0.1", "0.2", "0.3", "0.4", "0.5+"])
        cbar.ax.tick_params(labelsize=8)

        ax.set_title(display_title, fontsize=11, pad=10)
        plt.tight_layout(pad=0.5)
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# LAYOUT
# ─────────────────────────────────────────────────────────────────────────────

st.title(_t("app_title"))

# ── KPI strip ────────────────────────────────────────────────────────────────
with st.spinner("Loading…"):
    try:
        provinces_all = load_provinces()
    except Exception as e:
        st.error(f"DB connection failed: {e}")
        st.stop()

# ── Sidebar controls ──────────────────────────────────────────────────────────
with st.sidebar:
    # Language toggle — must come first so _t() works for everything below
    st.radio("🌐", ["English", "中文"], horizontal=True,
             key="lang_radio", label_visibility="collapsed")

    st.header(_t("filters"))

    _today = date.today()
    date_range = st.date_input(
        _t("date_range"),
        value=(date(2026, 1, 1), _today),
        min_value=date(2024, 1, 1),
        max_value=_today,
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        d_start, d_end = date_range
    else:
        d_start, d_end = date(2026, 1, 1), _today

    prov_options = sorted(provinces_all)
    default_provs = [p for p in ["Shandong", "Shanxi", "Mengxi", "Guangdong", "Sichuan"]
                     if p in prov_options] or prov_options[:5]
    selected_provs = st.multiselect(
        _t("provinces"),
        prov_options,
        default=default_provs,
        help="Select one or more provinces to compare",
    )

    show_band = st.checkbox(_t("show_band"), value=True)
    quality_filter = st.checkbox(
        _t("filter_bad_data"),
        value=True,
        help=_t("filter_bad_data_help"),
    )

    st.divider()
    st.caption(_t("data_caption"))

if not selected_provs:
    st.info(_t("select_prov_info"))
    st.stop()

# ── Load data ─────────────────────────────────────────────────────────────────
kpis = load_kpis(quality_filter)
df = load_all(d_start, d_end, quality_filter)
df_sel = df[df["province_en"].isin(selected_provs)]

# ── KPI strip ─────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric(_t("latest_date"),   str(kpis["latest_date"]) if kpis["latest_date"] else "—")
k2.metric(_t("dates_in_db"),   kpis["total_dates"])
k3.metric(_t("provinces_kpi"), kpis["total_provinces"])
k4.metric(_t("complete_rows"), kpis["complete_rows"],
          delta=f"/ {kpis['total_rows']} total", delta_color="off")
k5.metric(_t("coverage"),
          f"{100*kpis['complete_rows']/kpis['total_rows']:.0f}%" if kpis["total_rows"] else "—")

if quality_filter:
    n_bad = load_kpis(False)["total_rows"] - kpis["total_rows"]
    if n_bad > 0:
        st.caption(f"ℹ️ {n_bad} rows with invalid avg/min/max values hidden (toggle '{_t('filter_bad_data')}' in sidebar to include)")

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────
tab_overview, tab_spread, tab_heatmap, tab_province, tab_dist, tab_geo, tab_interprov, tab_mgmt = st.tabs([
    _t("tab_overview"), _t("tab_spread"), _t("tab_heatmap"),
    _t("tab_province"), _t("tab_dist"), _t("tab_geo"),
    _t("tab_interprov"), _t("tab_mgmt"),
])

# ── Tab 1: Overview ───────────────────────────────────────────────────────────
with tab_overview:
    col_da, col_rt = st.columns(2)
    with col_da:
        st.plotly_chart(chart_timeseries(df_sel, selected_provs, "da", show_band),
                        use_container_width=True)
    with col_rt:
        st.plotly_chart(chart_timeseries(df_sel, selected_provs, "rt", show_band),
                        use_container_width=True)

    st.subheader(_t("latest_prices"))
    latest = (
        df[df["province_en"].isin(selected_provs)]
        .sort_values("report_date", ascending=False)
        .groupby("province_en")
        .first()
        .reset_index()
        [["province_en", "province_cn", "report_date",
          "da_avg", "da_max", "da_min",
          "rt_avg", "rt_max", "rt_min"]]
        .rename(columns={
            "province_en": _t("col_province"),
            "province_cn": _t("col_province_cn"),
            "report_date": _t("col_date"),
        })
        .sort_values(_t("col_province"))
    )
    latest[_t("col_date")] = pd.to_datetime(latest[_t("col_date")]).dt.date
    for c in ["da_avg","da_max","da_min","rt_avg","rt_max","rt_min"]:
        latest[c] = latest[c].map(lambda v: f"{v:.4f}" if pd.notna(v) else "—")
    st.dataframe(latest, use_container_width=True, hide_index=True)

# ── Tab 2: Spread ─────────────────────────────────────────────────────────────
with tab_spread:
    st.plotly_chart(chart_spread(df_sel, selected_provs), use_container_width=True)

    st.subheader(_t("spread_stats"))
    spread_rows = []
    for prov in sorted(selected_provs):
        sub = df_sel[df_sel["province_en"] == prov].dropna(subset=["da_avg", "rt_avg"])
        if sub.empty:
            continue
        s = sub["da_avg"] - sub["rt_avg"]
        spread_rows.append({
            _t("col_province"): prov,
            _t("col_mean"):     f"{s.mean():.4f}",
            _t("col_std"):      f"{s.std():.4f}",
            _t("col_min"):      f"{s.min():.4f}",
            _t("col_max"):      f"{s.max():.4f}",
            _t("col_da_gt_rt"): f"{(s > 0).mean()*100:.0f}%",
            _t("col_days"):     len(s),
        })
    if spread_rows:
        st.dataframe(pd.DataFrame(spread_rows), use_container_width=True, hide_index=True)

# ── Tab 3: Heatmap ────────────────────────────────────────────────────────────
with tab_heatmap:
    hm_metric = st.radio(_t("metric_label"), ["DA", "RT"], horizontal=True)
    fig_hm = chart_heatmap(df[df["province_en"].isin(selected_provs)], hm_metric.lower())
    if fig_hm.data:
        st.plotly_chart(fig_hm, use_container_width=True)
    else:
        st.info("No data for selected range / provinces.")

# ── Tab 4: Province Deep-Dive ────────────────────────────────────────────────
with tab_province:
    dive_prov = st.selectbox(_t("select_province"), sorted(selected_provs))
    if dive_prov:
        st.plotly_chart(chart_da_rt_overlay(df_sel, dive_prov), use_container_width=True)

        sub = df_sel[df_sel["province_en"] == dive_prov].sort_values("report_date").copy()
        sub["report_date"] = pd.to_datetime(sub["report_date"]).dt.date
        st.subheader(f"{dive_prov} — {_t('raw_data')}")
        st.dataframe(
            sub[["report_date","da_avg","da_max","da_min","rt_avg","rt_max","rt_min"]]
            .rename(columns={"report_date": _t("col_date")})
            .style.format(
                {c: "{:.4f}" for c in ["da_avg","da_max","da_min","rt_avg","rt_max","rt_min"]},
                na_rep="—",
            ),
            use_container_width=True, hide_index=True,
        )

        # ── Market summaries for the selected period ──────────────────────────
        st.divider()
        st.subheader(_t("summaries_title"))
        df_summ = load_summaries(d_start, d_end)
        # Filter to dates where this province has data
        prov_dates = set(sub["report_date"].astype(str))
        if df_summ.empty or "report_date" not in df_summ.columns:
            df_summ_prov = pd.DataFrame()
        else:
            df_summ_prov = df_summ[df_summ["report_date"].astype(str).isin(prov_dates)]
        if df_summ_prov.empty:
            st.info(_t("summaries_no_data"))
        else:
            for _, row in df_summ_prov.iterrows():
                with st.expander(_t("summary_label", date=str(row["report_date"]))):
                    st.markdown(row["summary_text"])
                    st.caption(f"{row['model']} · {row['source_pdf']}")

# ── Tab 5: Distributions ─────────────────────────────────────────────────────
with tab_dist:
    dc1, dc2, dc3 = st.columns([2, 1, 1])
    with dc1:
        both_opt = _t("both_label")
        dist_metric = st.radio(_t("market_label"), ["DA", "RT", both_opt],
                               horizontal=True, key="dist_metric")
    with dc2:
        nbins = st.slider(_t("hist_bins"), 10, 80, 30, key="dist_bins")
    with dc3:
        show_kde = st.checkbox(_t("kde_label"), value=True, key="dist_kde")

    metrics_to_show = ["da", "rt"] if dist_metric == both_opt else [dist_metric.lower()]

    for m in metrics_to_show:
        st.plotly_chart(
            chart_distributions(df_sel, selected_provs, m, nbins, show_kde),
            use_container_width=True,
        )
        st.plotly_chart(
            chart_violin(df_sel, selected_provs, m),
            use_container_width=True,
        )
        st.subheader(f"{'DA' if m == 'da' else 'RT'} — {_t('desc_stats')}")
        stats_df = _dist_stats(df_sel, selected_provs, m)
        if not stats_df.empty:
            st.dataframe(stats_df, use_container_width=True, hide_index=True)
        if dist_metric == both_opt and m == "da":
            st.divider()

# ── Tab 6: Geo Map ────────────────────────────────────────────────────────────
with tab_geo:
    st.caption(f"{_t('avg_by_province')} · **{d_start}** → **{d_end}**")

    df_geo = load_all(d_start, d_end, quality_filter)

    if df_geo.empty:
        st.info("No data for selected period.")
    else:
        st.subheader(_t("avg_by_province"))
        st.caption(_t("geo_color_caption"))

        tbl_rows = []
        for prov_en in sorted(df_geo["province_en"].unique()):
            sub = df_geo[df_geo["province_en"] == prov_en]
            da_vals = sub["da_avg"].dropna()
            rt_vals = sub["rt_avg"].dropna()
            da_avg  = da_vals.mean() if not da_vals.empty else None
            rt_avg  = rt_vals.mean() if not rt_vals.empty else None
            tbl_rows.append({
                _t("col_province"):  prov_en,
                _t("col_avg_da"):    round(da_avg, 4) if da_avg is not None else None,
                _t("col_da_level"):  _price_level(da_avg),
                _t("col_avg_rt"):    round(rt_avg, 4) if rt_avg is not None else None,
                _t("col_rt_level"):  _price_level(rt_avg),
                _t("col_days_da"):   len(da_vals),
                _t("col_days_rt"):   len(rt_vals),
            })

        tbl_df = pd.DataFrame(tbl_rows)
        da_level_col = _t("col_da_level")
        rt_level_col = _t("col_rt_level")
        avg_da_col   = _t("col_avg_da")
        avg_rt_col   = _t("col_avg_rt")

        def _style_level(col: pd.Series) -> list[str]:
            return [f"background-color: {_level_bg(v)}" for v in col]

        styled = (
            tbl_df.style
            .apply(_style_level, subset=[da_level_col])
            .apply(_style_level, subset=[rt_level_col])
            .format({
                avg_da_col: lambda v: f"{v:.4f}" if pd.notna(v) else "—",
                avg_rt_col: lambda v: f"{v:.4f}" if pd.notna(v) else "—",
            })
        )
        st.dataframe(styled, use_container_width=True, hide_index=True)

        st.divider()

        st.subheader(_t("geo_maps_title"))
        st.caption(_t("geo_color_scale"))

        _geojson, _geo_err = _load_china_geojson()
        if _geo_err:
            st.warning(f"{_t('geo_unavailable')} ({_geo_err})")

        col_map_da, col_map_rt = st.columns(2)
        with col_map_da:
            st.caption(f"**{_t('da_caption')}** · {d_start} → {d_end}")
            fig_da = chart_geo_map(df_geo, "da", _geojson)
            st.pyplot(fig_da, use_container_width=True)
            plt.close(fig_da)
        with col_map_rt:
            st.caption(f"**{_t('rt_caption')}** · {d_start} → {d_end}")
            fig_rt = chart_geo_map(df_geo, "rt", _geojson)
            st.pyplot(fig_rt, use_container_width=True)
            plt.close(fig_rt)

        # ── Section: Monthly RT Animation ────────────────────────────────────
        st.divider()
        st.subheader(_t("anim_title"))

        _all_years = list(range(2020, date.today().year + 2))
        anim_c1, anim_c2, anim_c3, anim_c4 = st.columns(4)
        with anim_c1:
            _anim_sy = st.selectbox(
                _t("anim_start_year"), _all_years,
                index=_all_years.index(min(d_start.year, _all_years[-1])),
                key="anim_sy",
            )
        with anim_c2:
            _anim_sm = st.selectbox(
                _t("anim_start_month"), list(range(1, 13)),
                index=d_start.month - 1, key="anim_sm",
                format_func=lambda m: f"{m:02d}",
            )
        with anim_c3:
            _anim_ey = st.selectbox(
                _t("anim_end_year"), _all_years,
                index=_all_years.index(min(d_end.year, _all_years[-1])),
                key="anim_ey",
            )
        with anim_c4:
            _anim_em = st.selectbox(
                _t("anim_end_month"), list(range(1, 13)),
                index=d_end.month - 1, key="anim_em",
                format_func=lambda m: f"{m:02d}",
            )

        _anim_period_start = date(_anim_sy, _anim_sm, 1)
        _anim_period_end   = date(_anim_ey, _anim_em, 1)

        # Build ordered list of month start dates
        _anim_months: list[date] = []
        _m = _anim_period_start
        while _m <= _anim_period_end:
            _anim_months.append(_m)
            _m = (_m.replace(day=28) + timedelta(days=4)).replace(day=1)

        if _anim_months:
            _month_labels = [m.strftime("%Y-%m") for m in _anim_months]
            _n_frames = len(_anim_months)

            # Initialise session state for animation
            if "anim_playing" not in st.session_state:
                st.session_state["anim_playing"] = False
            if "anim_frame_idx" not in st.session_state:
                st.session_state["anim_frame_idx"] = 0
            # Clamp index in case period changed
            st.session_state["anim_frame_idx"] = (
                st.session_state["anim_frame_idx"] % _n_frames
            )

            # Controls row
            ctrl_c1, ctrl_c2, ctrl_c3 = st.columns([1, 1, 3])
            with ctrl_c1:
                if st.button(_t("anim_play"), key="anim_play_btn"):
                    st.session_state["anim_playing"] = True
            with ctrl_c2:
                if st.button(_t("anim_pause"), key="anim_pause_btn"):
                    st.session_state["anim_playing"] = False
            with ctrl_c3:
                anim_speed = st.slider(
                    _t("anim_speed"), min_value=1, max_value=10,
                    value=5, step=1, key="anim_speed",
                )

            # Manual scrub slider — value= drives position; no key so Streamlit
            # always uses the value we pass (avoids stale session-state reads)
            _cur_idx = st.session_state["anim_frame_idx"]
            _sel_label = st.select_slider(
                _t("anim_slider"), options=_month_labels,
                value=_month_labels[_cur_idx],
            )
            # Detect manual scrub: slider returned something different from
            # what we passed in (user dragged it) → jump and stop auto-play
            _slider_idx = _month_labels.index(_sel_label)
            if _slider_idx != _cur_idx:
                st.session_state["anim_frame_idx"] = _slider_idx
                st.session_state["anim_playing"] = False
                _cur_idx = _slider_idx

            _sel_m = _anim_months[_cur_idx]
            _sel_m_end = (_sel_m.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)

            df_anim = load_all(_sel_m, _sel_m_end, quality_filter)
            if df_anim.empty:
                st.info(_t("anim_no_data"))
            else:
                _anim_title = _t("anim_map_title", month=_month_labels[_cur_idx])
                fig_anim = chart_geo_map(df_anim, "rt", _geojson, title=_anim_title)
                st.pyplot(fig_anim, use_container_width=True)
                plt.close(fig_anim)

            # Auto-advance: sleep then advance index and rerun
            if st.session_state["anim_playing"]:
                time.sleep(anim_speed)
                _next_idx = (_cur_idx + 1) % _n_frames
                st.session_state["anim_frame_idx"] = _next_idx
                st.rerun()

        # ── Section: Period Comparison ────────────────────────────────────────
        st.divider()
        st.subheader(_t("cmp_title"))

        _cmp_metric_opt = st.radio(
            _t("cmp_metric"), ["DA", "RT"], horizontal=True, key="cmp_metric"
        )
        _cmp_m = _cmp_metric_opt.lower()

        cmp_a_col, cmp_b_col = st.columns(2)
        with cmp_a_col:
            st.markdown(f"**{_t('cmp_period_a')}**")
            cmp_a_start = st.date_input(_t("cmp_start"), value=d_start, key="cmp_a_start")
            cmp_a_end   = st.date_input(_t("cmp_end"),   value=d_end,   key="cmp_a_end")
        with cmp_b_col:
            st.markdown(f"**{_t('cmp_period_b')}**")
            _b_default_end   = d_end - timedelta(days=365)
            _b_default_start = d_start - timedelta(days=365)
            cmp_b_start = st.date_input(_t("cmp_start"), value=_b_default_start, key="cmp_b_start")
            cmp_b_end   = st.date_input(_t("cmp_end"),   value=_b_default_end,   key="cmp_b_end")

        df_cmp_a = load_all(cmp_a_start, cmp_a_end, quality_filter)
        df_cmp_b = load_all(cmp_b_start, cmp_b_end, quality_filter)

        cmp_map_a, cmp_map_b = st.columns(2)
        with cmp_map_a:
            _title_a = _t("cmp_map_title",
                          metric=_cmp_metric_opt, start=cmp_a_start, end=cmp_a_end)
            if df_cmp_a.empty:
                st.info(_t("cmp_no_data"))
            else:
                fig_cmp_a = chart_geo_map(df_cmp_a, _cmp_m, _geojson, title=_title_a)
                st.pyplot(fig_cmp_a, use_container_width=True)
                plt.close(fig_cmp_a)
        with cmp_map_b:
            _title_b = _t("cmp_map_title",
                          metric=_cmp_metric_opt, start=cmp_b_start, end=cmp_b_end)
            if df_cmp_b.empty:
                st.info(_t("cmp_no_data"))
            else:
                fig_cmp_b = chart_geo_map(df_cmp_b, _cmp_m, _geojson, title=_title_b)
                st.pyplot(fig_cmp_b, use_container_width=True)
                plt.close(fig_cmp_b)

# ── Tab 7: Inter-Provincial Flow ─────────────────────────────────────────────
with tab_interprov:
    st.subheader(_t("interprov_title"))
    st.caption(f"**{d_start}** → **{d_end}**")

    df_ip = load_interprov(d_start, d_end)

    if df_ip.empty:
        st.info(_t("interprov_no_data"))
    else:
        _dir_export = "送端"
        _dir_import = "受端"

        # ── Price trend chart (最高均价 for each direction) ───────────────────
        _price_rows = df_ip[df_ip["metric_type"] == "最高均价"].copy()
        if not _price_rows.empty:
            fig_ip_price = go.Figure()
            for _dir, _label in [(_dir_export, _t("direction_export")),
                                  (_dir_import, _t("direction_import"))]:
                _s = _price_rows[_price_rows["direction"] == _dir].sort_values("report_date")
                if not _s.empty:
                    fig_ip_price.add_trace(go.Scatter(
                        x=_s["report_date"], y=_s["price_yuan_kwh"],
                        mode="lines+markers", name=_label,
                        hovertemplate="%{x}<br>%{y:.4f} ¥/kWh<extra></extra>",
                    ))
            fig_ip_price.update_layout(
                title=_t("interprov_price_trend"),
                xaxis_title="", yaxis_title="¥/kWh",
                height=320, margin=dict(l=40, r=20, t=40, b=30),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_ip_price, use_container_width=True)

        # ── Volume trend chart (total_vol_100gwh, one bar per direction/date) ─
        _vol_rows = df_ip[df_ip["total_vol_100gwh"].notna()].copy()
        if not _vol_rows.empty:
            fig_ip_vol = go.Figure()
            for _dir, _label in [(_dir_export, _t("direction_export")),
                                  (_dir_import, _t("direction_import"))]:
                _sv = _vol_rows[_vol_rows["direction"] == _dir].sort_values("report_date")
                if not _sv.empty:
                    fig_ip_vol.add_trace(go.Bar(
                        x=_sv["report_date"], y=_sv["total_vol_100gwh"],
                        name=_label,
                        hovertemplate="%{x}<br>%{y:.2f} 亿kWh<extra></extra>",
                    ))
            fig_ip_vol.update_layout(
                title=_t("interprov_vol_trend"),
                xaxis_title="", yaxis_title="亿kWh",
                barmode="group", height=280,
                margin=dict(l=40, r=20, t=40, b=30),
                legend=dict(orientation="h", yanchor="bottom", y=1.02),
            )
            st.plotly_chart(fig_ip_vol, use_container_width=True)

        # ── Detail tables per direction ───────────────────────────────────────
        st.divider()
        _col_map = {
            "report_date":      _t("col_date"),
            "metric_type":      _t("col_metric_type"),
            "province_cn":      _t("col_province_cn"),
            "province_share":   _t("col_share"),
            "price_yuan_kwh":   _t("col_price_kwh"),
            "price_chg_pct":    _t("col_price_chg"),
            "time_period":      _t("col_time_period"),
            "total_vol_100gwh": _t("col_volume_gwh"),
            "source_pdf":       _t("col_source"),
        }
        _display_cols = list(_col_map.keys())

        for _dir, _label in [(_dir_export, _t("direction_export")),
                              (_dir_import, _t("direction_import"))]:
            _sub = (df_ip[df_ip["direction"] == _dir][_display_cols]
                    .rename(columns=_col_map)
                    .sort_values(_t("col_date"), ascending=False))
            if not _sub.empty:
                st.subheader(_label)
                _fmt = {
                    _t("col_price_kwh"):   "{:.4f}",
                    _t("col_price_chg"):   "{:.2f}",
                    _t("col_volume_gwh"):  "{:.4f}",
                    _t("col_share"):       "{:.2f}",
                }
                st.dataframe(
                    _sub.style.format(_fmt, na_rep="—"),
                    use_container_width=True, hide_index=True,
                )


# ── Tab 8: Data Management ────────────────────────────────────────────────────
with tab_mgmt:
    import re as _re
    from pathlib import Path as _Path

    PROVINCES_MAP: dict[str, str] = {
        "山东": "Shandong", "山西": "Shanxi", "蒙西": "Mengxi", "内蒙古": "Mengxi",
        "甘肃": "Gansu", "广东": "Guangdong", "四川": "Sichuan", "云南": "Yunnan",
        "贵州": "Guizhou", "广西": "Guangxi", "湖南": "Hunan", "湖北": "Hubei",
        "安徽": "Anhui", "浙江": "Zhejiang", "江苏": "Jiangsu", "福建": "Fujian",
        "河南": "Henan", "陕西": "Shaanxi", "宁夏": "Ningxia", "新疆": "Xinjiang",
        "辽宁": "Liaoning", "吉林": "Jilin", "黑龙江": "Heilongjiang", "蒙东": "Mengdong",
        "河北": "Hebei", "冀北": "Hebei-North", "冀南": "Hebei-South",
        "河北南网": "Hebei-South", "青海": "Qinghai",
        "江西": "Jiangxi", "海南": "Hainan", "重庆": "Chongqing", "上海": "Shanghai",
        "北京": "Beijing", "天津": "Tianjin",
    }

    def _parse_pdf_date_range(stem: str, year: int = 2026):
        stem = stem.strip().rstrip("）)） ")

        m = _re.fullmatch(r"(\d{2})(\d{2})(?:-(\d{2})(\d{2}))?", stem)
        if m:
            try:
                start = date(year, int(m.group(1)), int(m.group(2)))
                end   = date(year, int(m.group(3) or m.group(1)),
                             int(m.group(4) or m.group(2)))
                return start, end
            except ValueError:
                pass

        m = _re.search(r"(\d{1,2})\.(\d{1,2})(?:-(?:(\d{1,2})\.)?(\d{1,2}))?", stem)
        if m:
            try:
                m1, d1 = int(m.group(1)), int(m.group(2))
                start  = date(year, m1, d1)
                if m.group(4):
                    m2 = int(m.group(3)) if m.group(3) else m1
                    d2 = int(m.group(4))
                    end_year = year + 1 if m2 < m1 else year
                    end = date(end_year, m2, d2)
                else:
                    end = start
                return start, end
            except ValueError:
                pass

        return None

    @st.cache_data(ttl=60, show_spinner=False)
    def _scan_pdf_inventory(year: int = 2026):
        data_dir = _REPO / "data" / "spot reports" / str(year)
        pdfs = []
        if not data_dir.exists():
            return pdfs
        for p in sorted(data_dir.glob("*.pdf")):
            stem = p.stem
            m = _re.search(r"[（(]([^)）]+)[）)]", stem)
            if not m:
                continue
            date_range_result = _parse_pdf_date_range(m.group(1).strip(), year)
            if date_range_result:
                pdfs.append((p.name, date_range_result[0], date_range_result[1], p))
        return pdfs

    @st.cache_data(ttl=30, show_spinner=False)
    def _db_coverage(year: int = 2026):
        cur = _conn().cursor()
        cur.execute(
            "SELECT DISTINCT report_date FROM spot_daily "
            "WHERE report_date BETWEEN %s AND %s",
            (date(year, 1, 1), date(year, 12, 31)),
        )
        return {r[0] for r in cur.fetchall()}

    @st.cache_data(ttl=30, show_spinner=False)
    def _db_coverage_detail(year: int = 2026):
        cur = _conn().cursor()
        cur.execute(
            """SELECT report_date::date, COUNT(da_avg), COUNT(rt_avg)
               FROM spot_daily
               WHERE report_date BETWEEN %s AND %s
               GROUP BY 1""",
            (date(year, 1, 1), date(year, 12, 31)),
        )
        return {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    # ── Layout ────────────────────────────────────────────────────────────────
    st.subheader(_t("data_mgmt_title"))

    col_yr, _, _ = st.columns([1, 2, 1])
    with col_yr:
        sel_year = st.selectbox(_t("report_year"), [2026, 2025, 2024], key="mgmt_year")

    c_left, c_right = st.columns([2, 1])

    with c_left:
        mgmt_mode = st.radio(
            _t("mode_label"),
            [_t("mode_fill_gaps"), _t("mode_backfill")],
            horizontal=False,
            key="mgmt_mode",
        )
        st.caption(_t("additional_steps"))
        run_interprov = st.checkbox(
            _t("chk_interprov"),
            value=True,
            key="mgmt_interprov",
            help=_t("chk_interprov_help"),
        )
        run_ai = st.checkbox(
            _t("chk_ai"),
            value=False,
            key="mgmt_ai",
            help=_t("chk_ai_help"),
        )

    with c_right:
        _yr_end = date(sel_year, 12, 31) if sel_year < date.today().year else date.today() - timedelta(days=1)
        bf_start = st.date_input(_t("start_date"), date(sel_year, 1, 1), key=f"bf_start_{sel_year}")
        bf_end   = st.date_input(_t("end_date"),   _yr_end,              key=f"bf_end_{sel_year}")

    st.divider()

    # ── PDF inventory + gap analysis ──────────────────────────────────────────
    inventory = _scan_pdf_inventory(sel_year)
    coverage = _db_coverage_detail(sel_year)
    existing_dates = set(coverage.keys())

    relevant_pdfs = [
        (fname, s, e, path)
        for fname, s, e, path in inventory
        if s <= bf_end and e >= bf_start
    ]

    inv_rows = []
    for fname, s, e, path in relevant_pdfs:
        dates_in_range = [
            s + timedelta(days=i)
            for i in range((e - s).days + 1)
            if bf_start <= s + timedelta(days=i) <= bf_end
        ]
        missing = [d for d in dates_in_range if d not in existing_dates]
        partial = [
            d for d in dates_in_range
            if d in existing_dates and (coverage[d][0] == 0 or coverage[d][1] == 0)
        ]
        inv_rows.append({
            _t("col_pdf"):          fname,
            _t("col_covers"):       f"{s} → {e}",
            _t("col_dates_range"):  len(dates_in_range),
            _t("col_missing"):      len(missing),
            _t("col_partial"):      len(partial),
            _t("col_status"):       _t("status_missing") if missing else (
                                        _t("status_partial") if partial else _t("status_ok")
                                    ),
        })

    if inv_rows:
        inv_df = pd.DataFrame(inv_rows)
        status_col = _t("col_status")
        st.dataframe(
            inv_df.style.apply(
                lambda col: [
                    "background-color: #ffe0e0" if v == _t("status_missing")
                    else "background-color: #fff3cd" if v == _t("status_partial")
                    else "background-color: #d4edda"
                    for v in col
                ],
                subset=[status_col],
            ),
            use_container_width=True, hide_index=True,
        )
    else:
        st.info(_t("no_pdfs"))

    if relevant_pdfs:
        needs_work = [
            (fname, s, e, path)
            for fname, s, e, path in relevant_pdfs
            if any(
                s + timedelta(days=i) not in existing_dates
                for i in range((e - s).days + 1)
                if bf_start <= s + timedelta(days=i) <= bf_end
            )
        ]
        partial_pdfs = [
            (fname, s, e, path)
            for fname, s, e, path in relevant_pdfs
            if any(
                (s + timedelta(days=i)) in existing_dates
                and (coverage[s + timedelta(days=i)][0] == 0
                     or coverage[s + timedelta(days=i)][1] == 0)
                for i in range((e - s).days + 1)
                if bf_start <= s + timedelta(days=i) <= bf_end
            )
        ]

        if mgmt_mode == _t("mode_fill_gaps"):
            pdfs_to_run = needs_work
            btn_label = _t("btn_fill_gaps", n=len(pdfs_to_run))
        else:
            pdfs_to_run = relevant_pdfs
            btn_label = _t("btn_reingest", n=len(pdfs_to_run))

        col_btn, col_info = st.columns([1, 3])
        with col_btn:
            run_backfill = st.button(btn_label, type="primary", disabled=len(pdfs_to_run) == 0)
        with col_info:
            if mgmt_mode == _t("mode_fill_gaps") and not needs_work and partial_pdfs:
                st.warning(_t("warn_partial", n=len(partial_pdfs)))
            elif not pdfs_to_run:
                st.success(_t("all_present"))

        if run_backfill:
            from services.spot_ingest.pdf_parser import parse_pdf as _parse_pdf
            from services.spot_ingest.db_upsert import upsert_rows as _upsert_rows
            if run_interprov:
                from services.spot_ingest.interprov_parser import parse_interprov as _parse_interprov
                from services.spot_ingest.interprov_upsert import upsert_interprov_rows as _upsert_interprov_rows
            if run_ai:
                from services.spot_ingest.ai_summary import generate_summary as _gen_summary
                from services.spot_ingest.interprov_upsert import upsert_summary as _upsert_summary

            provinces_cn = list(PROVINCES_MAP.keys())
            total = len(pdfs_to_run)
            progress = st.progress(0, text=_t("prog_starting"))
            results = []

            for i, (fname, s, e, path) in enumerate(pdfs_to_run):
                progress.progress(i / total, text=_t("prog_parsing", fname=fname))
                pdf_year = int(path.parent.name) if path.parent.name.isdigit() else sel_year
                interprov_count = 0
                ai_count = 0
                try:
                    parsed = _parse_pdf(path, pdf_year, provinces_cn)
                    rows = []
                    for rdate, provs in parsed.items():
                        for pcn, vals in provs.items():
                            rows.append({
                                "report_date": rdate,
                                "province_cn": pcn,
                                "province_en": PROVINCES_MAP.get(pcn, pcn),
                                **vals,
                            })
                    n = _upsert_rows(rows)

                    interprov_rows: list = []
                    if run_interprov:
                        progress.progress((i + 0.5) / total,
                                          text=_t("prog_interprov", fname=fname))
                        interprov_rows = _parse_interprov(path, pdf_year)
                        if interprov_rows:
                            interprov_count = _upsert_interprov_rows(interprov_rows)

                    if run_ai:
                        for rdate in sorted(parsed.keys()):
                            progress.progress((i + 0.7) / total,
                                              text=_t("prog_ai", rdate=rdate))
                            day_prices = [
                                {
                                    "province_en": r.get("province_en", r.get("province_cn", "")),
                                    "da_avg": r.get("da_avg"),
                                    "rt_avg": r.get("rt_avg"),
                                }
                                for r in rows
                                if r.get("report_date") == rdate
                            ]
                            day_interprov = [r for r in interprov_rows if r["report_date"] == rdate]
                            summary = _gen_summary(rdate, day_prices, day_interprov, fname)
                            if summary:
                                _upsert_summary(summary)
                                ai_count += 1

                    results.append({
                        _t("col_pdf"):       fname,
                        _t("col_dates"):     str(sorted(parsed.keys())),
                        _t("col_rows"):      n,
                        _t("col_interprov"): interprov_count,
                        _t("col_ai"):        ai_count,
                        _t("col_error"):     "",
                    })
                except Exception as exc:
                    results.append({
                        _t("col_pdf"):       fname,
                        _t("col_dates"):     "",
                        _t("col_rows"):      0,
                        _t("col_interprov"): 0,
                        _t("col_ai"):        0,
                        _t("col_error"):     str(exc)[:120],
                    })

            progress.progress(1.0, text=_t("prog_done"))
            st.success(_t("backfill_complete", n=total))
            st.dataframe(pd.DataFrame(results), use_container_width=True, hide_index=True)

            load_all.clear()
            load_kpis.clear()
            _db_coverage.clear()
            _db_coverage_detail.clear()
            st.rerun()
