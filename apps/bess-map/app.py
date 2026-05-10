"""
BESS Asset Map — Pillar 2
Province-level BESS investment screening, dispatch economics, IRR modelling.

Run locally:
    set -a && source config/.env && set +a
    streamlit run apps/bess-map/app.py --server.port 8503
"""
from __future__ import annotations

import os
import sys
import subprocess
import datetime as dt
import tempfile
from pathlib import Path
from typing import Optional

import json

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.font_manager as _mfm
from matplotlib.patches import Polygon as MplPolygon

# Detect a CJK-capable font for Chinese map titles (mirrors spot-market pattern)
_CJK_FONTS = ["Noto Sans CJK SC", "Microsoft YaHei", "SimHei", "SimSun",
               "STHeiti", "WenQuanYi Micro Hei", "Arial Unicode MS"]
_CJK_FONT: str | None = None
for _f in _CJK_FONTS:
    try:
        if _mfm.findfont(_mfm.FontProperties(family=_f), fallback_to_default=False):
            _CJK_FONT = _f
            break
    except (ValueError, OSError):
        pass
if _CJK_FONT is None:
    for _fp in _mfm.findSystemFonts():
        _bn = os.path.basename(_fp).lower()
        if any(k in _bn for k in ("notocjk", "notosanscjk", "noto_cjk",
                                   "wqymicro", "wenquanyi")):
            try:
                _mfm.fontManager.addfont(_fp)
                _CJK_FONT = _mfm.FontProperties(fname=_fp).get_name()
                break
            except Exception:
                pass
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text as sql_text

# ── path / env setup ──────────────────────────────────────────────────────────
_REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_REPO))

for _env in [_REPO / "config" / ".env", _REPO / ".env"]:
    if _env.exists():
        load_dotenv(_env)

st.set_page_config(page_title="BESS Asset Map", layout="wide", page_icon="🔋")

# ── auth ──────────────────────────────────────────────────────────────────────
try:
    from auth.rbac import get_user, get_role, get_email, require_role
    _AUTH_AVAILABLE = True
except Exception:
    _AUTH_AVAILABLE = False

if _AUTH_AVAILABLE:
    try:
        require_role(["Admin", "Quant", "Trader", "Analyst"])
        _user = get_user()
        if _user:
            st.caption(f"User: {_user.get('email','?')} | Role: {get_role() or '?'}")
    except Exception:
        pass  # local mode — no auth

# ── translations ──────────────────────────────────────────────────────────────
_T: dict[str, dict[str, str]] = {
    "en": {
        "app_title":            "Quant Analyst",
        "lang_label":           "🌐 Language",
        "filters":              "Filters",
        "date_range":           "Date range",
        "duration_label":       "Duration",
        "all_durations":        "Both (2h & 4h)",
        # tabs
        "tab_ranking":          "Province Ranking",
        "tab_dispatch":         "Dispatch & Economics",
        "tab_irr":              "IRR Calculator",
        "tab_mgmt":             "Data Management",
        "tab_agent":            "Quant",
        # ranking
        "rank_title":           "BESS Investment Screening — Province Ranking",
        "rank_caption":         "Annual arbitrage revenue per MWh of **installed energy capacity** (= power_MW × duration_h). Based on LP perfect-foresight dispatch.",
        "rank_kpi_2h":          "Best Province (2h)",
        "rank_kpi_4h":          "Best Province (4h)",
        "rank_kpi_capture":     "Avg Capture Rate",
        "rank_chart_title":     "Annual Revenue by Province (¥/MWh_installed/yr)",
        "rank_col_province":    "Province",
        "rank_col_2h":          "2h Rev (¥/MWh_cap/yr)",
        "rank_col_4h":          "4h Rev (¥/MWh_cap/yr)",
        "rank_col_capture":     "Capture Rate (%)",
        "rank_col_days":        "Days",
        "rank_spread_title":    "Intraday RT Spread by Province (¥/kWh)",
        "rank_spread_caption":  "Max − Min of hourly avg RT prices. Direct measure of intraday arbitrage opportunity.",
        # dispatch
        "disp_province":        "Province",
        "disp_duration":        "Duration",
        "disp_date_range":      "Date range",
        "disp_monthly_title":   "Monthly Avg Daily Revenue per MWh of Installed Capacity (¥/MWh_cap/day)",
        "disp_capture_title":   "Monthly Capture Rate (%)",
        "disp_detail_title":    "Dispatch Detail — Hourly",
        "disp_detail_date":     "Select date",
        "disp_no_dispatch":     "No dispatch data for selected date.",
        "disp_charge":          "Charge (MW)",
        "disp_discharge":       "Discharge (MW)",
        "disp_soc":             "SoC (MWh)",
        "disp_rt_price":        "RT Price (¥/kWh)",
        # irr
        "irr_title":            "BESS IRR Calculator",
        "irr_caption":          "Revenue basis is pulled from DB for the selected province/duration. All other parameters are user-defined.",
        "irr_province":         "Province",
        "irr_duration":         "Duration",
        "irr_fetch_btn":        "Load revenue basis from DB",
        "irr_rev_basis":        "Revenue basis (from DB)",
        "irr_theo_day":         "Theoretical ¥/MWh_cap/day",
        "irr_capture":          "Avg capture rate",
        "irr_real_day":         "Realised ¥/MWh_cap/day",
        "irr_capex":            "Capex (¥/kWh)",
        "irr_rte":              "Round-trip efficiency (%)",
        "irr_om":               "O&M (¥/MW/year)",
        "irr_subsidy":          "Discharge subsidy (¥/MWh)",
        "irr_degradation":      "Capacity degradation (%/year)",
        "irr_equity":           "Equity (%)",
        "irr_loan_rate":        "Loan rate (%/year)",
        "irr_loan_tenure":      "Loan tenure (years)",
        "irr_life":             "Project life (years)",
        "irr_calc_btn":         "Calculate IRR",
        "irr_result_irr":       "Equity IRR",
        "irr_result_payback":   "Simple Payback",
        "irr_result_npv":       "NPV (8% discount)",
        "irr_result_years":     " yrs",
        "irr_cashflow_title":   "Annual Cashflow (¥/MWh capacity)",
        "irr_sensitivity":      "IRR Sensitivity: Capex vs Revenue Multiplier",
        "irr_no_data":          "No DB data for this province/duration. Run capture pipeline first.",
        "irr_negative_irr":     "Negative IRR — project does not recover equity over project life.",
        "irr_cf_revenue":       "Revenue",
        "irr_cf_om":            "O&M",
        "irr_cf_debt":          "Debt service",
        "irr_cf_net":           "Net FCF",
        # mgmt
        "mgmt_title":           "Data Management",
        "mgmt_upload_title":    "Upload Province Excel Files",
        "mgmt_upload_help":     "Upload hourly RT/DA price Excel files (one per province, Chinese filename).",
        "mgmt_ingest_title":    "Ingest Uploaded Files → DB",
        "mgmt_ingest_btn":      "Run ingestion",
        "mgmt_ingest_no_files": "No files uploaded in this session. Upload Excel files above first.",
        "mgmt_ingest_s3_needed":"S3 not configured — cannot download files for ingestion.",
        "mgmt_capture_title":   "Run Capture Pipeline",
        "mgmt_capture_provs":   "Provinces (blank = all)",
        "mgmt_capture_dur":     "Duration",
        "mgmt_capture_force":   "Force recompute",
        "mgmt_capture_btn":     "Run capture pipeline",
        "mgmt_coverage_title":  "DB Coverage",
        "mgmt_col_province":    "Province",
        "mgmt_col_last_hourly": "Last hourly date",
        "mgmt_col_last_capture":"Last capture date",
        "mgmt_col_status":      "Status",
        "mgmt_status_ok":       "OK",
        "mgmt_status_stale":    "Stale (>30d)",
        "mgmt_status_missing":  "No data",
        "mgmt_fund_title":      "Fundamentals Ingest (Load / Bidding Space / Wind / Solar)",
        "mgmt_fund_btn":        "Run Fundamentals Ingest",
        "mgmt_fund_no_files":   "No files uploaded this session. Upload Excel files above first.",
        "mgmt_fund_s3_needed":  "S3 not configured — cannot download files for ingestion.",
        "mgmt_col_last_fund":   "Last fundamentals date",
        # agent
        "agent_title":          "BESS Market AI Agent",
        "agent_caption":        "Ask about province BESS economics, IRR scenarios, or dispatch performance.",
        "agent_welcome":        "Hi! I can query BESS economics, dispatch data, and run IRR calculations for any province. What would you like to know?",
        "agent_placeholder":    "e.g. Which province has the best 4h BESS IRR at 600 ¥/kWh capex?",
        "agent_thinking":       "Thinking...",
        "agent_tool_call":      "Tool call: {tool}",
        "agent_tool_result":    "Result ({n} rows)",
        "agent_no_key":         "ANTHROPIC_API_KEY is not set.",
        "agent_clear":          "Clear chat",
        "agent_error":          "Agent error: {err}",
        # memory
        "mem_section":          "Agent Memory",
        "mem_caption":          "Facts, views, and decisions saved from past conversations. Injected into every session.",
        "mem_suggested":        "Suggested memories from this exchange",
        "mem_save_selected":    "Save selected",
        "mem_nothing":          "Nothing worth saving extracted.",
        "mem_saved_ok":         "Saved {n} memory item(s).",
        "mem_manage":           "Manage Memory",
        "mem_empty":            "No memories yet.",
        "mem_delete":           "Delete",
        "mem_col_cat":          "Category",
        "mem_col_subject":      "Subject",
        "mem_col_content":      "Content",
        "mem_col_source":       "Source",
        "mem_col_date":         "Saved",
        # forecast method
        "forecast_method_label":   "Revenue basis",
        "forecast_theoretical":    "Theoretical (LP perfect foresight)",
        "forecast_realized":       "Realized (forecast model)",
        # model selector
        "model_selector_label":    "Forecast model",
        "model_naive_ar17":        "Naive AR (D-1 & D-7 combined)",
        "model_ols_time":          "OLS + Time (ARIMA proxy)",
        "model_ols_fund":          "OLS + Fundamentals (D-1 bidding space)",
        # cycles
        "rank_col_cycles":         "Avg Daily Cycles",
        "rank_kpi_cycles":         "Avg Daily Cycles (4h)",
        # geo
        "tab_geo":                 "Geo Map",
        "geo_title":               "Annual BESS Revenue by Province (¥/MWh/yr)",
        "geo_caption":             "🟢 ≤3yr payback · 🟡 3–5yr · 🟠 5–7yr · 🔴 >7yr (assumes standard capex)",
        "geo_unavailable":         "Province boundary data unavailable.",
        "geo_2h_title":            "2h BESS — Annual Revenue (¥/MWh/yr)",
        "geo_4h_title":            "4h BESS — Annual Revenue (¥/MWh/yr)",
    },
    "zh": {
        "app_title":            "量化分析师",
        "lang_label":           "🌐 语言",
        "filters":              "筛选条件",
        "date_range":           "日期范围",
        "duration_label":       "时长",
        "all_durations":        "全部（2h和4h）",
        "tab_ranking":          "省份排名",
        "tab_dispatch":         "调度与收益",
        "tab_irr":              "IRR计算器",
        "tab_mgmt":             "数据管理",
        "tab_agent":            "量化分析师",
        "rank_title":           "储能投资筛选 — 省份排名",
        "rank_caption":         "每MWh**安装能量容量**（= 功率MW × 时长h）的年度套利收益（元/MWh/年）。基于LP完美预见调度。",
        "rank_kpi_2h":          "最优省份（2h）",
        "rank_kpi_4h":          "最优省份（4h）",
        "rank_kpi_capture":     "平均捕获率",
        "rank_chart_title":     "各省年度理论收益（元/MWh/年）",
        "rank_col_province":    "省份",
        "rank_col_2h":          "2h年收益（元/MWh/年）",
        "rank_col_4h":          "4h年收益（元/MWh/年）",
        "rank_col_capture":     "捕获率（%）",
        "rank_col_days":        "天数",
        "rank_spread_title":    "各省日内实时价差（元/千瓦时）",
        "rank_spread_caption":  "小时均价最大值减最小值。日内套利机会的直接衡量指标。",
        "disp_province":        "省份",
        "disp_duration":        "时长",
        "disp_date_range":      "日期范围",
        "disp_monthly_title":   "月度日均收益（元/MWh安装容量/天）",
        "disp_capture_title":   "月度捕获率（%）",
        "disp_detail_title":    "调度明细 — 小时数据",
        "disp_detail_date":     "选择日期",
        "disp_no_dispatch":     "所选日期无调度数据。",
        "disp_charge":          "充电（MW）",
        "disp_discharge":       "放电（MW）",
        "disp_soc":             "荷电状态（MWh）",
        "disp_rt_price":        "实时电价（元/千瓦时）",
        "irr_title":            "储能IRR计算器",
        "irr_caption":          "收益基准从数据库中读取（选定省份/时长）。其他参数由用户自定义。",
        "irr_province":         "省份",
        "irr_duration":         "时长",
        "irr_fetch_btn":        "从数据库加载收益基准",
        "irr_rev_basis":        "收益基准（来自数据库）",
        "irr_theo_day":         "理论日收益（元/MWh容量/天）",
        "irr_capture":          "平均捕获率",
        "irr_real_day":         "实际日收益（元/MWh容量/天）",
        "irr_capex":            "资本支出（元/kWh）",
        "irr_rte":              "往返效率（%）",
        "irr_om":               "运维成本（元/MW/年）",
        "irr_subsidy":          "放电补贴（元/MWh）",
        "irr_degradation":      "容量衰减（%/年）",
        "irr_equity":           "权益比例（%）",
        "irr_loan_rate":        "贷款利率（%/年）",
        "irr_loan_tenure":      "贷款年限（年）",
        "irr_life":             "项目寿命（年）",
        "irr_calc_btn":         "计算IRR",
        "irr_result_irr":       "权益IRR",
        "irr_result_payback":   "简单回收期",
        "irr_result_npv":       "NPV（8%折现率）",
        "irr_result_years":     "年",
        "irr_cashflow_title":   "年度现金流（元/MWh容量）",
        "irr_sensitivity":      "IRR敏感性分析：资本支出 × 收益倍数",
        "irr_no_data":          "该省份/时长无数据库数据，请先运行捕获流水线。",
        "irr_negative_irr":     "IRR为负 — 项目在生命周期内无法回收权益。",
        "irr_cf_revenue":       "收益",
        "irr_cf_om":            "运维成本",
        "irr_cf_debt":          "还本付息",
        "irr_cf_net":           "净自由现金流",
        "mgmt_title":           "数据管理",
        "mgmt_upload_title":    "上传省份Excel文件",
        "mgmt_upload_help":     "上传含小时实时/日前价格的Excel文件（每省一个，中文文件名）。",
        "mgmt_ingest_title":    "导入已上传文件→数据库",
        "mgmt_ingest_btn":      "运行导入",
        "mgmt_ingest_no_files": "本次会话无已上传文件，请先在上方上传Excel文件。",
        "mgmt_ingest_s3_needed":"S3未配置——无法下载文件进行导入。",
        "mgmt_capture_title":   "运行捕获流水线",
        "mgmt_capture_provs":   "省份（空=全部）",
        "mgmt_capture_dur":     "时长",
        "mgmt_capture_force":   "强制重算",
        "mgmt_capture_btn":     "运行捕获流水线",
        "mgmt_coverage_title":  "数据库覆盖情况",
        "mgmt_col_province":    "省份",
        "mgmt_col_last_hourly": "最新小时数据日期",
        "mgmt_col_last_capture":"最新捕获日期",
        "mgmt_col_status":      "状态",
        "mgmt_status_ok":       "正常",
        "mgmt_status_stale":    "数据过旧（>30天）",
        "mgmt_status_missing":  "无数据",
        "mgmt_fund_title":      "基本面数据导入（负荷/竞价空间/风光出力）",
        "mgmt_fund_btn":        "运行基本面导入",
        "mgmt_fund_no_files":   "本次会话无已上传文件，请先在上方上传Excel文件。",
        "mgmt_fund_s3_needed":  "S3未配置——无法下载文件进行导入。",
        "mgmt_col_last_fund":   "最新基本面日期",
        "agent_title":          "储能市场量化分析师",
        "agent_caption":        "询问省份储能经济性、IRR情景或调度表现。",
        "agent_welcome":        "您好！我可以查询储能经济数据、调度数据，并为任意省份计算IRR。请问您想了解什么？",
        "agent_placeholder":    "例如：在600元/kWh资本支出下，哪个省份的4h储能IRR最高？",
        "agent_thinking":       "思考中...",
        "agent_tool_call":      "工具调用：{tool}",
        "agent_tool_result":    "结果（{n}行）",
        "agent_no_key":         "ANTHROPIC_API_KEY未设置。",
        "agent_clear":          "清空对话",
        "agent_error":          "助手错误：{err}",
        # memory
        "mem_section":          "智能助手记忆",
        "mem_caption":          "从历史对话中保存的事实、观点和决策，每次会话自动注入。",
        "mem_suggested":        "本次对话建议保存的记忆",
        "mem_save_selected":    "保存所选",
        "mem_nothing":          "未提取到值得保存的内容。",
        "mem_saved_ok":         "已保存 {n} 条记忆。",
        "mem_manage":           "管理记忆",
        "mem_empty":            "暂无记忆。",
        "mem_delete":           "删除",
        "mem_col_cat":          "类别",
        "mem_col_subject":      "主题",
        "mem_col_content":      "内容",
        "mem_col_source":       "来源",
        "mem_col_date":         "保存时间",
        # forecast method
        "forecast_method_label":   "收益基准",
        "forecast_theoretical":    "理论值（LP完美预见）",
        "forecast_realized":       "实际值（预测模型）",
        # model selector
        "model_selector_label":    "预测模型",
        "model_naive_ar17":        "朴素AR（D-1与D-7组合）",
        "model_ols_time":          "OLS+时间特征（ARIMA代理）",
        "model_ols_fund":          "OLS+基本面（D-1竞价空间）",
        # cycles
        "rank_col_cycles":         "日均循环次数",
        "rank_kpi_cycles":         "日均循环次数（4h）",
        # geo
        "tab_geo":                 "地理分布图",
        "geo_title":               "各省年度储能收益（元/MWh/年）",
        "geo_caption":             "🟢 ≤3年回收 · 🟡 3–5年 · 🟠 5–7年 · 🔴 >7年（按标准资本支出）",
        "geo_unavailable":         "省级边界数据不可用。",
        "geo_2h_title":            "2h储能 — 年收益（元/MWh/年）",
        "geo_4h_title":            "4h储能 — 年收益（元/MWh/年）",
    },
}

def _t(key: str, **kw) -> str:
    lang = st.session_state.get("lang_radio", "English")
    d = "zh" if lang == "中文" else "en"
    v = _T[d].get(key, _T["en"].get(key, key))
    return v.format(**kw) if kw else v

# ── Province geo mappings (Chinese name → adcode) ────────────────────────────
_ZH_PROV_ADCODE: dict[str, str] = {
    "北京": "110000", "天津": "120000", "河北": "130000", "冀北": "130000",
    "河北南网": "130000", "山西": "140000", "蒙西": "150000", "内蒙古": "150000",
    "辽宁": "210000", "吉林": "220000", "黑龙江": "230000",
    "上海": "310000", "江苏": "320000", "浙江": "330000",
    "安徽": "340000", "福建": "350000", "江西": "360000",
    "山东": "370000", "河南": "410000", "豫北": "410000", "豫南": "410000",
    "豫西": "410000", "豫中东": "410000",
    "湖北": "420000", "湖南": "430000", "广东": "440000", "广西": "450000",
    "海南": "460000", "海南礼记": "460000", "海南那悦": "460000",
    "重庆": "500000", "四川": "510000", "贵州": "520000", "云南": "530000",
    "陕西": "610000", "甘肃": "620000", "青海": "630000",
    "宁夏": "640000", "新疆": "650000",
}

_PROV_CENTROIDS_BESS: dict[str, tuple[float, float]] = {
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

# ── DB engine ─────────────────────────────────────────────────────────────────
@st.cache_resource
def _get_engine():
    url = (
        os.environ.get("PGURL")
        or os.environ.get("DATABASE_URL")
        or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
    )
    if url.startswith("postgres://"):
        url = "postgresql://" + url[len("postgres://"):]
    if url.startswith("postgresql://") and not url.startswith("postgresql+psycopg2://"):
        url = "postgresql+psycopg2://" + url[len("postgresql://"):]
    return create_engine(url, pool_pre_ping=True,
                         connect_args={"connect_timeout": 10})

def _eng():
    engine = _get_engine()
    try:
        with engine.connect() as c:
            c.execute(sql_text("SELECT 1"))
    except Exception:
        _get_engine.clear()
        engine = _get_engine()
    return engine

# ── data loaders ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_province_ranking(_eng_key, start: str, end: str, model: str = "ols_rt_time_v1"):
    # Theoretical LP profit is model-agnostic; fetch it without model filter so the
    # ranking always shows even before a specific model's capture pipeline has run.
    # Realized profit and capture rate are LEFT JOINed from the selected model only.
    sql = sql_text("""
        SELECT t.province, t.duration_h, t.annual_theo, t.days,
               r.annual_real, r.capture_pct
        FROM (
            SELECT province, duration_h,
                   ROUND((AVG(theoretical_profit_per_mwh_day) * 365)::numeric, 0) AS annual_theo,
                   COUNT(DISTINCT date) AS days
            FROM marketdata.bess_capture_daily
            WHERE date BETWEEN :start AND :end
            GROUP BY province, duration_h
        ) t
        LEFT JOIN (
            SELECT province, duration_h,
                   ROUND((AVG(realized_profit_per_mwh_day)   * 365)::numeric, 0) AS annual_real,
                   ROUND((AVG(NULLIF(capture_rate, 'NaN'::double precision)) * 100)::numeric, 1) AS capture_pct
            FROM marketdata.bess_capture_daily
            WHERE date BETWEEN :start AND :end AND model = :model
            GROUP BY province, duration_h
        ) r USING (province, duration_h)
        ORDER BY annual_theo DESC NULLS LAST
    """)
    return pd.read_sql(sql, _eng(), params={"start": start, "end": end, "model": model})

@st.cache_data(ttl=3600)
def load_intraday_spread(_eng_key, start: str, end: str):
    sql = sql_text("""
        SELECT province, MAX(avg_price) - MIN(avg_price) AS spread
        FROM (
            SELECT province, EXTRACT(hour FROM datetime)::int AS hour,
                   AVG(rt_price) AS avg_price
            FROM marketdata.spot_prices_hourly
            WHERE datetime BETWEEN :start AND :end
            GROUP BY province, hour
        ) t GROUP BY province ORDER BY spread DESC
    """)
    return pd.read_sql(sql, _eng(), params={"start": start, "end": end})

@st.cache_data(ttl=3600)
def load_monthly_economics(_eng_key, province: str, duration_h: float, start: str, end: str, model: str = "ols_rt_time_v1"):
    # Theoretical is model-agnostic; realized/capture are model-specific.
    sql = sql_text("""
        SELECT t.month, t.province, t.theo_avg,
               r.real_avg, r.capture_pct
        FROM (
            SELECT date_trunc('month', date)::date AS month, province,
                   ROUND(AVG(theoretical_profit_per_mwh_day)::numeric, 2) AS theo_avg
            FROM marketdata.bess_capture_daily
            WHERE province = :p AND ABS(duration_h - :d) < 0.01
              AND date BETWEEN :start AND :end
            GROUP BY 1, 2
        ) t
        LEFT JOIN (
            SELECT date_trunc('month', date)::date AS month, province,
                   ROUND(AVG(realized_profit_per_mwh_day)::numeric, 2) AS real_avg,
                   ROUND((AVG(NULLIF(capture_rate, 'NaN'::double precision)) * 100)::numeric, 1) AS capture_pct
            FROM marketdata.bess_capture_daily
            WHERE province = :p AND ABS(duration_h - :d) < 0.01
              AND date BETWEEN :start AND :end AND model = :model
            GROUP BY 1, 2
        ) r USING (month, province)
        ORDER BY 1
    """)
    return pd.read_sql(sql, _eng(),
                       params={"p": province, "d": duration_h, "start": start, "end": end, "model": model},
                       parse_dates=["month"])

@st.cache_data(ttl=3600)
def load_last_dispatch_date(_eng_key, province: str, duration_h: float) -> dt.date | None:
    sql = sql_text("""
        SELECT MAX(datetime::date) AS last_date
        FROM marketdata.spot_dispatch_hourly_theoretical
        WHERE province = :p AND ABS(duration_h - :d) < 0.01
    """)
    try:
        row = pd.read_sql(sql, _eng(), params={"p": province, "d": duration_h}).iloc[0]
        v = row["last_date"]
        return v.date() if hasattr(v, "date") else (v if isinstance(v, dt.date) else None)
    except Exception:
        return None


@st.cache_data(ttl=3600)
def load_dispatch_day(_eng_key, province: str, duration_h: float, day: str):
    sql = sql_text("""
        SELECT d.datetime, d.charge_mw, d.discharge_mw, d.soc_mwh,
               p.rt_price, p.da_price
        FROM marketdata.spot_dispatch_hourly_theoretical d
        JOIN marketdata.spot_prices_hourly p
          ON p.province = d.province AND p.datetime = d.datetime
        WHERE d.province = :p AND ABS(d.duration_h - :d) < 0.01
          AND d.datetime::date = :day
        ORDER BY d.datetime
    """)
    return pd.read_sql(sql, _eng(),
                       params={"p": province, "d": duration_h, "day": day},
                       parse_dates=["datetime"])

@st.cache_data(ttl=3600)
def load_avg_economics(_eng_key, province: str, duration_h: float):
    sql = sql_text("""
        SELECT AVG(theoretical_profit_per_mwh_day) AS theo_per_mwh_day,
               AVG(NULLIF(realized_profit_per_mwh_day, 0))    AS real_per_mwh_day,
               AVG(capture_rate)                   AS capture_rate
        FROM marketdata.bess_capture_daily
        WHERE province = :p AND ABS(duration_h - :d) < 0.01
    """)
    row = pd.read_sql(sql, _eng(), params={"p": province, "d": duration_h}).iloc[0]
    return row

@st.cache_data(ttl=3600)
def load_province_list(_eng_key):
    # Use spot_prices_hourly (source of truth for ingested data) so newly uploaded
    # provinces appear in the capture pipeline selector before capture runs.
    sql = sql_text("SELECT DISTINCT province FROM marketdata.spot_prices_hourly ORDER BY 1")
    return pd.read_sql(sql, _eng())["province"].tolist()

@st.cache_data(ttl=3600)
def load_avg_cycles(_eng_key, start: str, end: str):
    """Avg daily full-cycle equivalents from LP theoretical dispatch."""
    sql = sql_text("""
        SELECT province, duration_h,
               ROUND(AVG(daily_discharge / (power_mw * duration_h))::numeric, 2) AS avg_cycles
        FROM (
            SELECT province, ts::date AS day, duration_h, power_mw,
                   SUM(GREATEST(dispatch_grid_mw, 0)) AS daily_discharge
            FROM marketdata.bess_dispatch_hourly
            WHERE ts BETWEEN :start AND :end
            GROUP BY province, ts::date, duration_h, power_mw
        ) t
        GROUP BY province, duration_h
        ORDER BY province, duration_h
    """)
    return pd.read_sql(sql, _eng(), params={"start": start, "end": end})

@st.cache_data(ttl=3600)
def load_coverage(_eng_key):
    sql = sql_text("""
        SELECT h.province,
               MAX(h.datetime)::date AS last_hourly,
               MAX(c.date)           AS last_capture,
               f.last_fund
        FROM marketdata.spot_prices_hourly h
        LEFT JOIN marketdata.bess_capture_daily c USING (province)
        LEFT JOIN (
            SELECT province, MAX(datetime)::date AS last_fund
            FROM marketdata.spot_fundamentals_hourly
            GROUP BY province
        ) f USING (province)
        GROUP BY h.province, f.last_fund
        ORDER BY h.province
    """)
    return pd.read_sql(sql, _eng(), parse_dates=["last_hourly", "last_capture", "last_fund"])

# ── Agent memory ──────────────────────────────────────────────────────────────
_APP_NAME = "bess_map"

@st.cache_resource
def _ensure_memory_table():
    """Create agent_memory table once per process — idempotent."""
    try:
        with _get_engine().begin() as conn:
            conn.execute(sql_text("""
                CREATE TABLE IF NOT EXISTS marketdata.agent_memory (
                    id         SERIAL PRIMARY KEY,
                    app        TEXT NOT NULL DEFAULT 'bess_map',
                    category   TEXT NOT NULL,
                    subject    TEXT NOT NULL,
                    content    TEXT NOT NULL,
                    source     TEXT DEFAULT 'manual',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    active     BOOLEAN DEFAULT TRUE
                )
            """))
            # Add app column if table already exists without it
            conn.execute(sql_text("""
                ALTER TABLE marketdata.agent_memory
                ADD COLUMN IF NOT EXISTS app TEXT NOT NULL DEFAULT 'bess_map'
            """))
    except Exception:
        pass  # RDS may reject DDL without superuser; fail silently
    return True

@st.cache_data(ttl=60)
def load_memories(_eng_key) -> pd.DataFrame:
    try:
        return pd.read_sql(
            sql_text("""
                SELECT id, category, subject, content, source,
                       created_at::date AS saved
                FROM marketdata.agent_memory
                WHERE active = TRUE AND app = :app
                ORDER BY created_at DESC
            """),
            _eng(),
            params={"app": _APP_NAME},
        )
    except Exception:
        return pd.DataFrame(columns=["id", "category", "subject", "content", "source", "saved"])

def _save_memory(category: str, subject: str, content: str, source: str = "manual") -> None:
    with _get_engine().begin() as conn:
        conn.execute(
            sql_text("INSERT INTO marketdata.agent_memory (app, category, subject, content, source) "
                     "VALUES (:app, :cat, :sub, :con, :src)"),
            {"app": _APP_NAME, "cat": category, "sub": subject, "con": content, "src": source},
        )
    load_memories.clear()

def _delete_memory(memory_id: int) -> None:
    with _get_engine().begin() as conn:
        conn.execute(
            sql_text("UPDATE marketdata.agent_memory SET active=FALSE WHERE id=:id AND app=:app"),
            {"id": memory_id, "app": _APP_NAME},
        )
    load_memories.clear()

# ── Geo map helpers ───────────────────────────────────────────────────────────
_GEO_FILE_BESS     = _REPO / "apps" / "bess-map"    / "data" / "china_provinces.geojson"
_GEO_FILE_FALLBACK = _REPO / "apps" / "spot-market" / "data" / "china_provinces.geojson"

_PAYBACK_COLORS = {
    "≤3yr":  "#00aa44",   # green
    "3–5yr": "#ffe000",   # yellow
    "5–7yr": "#ff6600",   # orange
    ">7yr":  "#cc2200",   # red
    "n/a":   "#d0d0d0",
}

def _payback_color(annual_rev: float | None, capex_per_kwh: float) -> str:
    """Map annual rev (¥/MWh_cap/yr) to a payback-bucket colour."""
    if annual_rev is None or annual_rev <= 0:
        return _PAYBACK_COLORS[">7yr"]
    # capex ¥/kWh × 1000 = ¥/MWh_cap
    payback = capex_per_kwh * 1000.0 / annual_rev
    if payback <= 3:
        return _PAYBACK_COLORS["≤3yr"]
    if payback <= 5:
        return _PAYBACK_COLORS["3–5yr"]
    if payback <= 7:
        return _PAYBACK_COLORS["5–7yr"]
    return _PAYBACK_COLORS[">7yr"]

@st.cache_data(ttl=None, show_spinner=False)
def _load_china_geojson_bess() -> tuple[dict | None, str | None]:
    for gf in [_GEO_FILE_BESS, _GEO_FILE_FALLBACK]:
        if gf.exists():
            try:
                return json.loads(gf.read_text(encoding="utf-8")), None
            except Exception:
                pass
    try:
        import requests as _req
        resp = _req.get(
            "https://geo.datav.aliyun.com/areas_v3/bound/100000_full.json",
            timeout=20,
        )
        resp.raise_for_status()
        gj = resp.json()
        _GEO_FILE_BESS.parent.mkdir(parents=True, exist_ok=True)
        _GEO_FILE_BESS.write_text(json.dumps(gj), encoding="utf-8")
        return gj, None
    except Exception as exc:
        return None, str(exc)


def chart_bess_revenue_map(rank_df: pd.DataFrame, duration_h: float,
                           col: str, geojson: dict | None,
                           capex_per_kwh: float = 600.0,
                           title: str | None = None) -> plt.Figure:
    """Choropleth coloured by simple capex payback period (years)."""
    sub = rank_df[abs(rank_df["duration_h"] - duration_h) < 0.01].copy()
    sub["adcode"] = sub["province"].map(_ZH_PROV_ADCODE)
    sub = sub.dropna(subset=["adcode", col])

    rev_map: dict[int, float | None] = {
        int(row["adcode"]): float(row[col]) if pd.notna(row[col]) else None
        for _, row in sub.iterrows()
    }
    # label: revenue + payback years
    label_map: dict[int, str] = {}
    for _, row in sub.iterrows():
        acode = int(row["adcode"])
        rev = float(row[col]) if pd.notna(row[col]) else None
        if rev is not None and rev > 0:
            pb = capex_per_kwh * 1000.0 / rev
            label_map[acode] = f"{rev:,.0f}\n({pb:.1f}yr)"
        elif rev is not None:
            label_map[acode] = f"{rev:,.0f}"

    _lang = st.session_state.get("lang_radio", "English")
    _rc_font = {"font.family": _CJK_FONT} if _lang == "中文" and _CJK_FONT else {}
    with plt.rc_context(_rc_font):
        return _chart_bess_revenue_map_inner(sub, rev_map, label_map, geojson,
                                             capex_per_kwh, title, duration_h)


def _chart_bess_revenue_map_inner(sub, rev_map, label_map, geojson,
                                   capex_per_kwh, title, duration_h):
    fig, ax = plt.subplots(figsize=(9, 6), facecolor="white")
    ax.set_facecolor("#b8d4f0")

    if geojson:
        for feat in geojson.get("features", []):
            adcode_int = feat.get("properties", {}).get("adcode")
            rev = rev_map.get(adcode_int)
            fc = _payback_color(rev, capex_per_kwh) if adcode_int in rev_map else "#d0d0d0"
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

    for adcode_str, centroid in _PROV_CENTROIDS_BESS.items():
        adcode_int = int(adcode_str)
        if adcode_int in label_map:
            lat, lon = centroid
            ax.text(lon, lat, label_map[adcode_int],
                    ha="center", va="center", fontsize=5.5,
                    fontweight="bold", color="black", linespacing=1.3)

    ax.set_xlim(72, 137)
    ax.set_ylim(16, 54)
    ax.set_aspect("equal")
    ax.axis("off")

    # Discrete legend patches
    from matplotlib.patches import Patch
    legend_patches = [
        Patch(facecolor=_PAYBACK_COLORS["≤3yr"],  label="≤3 yr (capex recovered)"),
        Patch(facecolor=_PAYBACK_COLORS["3–5yr"], label="3–5 yr"),
        Patch(facecolor=_PAYBACK_COLORS["5–7yr"], label="5–7 yr"),
        Patch(facecolor=_PAYBACK_COLORS[">7yr"],  label=">7 yr"),
        Patch(facecolor=_PAYBACK_COLORS["n/a"],   label="No data"),
    ]
    ax.legend(handles=legend_patches, loc="lower left", fontsize=7,
              framealpha=0.85, title=f"Payback (capex={capex_per_kwh:.0f}¥/kWh)",
              title_fontsize=7)

    ax.set_title(title or f"{duration_h:.0f}h BESS — Capex Payback by Province",
                 fontsize=11, pad=10)
    plt.tight_layout(pad=0.5)
    return fig


# ── IRR computation ────────────────────────────────────────────────────────────
def _compute_irr(cashflows: list) -> Optional[float]:
    """Newton-Raphson IRR. Returns None if no solution found."""
    if not cashflows or cashflows[0] >= 0:
        return None
    r = 0.1
    for _ in range(300):
        npv  = sum(cf / (1 + r) ** t for t, cf in enumerate(cashflows))
        dnpv = sum(-t * cf / (1 + r) ** (t + 1) for t, cf in enumerate(cashflows))
        if abs(dnpv) < 1e-12:
            break
        r -= npv / dnpv
        if r <= -1:
            return None
    return r if -1 < r < 10 else None

def _compute_npv(cashflows: list, rate: float = 0.08) -> float:
    return sum(cf / (1 + rate) ** t for t, cf in enumerate(cashflows))

def build_cashflows(
    theo_per_mwh_day: float,
    capture_rate: float,
    duration_h: float,
    capex_per_kwh: float,
    rte: float,
    om_per_kw_yr: float,
    subsidy_per_mwh: float,
    degradation: float,
    equity_pct: float,
    loan_rate: float,
    loan_tenure: int,
    project_life: int,
    power_mw: float = 1.0,
) -> tuple[list, dict]:
    """Returns (cashflows_list, annual_breakdown_dict) — normalised to 1 MW / N-hour plant."""
    e_cap = power_mw * duration_h          # MWh capacity
    capex = capex_per_kwh * e_cap * 1000   # yuan (1 MW = 1000 kW)
    equity_capex = capex * equity_pct
    debt = capex * (1 - equity_pct)
    ann_debt = (
        debt * loan_rate / (1 - (1 + loan_rate) ** (-loan_tenure))
        if debt > 0 and loan_rate > 0 else
        (debt / loan_tenure if loan_tenure > 0 else 0)
    )
    om_annual = om_per_kw_yr * power_mw          # om_per_kw_yr is actually ¥/MW/yr (param renamed for back-compat)
    # Approx: ~1 effective full cycle per day; discharge MWh ≈ e_cap × RTE
    daily_discharge = e_cap * rte
    base_rev_daily = (
        theo_per_mwh_day * capture_rate * e_cap
        + subsidy_per_mwh * daily_discharge
    )

    cfs = [-equity_capex]
    breakdown = {}
    for yr in range(1, project_life + 1):
        rev  = base_rev_daily * 365 * (1 - degradation) ** (yr - 1)
        ds   = ann_debt if yr <= loan_tenure else 0.0
        net  = rev - om_annual - ds
        cfs.append(net)
        breakdown[yr] = {"revenue": rev, "om": om_annual, "debt_svc": ds, "net": net}

    # Scale breakdown to per-MWh capacity for readability
    scale = 1.0 / e_cap if e_cap > 0 else 1.0
    bd_scaled = {
        yr: {k: v * scale for k, v in row.items()}
        for yr, row in breakdown.items()
    }
    return cfs, bd_scaled

# ── sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title(_t("app_title"))
    lang = st.radio(_t("lang_label"), ["English", "中文"], key="lang_radio", horizontal=True)
    st.divider()
    st.subheader(_t("filters"))
    _today = dt.date.today()
    _default_start = _today - dt.timedelta(days=365)
    date_range = st.date_input(
        _t("date_range"),
        value=(_default_start, _today),
        min_value=dt.date(2025, 1, 1),
        max_value=_today,
        key="date_range",
    )
    if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
        sel_start, sel_end = str(date_range[0]), str(date_range[1])
    else:
        sel_start, sel_end = str(_default_start), str(_today)

    dur_filter = st.radio(_t("duration_label"), ["2h", "4h", _t("all_durations")],
                          index=1, key="dur_filter")
    st.divider()
    _MODEL_OPTS = {
        "ols_rt_time_v1":      _t("model_ols_time"),
        "naive_rt_ar17":       _t("model_naive_ar17"),
        "ols_fundamentals_v1": _t("model_ols_fund"),
    }
    _model_label = st.selectbox(
        _t("model_selector_label"),
        options=list(_MODEL_OPTS.values()),
        index=0,
        key="model_selector",
    )
    sel_model = next(k for k, v in _MODEL_OPTS.items() if v == _model_label)
    st.divider()
    forecast_method = st.radio(
        _t("forecast_method_label"),
        [_t("forecast_theoretical"), _t("forecast_realized")],
        index=0, key="forecast_method",
    )

profit_col = (
    "theoretical_profit_per_mwh_day"
    if st.session_state.get("forecast_method", _t("forecast_theoretical")) == _t("forecast_theoretical")
    else "realized_profit_per_mwh_day"
)
rank_annual_col = "annual_theo" if profit_col == "theoretical_profit_per_mwh_day" else "annual_real"

# ── Agent base system prompt ──────────────────────────────────────────────────
_AGENT_BASE_SYSTEM = """\
You are a specialist BESS (Battery Energy Storage System) investment analyst for PJH ETRM, \
focused exclusively on China's provincial electricity spot markets.

## Grounding rules
- You ONLY use data returned by the tools provided or facts explicitly stated in this conversation.
- NEVER cite external reports, news, pricing forecasts, or general market knowledge you were not given.
- If you lack data to answer a question, say so and suggest which tool to call.

## Domain definitions (use these consistently)
- Revenue unit: ¥/MWh of INSTALLED CAPACITY per day (= power_MW × duration_h MWh capacity).
  A 100 MW / 4h plant has 400 MWh installed capacity. Revenue ¥/MWh_cap/day × 400 × 365 = annual ¥.
- Capture rate = realized OLS-forecast revenue ÷ theoretical LP perfect-foresight revenue.
  Capture rate > 80% over 3+ months = operationally strong signal. < 60% = concern.
- Simple payback = capex (¥/kWh × 1000 ¥/MWh) ÷ annual_revenue_per_MWh_cap.
- O&M baseline: 24,000 ¥/MW/year (NOT per kW).
- Capex range: 400–600 ¥/kWh for LFP, 600–900 ¥/kWh for premium/longer-duration.
- Primary duration targets: 2h (morning/evening peak arb) and 4h (full intraday arb).
- Preferred provinces for 4h BESS: screen by annual theoretical revenue > 100,000 ¥/MWh_cap/yr.

## Analytical framework
1. Province screening → use get_bess_economics to rank by annual revenue.
2. Dispatch quality → use get_dispatch_detail to verify charge/discharge pattern on a representative day.
3. Financial case → use get_irr_estimate; flag IRR < 8% as marginal, < 0% as rejected.
4. Always state: province, duration, revenue basis (theoretical vs realised), date range used.
5. Quote numbers with full units. Flag if data coverage is sparse (days < 180).
"""

# ── tabs ──────────────────────────────────────────────────────────────────────
tab_ranking, tab_geo, tab_dispatch, tab_irr, tab_mgmt, tab_agent = st.tabs([
    _t("tab_ranking"), _t("tab_geo"), _t("tab_dispatch"), _t("tab_irr"),
    _t("tab_mgmt"), _t("tab_agent"),
])

_ENG_KEY = "bess_map"   # hashable cache-bust token (stable)

# ── Tab 1: Province Ranking ───────────────────────────────────────────────────
with tab_ranking:
    st.subheader(_t("rank_title"))
    st.caption(_t("rank_caption"))

    rank_df = load_province_ranking(_ENG_KEY, sel_start, sel_end, sel_model)

    if rank_df.empty:
        st.warning("No data in bess_capture_daily for this period.")
    else:
        # Pivot to wide: province | 2h | 4h | capture (avg)
        r2 = rank_df[rank_df["duration_h"] == 2.0][["province", "annual_theo", "annual_real", "capture_pct"]].rename(
            columns={"annual_theo": "anno_2h_theo", "annual_real": "anno_2h_real", "capture_pct": "cap_2h"})
        r4 = rank_df[rank_df["duration_h"] == 4.0][["province", "annual_theo", "annual_real", "capture_pct"]].rename(
            columns={"annual_theo": "anno_4h_theo", "annual_real": "anno_4h_real", "capture_pct": "cap_4h"})
        wide = r2.merge(r4, on="province", how="outer")

        # pick primary revenue column based on forecast_method
        sort_2h = "anno_2h_theo" if rank_annual_col == "annual_theo" else "anno_2h_real"
        sort_4h = "anno_4h_theo" if rank_annual_col == "annual_theo" else "anno_4h_real"
        wide = wide.sort_values(sort_4h, ascending=False)

        # avg daily cycles
        cycles_df = load_avg_cycles(_ENG_KEY, sel_start, sel_end)
        cy4 = cycles_df[abs(cycles_df["duration_h"] - 4.0) < 0.01].set_index("province")["avg_cycles"]
        avg_cycles_4h = cy4.mean() if not cy4.empty else None

        # KPI strip
        k1, k2, k3, k4 = st.columns(4)
        if not wide.empty:
            _w2 = wide.dropna(subset=[sort_2h])
            _w4 = wide.dropna(subset=[sort_4h])
            best2 = _w2.iloc[0] if not _w2.empty else None
            best4 = _w4.iloc[0] if not _w4.empty else None
            avg_cap = rank_df["capture_pct"].dropna()
            k1.metric(_t("rank_kpi_2h"),
                      f"{best2['province']}  ¥{best2[sort_2h]:,.0f}" if best2 is not None else "—")
            k2.metric(_t("rank_kpi_4h"),
                      f"{best4['province']}  ¥{best4[sort_4h]:,.0f}" if best4 is not None else "—")
            k3.metric(_t("rank_kpi_capture"), f"{avg_cap.mean():.1f}%" if not avg_cap.empty else "—")
            k4.metric(_t("rank_kpi_cycles"),
                      f"{avg_cycles_4h:.2f}/day" if avg_cycles_4h is not None else "—")

        # Bar chart: always sort by annual_theo so ordering is stable regardless of model coverage
        plot_df = rank_df.copy()
        plot_df["Duration"] = plot_df["duration_h"].map({2.0: "2h", 4.0: "4h"})
        if dur_filter != _t("all_durations"):
            plot_df = plot_df[plot_df["Duration"] == dur_filter]
        plot_df = plot_df.sort_values("annual_theo", ascending=True)

        fig_rank = px.bar(
            plot_df, x=rank_annual_col, y="province", color="Duration",
            orientation="h", barmode="group",
            color_discrete_map={"2h": "#4CAF50", "4h": "#1565C0"},
            labels={rank_annual_col: "Annual Rev (¥/MWh/yr)", "province": ""},
            title=_t("rank_chart_title"),
        )
        fig_rank.update_layout(height=max(400, len(wide) * 26), margin=dict(t=40, b=20),
                                legend_title_text="Duration")
        st.plotly_chart(fig_rank, use_container_width=True)

        # Ranking table with cycles
        disp_wide = wide.copy()
        if not cycles_df.empty:
            cy2 = cycles_df[abs(cycles_df["duration_h"] - 2.0) < 0.01].set_index("province")["avg_cycles"]
            disp_wide["cycles_2h"] = disp_wide["province"].map(cy2)
            disp_wide["cycles_4h"] = disp_wide["province"].map(cy4)
        else:
            disp_wide["cycles_2h"] = None
            disp_wide["cycles_4h"] = None

        out = disp_wide[[
            "province", sort_2h, "cap_2h", "cycles_2h",
            sort_4h, "cap_4h", "cycles_4h"
        ]].copy()
        out.columns = [
            _t("rank_col_province"),
            "2h Rev", "2h Cap%", "2h Cycles",
            "4h Rev", "4h Cap%", "4h Cycles",
        ]
        for col in ["2h Rev", "4h Rev"]:
            out[col] = out[col].apply(lambda v: f"¥{v:,.0f}" if pd.notna(v) else "—")
        for col in ["2h Cap%", "4h Cap%"]:
            out[col] = out[col].apply(lambda v: f"{v:.1f}%" if pd.notna(v) else "—")
        for col in ["2h Cycles", "4h Cycles"]:
            out[col] = out[col].apply(lambda v: f"{v:.2f}" if pd.notna(v) else "—")
        st.dataframe(out, use_container_width=True, hide_index=True)

        st.divider()

        # Intraday spread section
        st.subheader(_t("rank_spread_title"))
        st.caption(_t("rank_spread_caption"))
        spread_df = load_intraday_spread(_ENG_KEY, sel_start, sel_end)
        if not spread_df.empty:
            fig_sp = px.bar(
                spread_df, x="spread", y="province", orientation="h",
                color="spread", color_continuous_scale="Blues",
                labels={"spread": "RT Intraday Spread (¥/kWh)", "province": ""},
            )
            fig_sp.update_layout(
                height=max(300, len(spread_df) * 22),
                margin=dict(t=10, b=10),
                showlegend=False, coloraxis_showscale=False,
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_sp, use_container_width=True)

# ── Tab 2: Geo Map ────────────────────────────────────────────────────────────
with tab_geo:
    st.caption(
        "Colour = simple payback period (annual revenue ÷ capex).  "
        "Revenue = ¥/MWh of **installed capacity** (power × duration) per year."
    )
    geo_capex = st.slider("Assumed capex for payback (¥/kWh)", 400, 900, 600, step=25,
                          key="geo_capex")
    geo_rank_df = load_province_ranking(_ENG_KEY, sel_start, sel_end, sel_model)
    _geojson_bess, _geo_err = _load_china_geojson_bess()
    if _geo_err:
        st.warning(f"{_t('geo_unavailable')} ({_geo_err})")

    if not geo_rank_df.empty and _geojson_bess:
        col_2h, col_4h = st.columns(2)
        with col_2h:
            st.subheader(_t("geo_2h_title"))
            fig_geo2 = chart_bess_revenue_map(
                geo_rank_df, 2.0, rank_annual_col, _geojson_bess,
                capex_per_kwh=geo_capex,
                title=_t("geo_2h_title"),
            )
            st.pyplot(fig_geo2, use_container_width=True)
            plt.close(fig_geo2)
        with col_4h:
            st.subheader(_t("geo_4h_title"))
            fig_geo4 = chart_bess_revenue_map(
                geo_rank_df, 4.0, rank_annual_col, _geojson_bess,
                capex_per_kwh=geo_capex,
                title=_t("geo_4h_title"),
            )
            st.pyplot(fig_geo4, use_container_width=True)
            plt.close(fig_geo4)

        st.caption(
            f"Revenue basis: **{_t('forecast_theoretical') if rank_annual_col == 'annual_theo' else _t('forecast_realized')}** · "
            f"{sel_start} → {sel_end} · Payback = capex ({geo_capex} ¥/kWh × 1000) ÷ annual rev"
        )
    elif geo_rank_df.empty:
        st.warning("No ranking data available for this period.")

# ── Tab 3: Dispatch & Economics ───────────────────────────────────────────────
with tab_dispatch:
    all_provs = load_province_list(_ENG_KEY)
    col_dp, col_dd, col_dr = st.columns([2, 1, 3])
    with col_dp:
        disp_prov = st.selectbox(_t("disp_province"), all_provs, key="disp_prov")
    with col_dd:
        disp_dur = st.radio(_t("disp_duration"), ["2h", "4h"], key="disp_dur", index=1)
    with col_dr:
        disp_dr = st.date_input(
            _t("disp_date_range"),
            value=(dt.date(2025, 1, 1), dt.date.today()),
            key="disp_dr",
        )
    if isinstance(disp_dr, (list, tuple)) and len(disp_dr) == 2:
        d_start, d_end = str(disp_dr[0]), str(disp_dr[1])
    else:
        d_start, d_end = "2025-01-01", "2026-01-31"
    disp_dur_h = 2.0 if disp_dur == "2h" else 4.0

    monthly = load_monthly_economics(_ENG_KEY, disp_prov, disp_dur_h, d_start, d_end, sel_model)

    if monthly.empty:
        st.warning("No data for this selection.")
    else:
        # Monthly theo vs realized revenue
        st.subheader(_t("disp_monthly_title"))
        fig_mo = go.Figure()
        fig_mo.add_bar(x=monthly["month"], y=monthly["theo_avg"], name="Theoretical",
                       marker_color="#1565C0")
        fig_mo.add_bar(x=monthly["month"], y=monthly["real_avg"], name="Realized",
                       marker_color="#4CAF50")
        fig_mo.update_layout(barmode="group", height=300, margin=dict(t=20, b=20),
                              yaxis_title="¥/MWh/day", xaxis_title="")
        st.plotly_chart(fig_mo, use_container_width=True)

        # Capture rate trend
        st.subheader(_t("disp_capture_title"))
        fig_cap = px.line(monthly, x="month", y="capture_pct",
                          labels={"month": "", "capture_pct": "Capture rate (%)"})
        fig_cap.update_layout(height=200, margin=dict(t=10, b=10))
        fig_cap.add_hline(y=100, line_dash="dot", line_color="grey")
        st.plotly_chart(fig_cap, use_container_width=True)

    st.divider()

    # Dispatch detail: single day
    st.subheader(_t("disp_detail_title"))
    _last_disp = load_last_dispatch_date(_ENG_KEY, disp_prov, disp_dur_h)
    _detail_default = _last_disp if _last_disp else dt.date.today() - dt.timedelta(days=7)
    detail_date = st.date_input(_t("disp_detail_date"),
                                value=_detail_default, key="detail_date")
    detail_df = load_dispatch_day(_ENG_KEY, disp_prov, disp_dur_h, str(detail_date))

    if detail_df.empty:
        st.info(_t("disp_no_dispatch"))
    else:
        detail_df["hour"] = detail_df["datetime"].dt.hour
        fig_det = make_subplots(rows=2, cols=1, shared_xaxes=True,
                                row_heights=[0.65, 0.35],
                                specs=[[{"secondary_y": True}], [{"secondary_y": False}]])

        fig_det.add_trace(
            go.Bar(x=detail_df["hour"], y=detail_df["discharge_mw"],
                   name=_t("disp_discharge"), marker_color="#4CAF50"),
            row=1, col=1,
        )
        fig_det.add_trace(
            go.Bar(x=detail_df["hour"], y=-detail_df["charge_mw"],
                   name=_t("disp_charge"), marker_color="#E53935"),
            row=1, col=1,
        )
        fig_det.add_trace(
            go.Scatter(x=detail_df["hour"], y=detail_df["rt_price"],
                       name=_t("disp_rt_price"), line=dict(color="orange", width=2)),
            row=1, col=1, secondary_y=True,
        )
        fig_det.add_trace(
            go.Scatter(x=detail_df["hour"], y=detail_df["soc_mwh"],
                       name=_t("disp_soc"), fill="tozeroy",
                       line=dict(color="#1565C0")),
            row=2, col=1,
        )
        fig_det.update_yaxes(title_text="MW", row=1, col=1, secondary_y=False)
        fig_det.update_yaxes(title_text="¥/kWh", row=1, col=1, secondary_y=True)
        fig_det.update_yaxes(title_text="MWh", row=2, col=1)
        fig_det.update_layout(height=450, barmode="relative",
                               margin=dict(t=20, b=20), legend=dict(orientation="h"))
        st.plotly_chart(fig_det, use_container_width=True)

# ── Tab 3: IRR Calculator ─────────────────────────────────────────────────────
with tab_irr:
    st.subheader(_t("irr_title"))
    st.caption(_t("irr_caption"))

    all_provs_irr = load_province_list(_ENG_KEY)
    col_irr_in, col_irr_out = st.columns([1, 1.4])

    with col_irr_in:
        irr_prov = st.selectbox(_t("irr_province"), all_provs_irr, key="irr_prov")
        irr_dur  = st.radio(_t("irr_duration"), ["2h", "4h"], key="irr_dur", index=1, horizontal=True)
        irr_dur_h = 2.0 if irr_dur == "2h" else 4.0

        # Revenue basis from DB — respect forecast_method selection
        econ = load_avg_economics(_ENG_KEY, irr_prov, irr_dur_h)
        theo_day  = float(econ["theo_per_mwh_day"] or 0)
        real_day_ = float(econ["real_per_mwh_day"]) if pd.notna(econ["real_per_mwh_day"]) else 0.0
        cap_rate  = float(econ["capture_rate"]) if pd.notna(econ["capture_rate"]) else 0.0
        # For IRR: theoretical mode uses theo_day as-is (capture_rate=1.0 passed to build_cashflows)
        # Realized mode uses real_per_mwh_day directly
        if profit_col == "theoretical_profit_per_mwh_day":
            irr_rev_day = theo_day
            irr_cap_rate = 1.0
        else:
            irr_rev_day = real_day_
            irr_cap_rate = 1.0
        real_day = theo_day * cap_rate  # display only

        if theo_day == 0:
            st.warning(_t("irr_no_data"))
        else:
            method_label = _t("forecast_theoretical") if profit_col == "theoretical_profit_per_mwh_day" else _t("forecast_realized")
            st.success(
                f"**{_t('irr_rev_basis')}** [{method_label}] — {irr_prov} {irr_dur}  \n"
                f"{_t('irr_theo_day')}: ¥{theo_day:.2f}  |  "
                f"{_t('irr_capture')}: {cap_rate*100:.1f}%  |  "
                f"{_t('irr_real_day')}: ¥{real_day:.2f}  |  "
                f"**IRR basis: ¥{irr_rev_day:.2f}/MWh/day**"
            )

        st.divider()
        capex   = st.slider(_t("irr_capex"),        400, 900, 600, step=25)
        rte_pct = st.slider(_t("irr_rte"),          70,  95,  85, step=1)
        om      = st.number_input(_t("irr_om"),      value=24000, step=1000)
        subsidy = st.number_input(_t("irr_subsidy"), value=0,     step=50)
        dgrad   = st.slider(_t("irr_degradation"),  0,   5,   2,  step=1) / 100.0

        st.divider()
        equity  = st.slider(_t("irr_equity"),       20,  100, 30, step=5) / 100.0
        lr_pct  = st.slider(_t("irr_loan_rate"),    3,   10,  5,  step=1) / 100.0
        tenure  = st.slider(_t("irr_loan_tenure"),  5,   15,  10, step=1)
        life    = st.slider(_t("irr_life"),          10,  25,  15, step=1)

        calc = st.button(_t("irr_calc_btn"), use_container_width=True, type="primary")

    with col_irr_out:
        if calc and theo_day > 0:
            cfs, bd = build_cashflows(
                theo_per_mwh_day=irr_rev_day,
                capture_rate=irr_cap_rate,
                duration_h=irr_dur_h,
                capex_per_kwh=capex,
                rte=rte_pct / 100.0,
                om_per_kw_yr=om,
                subsidy_per_mwh=subsidy,
                degradation=dgrad,
                equity_pct=equity,
                loan_rate=lr_pct,
                loan_tenure=tenure,
                project_life=life,
            )

            irr_val  = _compute_irr(cfs)
            npv_val  = _compute_npv(cfs, 0.08)
            cum = 0.0
            payback = None
            for yr, cf in enumerate(cfs[1:], start=1):
                cum += cf
                if cum >= 0 and payback is None:
                    payback = yr

            # KPI strip
            r1, r2, r3 = st.columns(3)
            irr_str = f"{irr_val*100:.1f}%" if irr_val is not None else "N/A"
            r1.metric(_t("irr_result_irr"), irr_str)
            r2.metric(_t("irr_result_payback"),
                      f"{payback}{_t('irr_result_years')}" if payback else "N/A")
            r3.metric(_t("irr_result_npv"),
                      f"¥{npv_val/1e6:.2f}M" if abs(npv_val) >= 1e5 else f"¥{npv_val:,.0f}")

            if irr_val is not None and irr_val < 0:
                st.warning(_t("irr_negative_irr"))

            # Cashflow waterfall
            years  = list(bd.keys())
            rev_s  = [bd[y]["revenue"]  for y in years]
            om_s   = [-bd[y]["om"]       for y in years]
            debt_s = [-bd[y]["debt_svc"] for y in years]
            net_s  = [bd[y]["net"]       for y in years]

            st.subheader(_t("irr_cashflow_title"))
            fig_cf = go.Figure()
            fig_cf.add_bar(x=years, y=rev_s,  name=_t("irr_cf_revenue"),
                           marker_color="#4CAF50")
            fig_cf.add_bar(x=years, y=om_s,   name=_t("irr_cf_om"),
                           marker_color="#E53935")
            fig_cf.add_bar(x=years, y=debt_s, name=_t("irr_cf_debt"),
                           marker_color="#FF7043")
            fig_cf.add_scatter(x=years, y=net_s, name=_t("irr_cf_net"),
                               line=dict(color="navy", width=2), mode="lines+markers")
            fig_cf.update_layout(barmode="relative", height=320,
                                  margin=dict(t=10, b=10),
                                  yaxis_title="¥/MWh capacity",
                                  legend=dict(orientation="h"))
            st.plotly_chart(fig_cf, use_container_width=True)

            # Sensitivity table
            st.subheader(_t("irr_sensitivity"))
            # Fixed capex rows (¥/kWh); rev multipliers relative to IRR basis
            capex_scenarios = [400, 500, 600, 700, 800, 900, 1000, 1200]
            rev_multipliers  = [0.7, 0.85, 1.0, 1.15, 1.3, 1.5]
            sens_rows = {}
            for cx in capex_scenarios:
                row = {}
                for rm in rev_multipliers:
                    cfs_s, _ = build_cashflows(
                        theo_per_mwh_day=irr_rev_day * rm,
                        capture_rate=irr_cap_rate,      # always 1.0 — irr_rev_day already captures method
                        duration_h=irr_dur_h,
                        capex_per_kwh=cx,
                        rte=rte_pct / 100.0,
                        om_per_kw_yr=om,
                        subsidy_per_mwh=subsidy,
                        degradation=dgrad,
                        equity_pct=equity,
                        loan_rate=lr_pct,
                        loan_tenure=tenure,
                        project_life=life,
                    )
                    irr_s = _compute_irr(cfs_s)
                    row[f"{rm*100:.0f}%"] = f"{irr_s*100:.1f}%" if irr_s is not None else "N/A"
                sens_rows[f"¥{cx}/kWh"] = row
            sens_df = pd.DataFrame(sens_rows).T
            sens_df.index.name = "Capex \\ Rev mult →"
            st.dataframe(sens_df, use_container_width=True)
        elif not calc:
            st.info(_t("irr_calc_btn") + " ←")

# ── Tab 4: Data Management ────────────────────────────────────────────────────
with tab_mgmt:
    st.subheader(_t("mgmt_title"))

    # S3 bucket
    S3_BUCKET = os.environ.get("S3_BUCKET") or os.environ.get("UPLOADS_BUCKET_NAME")
    try:
        import boto3
        _s3 = boto3.client("s3") if S3_BUCKET else None
    except ImportError:
        _s3 = None

    # Upload
    if "mgmt_uploaded_names" not in st.session_state:
        st.session_state["mgmt_uploaded_names"] = []

    st.subheader(_t("mgmt_upload_title"))
    uploaded = st.file_uploader(
        _t("mgmt_upload_help"), type="xlsx", accept_multiple_files=True,
        key="mgmt_upload",
    )
    _local_upload_dir = Path(tempfile.gettempdir()) / "bess_uploads"
    if uploaded:
        if _s3 and S3_BUCKET:
            for f in uploaded:
                _s3.upload_fileobj(f, S3_BUCKET, f"uploads/{f.name}")
            st.session_state["mgmt_uploaded_names"] = [f.name for f in uploaded]
            st.success(f"Uploaded {len(uploaded)} file(s) to S3.")
        else:
            _local_upload_dir.mkdir(parents=True, exist_ok=True)
            for f in uploaded:
                (_local_upload_dir / f.name).write_bytes(f.read())
            st.session_state["mgmt_uploaded_names"] = [f.name for f in uploaded]
            st.success(f"Saved {len(uploaded)} file(s) locally (S3 not configured).")

    # Ingest uploaded files → DB
    st.divider()
    st.subheader(_t("mgmt_ingest_title"))
    _uploaded_names = st.session_state["mgmt_uploaded_names"]
    if _uploaded_names:
        st.caption(f"Files from this session: {', '.join(_uploaded_names)}")
    if st.button(_t("mgmt_ingest_btn"), key="mgmt_ingest_run"):
        if not _uploaded_names:
            st.warning(_t("mgmt_ingest_no_files"))
        else:
            _ingest_script = _REPO / "services" / "bess_map" / "run_all_provinces.py"
            if not _ingest_script.exists():
                st.error(f"Script not found: {_ingest_script}")
            else:
                _local_dir = _local_upload_dir
                _local_dir.mkdir(parents=True, exist_ok=True)
                if _s3 and S3_BUCKET:
                    # Download uploaded files from S3
                    with st.spinner("Downloading files from S3..."):
                        for _fname in _uploaded_names:
                            _s3.download_file(S3_BUCKET, f"uploads/{_fname}",
                                              str(_local_dir / _fname))
                _cmd = [sys.executable, str(_ingest_script),
                        "--indir", str(_local_dir),
                        "--auto-cols", "--upload-db",
                        "--env", "none", "--schema", "marketdata",
                        "--continue-on-error"]
                st.caption(f"Running: {' '.join(_cmd)}")
                _log_area = st.empty()
                _proc = subprocess.Popen(_cmd, stdout=subprocess.PIPE,
                                         stderr=subprocess.STDOUT, text=True,
                                         cwd=str(_REPO),
                                         env={**os.environ, "PYTHONPATH": str(_REPO)})
                _buf = ""
                for _line in _proc.stdout:
                    _buf += _line
                    _log_area.code(_buf[-8000:])
                _proc.wait()
                if _proc.returncode != 0:
                    st.error(f"Ingestion failed (rc={_proc.returncode})")
                else:
                    st.success(f"Ingested {len(_uploaded_names)} file(s) into DB.")
                    st.session_state["mgmt_uploaded_names"] = []
                load_coverage.clear()
                st.cache_data.clear()

    # ── Fundamentals ingest ──────────────────────────────────────────────────
    st.divider()
    st.subheader(_t("mgmt_fund_title"))
    if _uploaded_names:
        st.caption(f"Files from this session: {', '.join(_uploaded_names)}")
    if st.button(_t("mgmt_fund_btn"), key="mgmt_fund_run"):
        if not _uploaded_names:
            st.warning(_t("mgmt_fund_no_files"))
        else:
            _fund_script = _REPO / "services" / "bess_map" / "run_fundamentals_ingest.py"
            if not _fund_script.exists():
                st.error(f"Script not found: {_fund_script}")
            else:
                _local_dir = _local_upload_dir
                _local_dir.mkdir(parents=True, exist_ok=True)
                if _s3 and S3_BUCKET:
                    with st.spinner("Downloading files from S3..."):
                        for _fname in _uploaded_names:
                            _s3.download_file(S3_BUCKET, f"uploads/{_fname}",
                                              str(_local_dir / _fname))
                _cmd2 = [sys.executable, str(_fund_script),
                         "--indir", str(_local_dir),
                         "--env", "none", "--schema", "marketdata",
                         "--continue-on-error"]
                st.caption(f"Running: {' '.join(_cmd2)}")
                _log_area2 = st.empty()
                _proc2 = subprocess.Popen(_cmd2, stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT, text=True,
                                          cwd=str(_REPO),
                                          env={**os.environ, "PYTHONPATH": str(_REPO)})
                _buf2 = ""
                for _line2 in _proc2.stdout:
                    _buf2 += _line2
                    _log_area2.code(_buf2[-8000:])
                _proc2.wait()
                if _proc2.returncode != 0:
                    st.error(f"Fundamentals ingest failed (rc={_proc2.returncode})")
                else:
                    st.success(f"Fundamentals ingested for {len(_uploaded_names)} province(s).")
                load_coverage.clear()
                st.cache_data.clear()

    # DB coverage
    st.divider()
    st.subheader(_t("mgmt_coverage_title"))
    cov = load_coverage(_ENG_KEY)
    if not cov.empty:
        today_dt = dt.date.today()
        def _status(row):
            if pd.isna(row["last_capture"]):
                return _t("mgmt_status_missing")
            lag = (today_dt - row["last_capture"].date()).days if pd.notna(row["last_capture"]) else 999
            return _t("mgmt_status_ok") if lag <= 30 else _t("mgmt_status_stale")
        cov["status"] = cov.apply(_status, axis=1)
        cov.columns = [_t("mgmt_col_province"), _t("mgmt_col_last_hourly"),
                       _t("mgmt_col_last_capture"), _t("mgmt_col_last_fund"),
                       _t("mgmt_col_status")]
        st.dataframe(cov, use_container_width=True, hide_index=True)

    # Capture pipeline runner
    st.divider()
    st.subheader(_t("mgmt_capture_title"))
    _all_provs_for_cap = load_province_list(_ENG_KEY)
    cap_provs_sel = st.multiselect(
        _t("mgmt_capture_provs"),
        options=_all_provs_for_cap,
        placeholder="Leave empty to run all provinces",
        key="cap_provs_sel",
    )
    cap_dur   = st.radio(_t("mgmt_capture_dur"), ["2h", "4h", "Both"], horizontal=True, key="cap_dur")
    cap_force = st.checkbox(_t("mgmt_capture_force"), key="cap_force")
    cap_model = st.selectbox(
        _t("model_selector_label"),
        options=list(_MODEL_OPTS.keys()),
        format_func=lambda k: _MODEL_OPTS[k],
        index=0,
        key="cap_model_sel",
    )

    if st.button(_t("mgmt_capture_btn"), type="primary"):
        _pipeline = _REPO / "services" / "bess_map" / "run_capture_pipeline.py"
        if not _pipeline.exists():
            st.error(f"Pipeline script not found: {_pipeline}")
        else:
            durations = ["2", "4"] if cap_dur == "Both" else [cap_dur.replace("h", "")]
            log_area = st.empty()
            for dur in durations:
                cmd = [sys.executable, str(_pipeline),
                       "--env", "none", "--schema", "marketdata",
                       "--duration-h", dur,
                       "--model", cap_model]
                if cap_provs_sel:
                    cmd += ["--province-list", ",".join(cap_provs_sel)]
                if cap_force:
                    cmd += ["--force", "--force-theoretical"]
                st.caption(f"Running: {' '.join(cmd)}")
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                        stderr=subprocess.STDOUT, text=True,
                                        cwd=str(_REPO),
                                        env={**os.environ, "PYTHONPATH": str(_REPO)})
                buf = ""
                for line in proc.stdout:
                    buf += line
                    log_area.code(buf[-8000:])
                proc.wait()
                if proc.returncode != 0:
                    st.error(f"Pipeline failed (rc={proc.returncode})")
                else:
                    st.success(f"{dur}h pipeline completed.")
            load_coverage.clear()
            st.cache_data.clear()

# ── Tab 6: Agent ──────────────────────────────────────────────────────────────
with tab_agent:
    _ensure_memory_table()  # deferred: runs once, only when agent tab is visited

    import anthropic as _ant

    _api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not _api_key:
        st.error(_t("agent_no_key"))
        st.stop()

    _client = _ant.Anthropic(api_key=_api_key)

    # ── session state init ────────────────────────────────────────────────────
    if "bess_agent_msgs" not in st.session_state:
        st.session_state["bess_agent_msgs"] = []
    if "bess_mem_suggestions" not in st.session_state:
        st.session_state["bess_mem_suggestions"] = []   # list of {category,subject,content}

    # ── build system prompt: base + injected memories ─────────────────────────
    def _build_system() -> str:
        _lang_hint = "\n\nRespond in Simplified Chinese for all answers." if st.session_state.get("lang_radio") == "中文" else ""
        mem_df = load_memories(_ENG_KEY)
        if mem_df.empty:
            mem_block = ""
        else:
            lines = [f"[{r.category}] {r.subject}: {r.content}" for r in mem_df.itertuples()]
            mem_block = "\n\n## Your memory from prior sessions (treat as established context):\n" + "\n".join(lines)
        return _AGENT_BASE_SYSTEM + mem_block + _lang_hint

    # ── agent tools ───────────────────────────────────────────────────────────
    _TOOLS = [
        {
            "name": "get_bess_economics",
            "description": "Get province-level BESS economics: annual theoretical and realised revenue per MWh of installed capacity, capture rate, avg daily cycles. Use this first when screening provinces.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "YYYY-MM-DD"},
                    "end_date":   {"type": "string", "description": "YYYY-MM-DD"},
                    "duration_h": {"type": "number", "description": "2 or 4 — omit for both"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "get_dispatch_detail",
            "description": "Get hourly LP-theoretical dispatch (charge MW, discharge MW, SoC MWh, RT price) for a province on a specific date. Use to verify dispatch quality.",
            "input_schema": {
                "type": "object",
                "properties": {
                    "province":   {"type": "string"},
                    "duration_h": {"type": "number", "description": "2 or 4"},
                    "date":       {"type": "string", "description": "YYYY-MM-DD"},
                },
                "required": ["province", "duration_h", "date"],
            },
        },
        {
            "name": "get_irr_estimate",
            "description": "Calculate BESS equity IRR, simple payback, and NPV for a province. Revenue basis pulled from DB. O&M is in ¥/MW/year (default 24000).",
            "input_schema": {
                "type": "object",
                "properties": {
                    "province":           {"type": "string"},
                    "duration_h":         {"type": "number", "description": "2 or 4"},
                    "capex_yuan_per_kwh": {"type": "number", "description": "¥/kWh, e.g. 600"},
                    "rte_pct":            {"type": "number", "description": "Round-trip efficiency %, default 85"},
                    "om_per_mw_yr":       {"type": "number", "description": "O&M ¥/MW/year, default 24000"},
                    "subsidy_per_mwh":    {"type": "number", "description": "Discharge subsidy ¥/MWh, default 0"},
                    "degradation_pct":    {"type": "number", "description": "Annual capacity fade %, default 2"},
                    "equity_pct":         {"type": "number", "description": "Equity share %, default 30"},
                    "loan_rate_pct":      {"type": "number", "description": "Loan rate %, default 5.5"},
                    "loan_tenure":        {"type": "integer", "description": "Loan years, default 10"},
                    "project_life":       {"type": "integer", "description": "Project life years, default 15"},
                    "use_realised":       {"type": "boolean", "description": "True = use realised OLS revenue; False (default) = theoretical"},
                },
                "required": ["province", "duration_h", "capex_yuan_per_kwh"],
            },
        },
    ]

    def _dispatch_tool(name: str, inp: dict) -> str:
        if name == "get_bess_economics":
            df = load_province_ranking(
                _ENG_KEY,
                inp.get("start_date", "2025-01-01"),
                inp.get("end_date", str(dt.date.today())),
                sel_model,
            )
            if inp.get("duration_h"):
                df = df[abs(df["duration_h"] - float(inp["duration_h"])) < 0.01]
            return df.to_json(orient="records", default_handler=str)

        elif name == "get_dispatch_detail":
            df = load_dispatch_day(
                _ENG_KEY,
                inp["province"],
                float(inp.get("duration_h", 4.0)),
                inp["date"],
            )
            return df.head(24).to_json(orient="records", default_handler=str)

        elif name == "get_irr_estimate":
            econ = load_avg_economics(_ENG_KEY, inp["province"],
                                      float(inp.get("duration_h", 4.0)))
            td = float(econ["theo_per_mwh_day"] or 0)
            rd = float(econ["real_per_mwh_day"]) if pd.notna(econ["real_per_mwh_day"]) else 0.0
            rev_day = rd if inp.get("use_realised") else td
            cfs, _ = build_cashflows(
                theo_per_mwh_day=rev_day,
                capture_rate=1.0,
                duration_h=float(inp.get("duration_h", 4.0)),
                capex_per_kwh=float(inp.get("capex_yuan_per_kwh", 600)),
                rte=float(inp.get("rte_pct", 85)) / 100,
                om_per_kw_yr=float(inp.get("om_per_mw_yr", 24000)),
                subsidy_per_mwh=float(inp.get("subsidy_per_mwh", 0)),
                degradation=float(inp.get("degradation_pct", 2)) / 100,
                equity_pct=float(inp.get("equity_pct", 30)) / 100,
                loan_rate=float(inp.get("loan_rate_pct", 5.5)) / 100,
                loan_tenure=int(inp.get("loan_tenure", 10)),
                project_life=int(inp.get("project_life", 15)),
            )
            irr = _compute_irr(cfs)
            npv = _compute_npv(cfs, 0.08)
            cum, payback = 0.0, None
            for yr, cf in enumerate(cfs[1:], 1):
                cum += cf
                if cum >= 0 and payback is None:
                    payback = yr
            return str({
                "province": inp["province"], "duration_h": inp.get("duration_h"),
                "revenue_basis": "realised" if inp.get("use_realised") else "theoretical",
                "rev_per_mwh_cap_day": round(rev_day, 2),
                "irr_pct": round(irr * 100, 2) if irr is not None else None,
                "simple_payback_yr": payback,
                "npv_yuan": round(npv, 0),
            })
        return "Unknown tool"

    # ── auto-extract helper ────────────────────────────────────────────────────
    def _extract_memories(user_msg: str, agent_reply: str) -> list[dict]:
        """Ask Haiku to extract saveable facts from this exchange. Returns list of dicts."""
        try:
            extract_resp = _client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=600,
                system=(
                    "You extract key investment facts, views, and methodology decisions from "
                    "BESS analyst conversations to build a persistent memory. "
                    "Output ONLY a JSON array (no markdown). Each item: "
                    "{\"category\": one of [market_view, methodology, province_note, red_flag, investment_thesis], "
                    "\"subject\": short title (≤60 chars), \"content\": the key fact or view (≤200 chars)}. "
                    "Return [] if nothing worth persisting."
                ),
                messages=[{"role": "user", "content":
                    f"User said: {user_msg}\n\nAgent replied: {agent_reply[:1500]}\n\n"
                    "What facts, views, or decisions from this exchange are worth remembering?"}],
            )
            import json as _json
            raw = extract_resp.content[0].text.strip()
            # strip markdown code fences if present
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            return _json.loads(raw)
        except Exception:
            return []

    # ── header + controls ─────────────────────────────────────────────────────
    hcol1, hcol2 = st.columns([6, 1])
    with hcol1:
        st.subheader(_t("agent_title"))
        st.caption(_t("agent_caption"))
    with hcol2:
        if st.button(_t("agent_clear"), key="agent_clear_btn"):
            st.session_state["bess_agent_msgs"] = []
            st.session_state["bess_mem_suggestions"] = []
            st.rerun()

    # ── chat history ──────────────────────────────────────────────────────────
    for msg in st.session_state["bess_agent_msgs"]:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if not st.session_state["bess_agent_msgs"]:
        with st.chat_message("assistant"):
            st.markdown(_t("agent_welcome"))

    # ── suggested memories from last exchange ─────────────────────────────────
    if st.session_state["bess_mem_suggestions"]:
        with st.expander(f"💾 {_t('mem_suggested')} ({len(st.session_state['bess_mem_suggestions'])})", expanded=True):
            selected_idxs = []
            for i, item in enumerate(st.session_state["bess_mem_suggestions"]):
                checked = st.checkbox(
                    f"**[{item['category']}]** {item['subject']}",
                    value=True, key=f"mem_chk_{i}",
                )
                if checked:
                    st.caption(f"  {item['content']}")
                    selected_idxs.append(i)
            if st.button(_t("mem_save_selected"), type="primary", key="mem_save_btn"):
                for i in selected_idxs:
                    item = st.session_state["bess_mem_suggestions"][i]
                    _save_memory(item["category"], item["subject"], item["content"], "auto-extract")
                st.success(_t("mem_saved_ok", n=len(selected_idxs)))
                st.session_state["bess_mem_suggestions"] = []
                st.rerun()
            if st.button("Dismiss", key="mem_dismiss_btn"):
                st.session_state["bess_mem_suggestions"] = []
                st.rerun()

    # ── chat input ────────────────────────────────────────────────────────────
    user_input = st.chat_input(_t("agent_placeholder"), key="bess_agent_input")
    if user_input:
        st.session_state["bess_agent_msgs"].append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        try:
            _sys = _build_system()
            _history = [
                {"role": m["role"], "content": m["content"]}
                for m in st.session_state["bess_agent_msgs"]
            ]

            with st.chat_message("assistant"):
                _status = st.status(_t("agent_thinking"), expanded=False)
                _reply_parts = []

                while True:
                    resp = _client.messages.create(
                        model="claude-sonnet-4-6",
                        max_tokens=4096,
                        system=_sys,
                        tools=_TOOLS,
                        messages=_history,
                    )
                    if resp.stop_reason == "tool_use":
                        _tool_results = []
                        for blk in resp.content:
                            if blk.type == "tool_use":
                                with _status:
                                    st.caption(_t("agent_tool_call", tool=blk.name))
                                result = _dispatch_tool(blk.name, blk.input)
                                _tool_results.append({
                                    "type": "tool_result",
                                    "tool_use_id": blk.id,
                                    "content": result,
                                })
                                with _status:
                                    st.caption(_t("agent_tool_result", n=len(result)//50))
                        _history.append({"role": "assistant", "content": resp.content})
                        _history.append({"role": "user", "content": _tool_results})
                    else:
                        for blk in resp.content:
                            if hasattr(blk, "text"):
                                _reply_parts.append(blk.text)
                        break

                _reply = "".join(_reply_parts)
                _status.update(state="complete", expanded=False)
                st.markdown(_reply)

            st.session_state["bess_agent_msgs"].append({"role": "assistant", "content": _reply})

            # ── auto-extract memories from this exchange ──────────────────────
            suggestions = _extract_memories(user_input, _reply)
            if suggestions:
                st.session_state["bess_mem_suggestions"] = suggestions
                st.rerun()

        except Exception as _e:
            st.error(_t("agent_error", err=str(_e)))

    # ── memory management (bottom of tab) ─────────────────────────────────────
    st.divider()
    with st.expander(f"🗄️ {_t('mem_manage')}", expanded=False):
        st.caption(_t("mem_caption"))
        mem_df = load_memories(_ENG_KEY)
        if mem_df.empty:
            st.info(_t("mem_empty"))
        else:
            for row in mem_df.itertuples():
                c1, c2, c3 = st.columns([1, 5, 1])
                c1.markdown(f"**{row.category}**")
                c2.markdown(f"**{row.subject}** — {row.content}")
                if c3.button(_t("mem_delete"), key=f"del_mem_{row.id}"):
                    _delete_memory(row.id)
                    st.rerun()
