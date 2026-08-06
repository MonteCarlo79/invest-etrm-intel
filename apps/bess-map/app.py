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
from irr_helpers import (
    _compute_irr, _compute_npv, build_cashflows,
    _irr_defaults_for_province, _build_extra_rev_map,
)

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
        "irr_components_title": "Revenue & Cost Detail",
        "irr_fr_util":          "FR Utilisation (%)",
        "irr_cf_spot":          "Spot Arbitrage",
        "irr_cf_fr":            "Freq Reg",
        "irr_cf_cap_comp":      "Cap Comp",
        "irr_cf_sysopfee":      "Sys Op Fee",
        "geo_extra_items":      "Add Revenue/Cost Items (overlay on payback)",
        "geo_extra_sysopfee":   "Sys Op Fee (cost)",
        "geo_extra_cap_comp":   "Cap Comp (revenue)",
        "geo_extra_fr":         "Freq Reg (revenue)",
        "geo_fr_util":          "FR Utilisation (%)",
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
        "mgmt_col_missing_dates":      "Price gaps",
        "mgmt_col_missing_fund_dates": "Fundamentals gaps",
        "data_ops_log_title":   "Data Operations Log",
        "mgmt_batch_title":     "Batch Backfill",
        "mgmt_batch_caption":   "Download + ingest + capture for stale or missing provinces via the LingFeng scheduled pipeline.",
        "mgmt_batch_start":     "Start date",
        "mgmt_batch_end":       "End date",
        "mgmt_batch_markets":   "Provinces to backfill",
        "mgmt_batch_btn":       "Run Batch Backfill",
        "mgmt_batch_no_creds":  "LINGFENG_USERNAME / LINGFENG_PASSWORD not set — batch download will fail.",
        "mgmt_advanced_title":  "Manual Data Steps (Advanced)",
        # agent
        "agent_title":          "BESS Market AI Agent",
        "agent_caption":        "Ask about province BESS economics, IRR scenarios, or dispatch performance.",
        "agent_welcome":        "Hi! I can query BESS economics, dispatch data, and run IRR calculations for any province. What would you like to know?",
        "agent_placeholder":    "e.g. Which province has the best 4h BESS IRR at 600 ¥/kWh capex?",
        "agent_thinking":       "Thinking...",
        "agent_tool_call":      "Tool call: {tool}",
        "agent_tool_result":    "Result ({n} rows)",
        "agent_no_key":         "No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION).",
        "agent_clear":          "Clear chat",
        "agent_error":          "Agent error: {err}",
        "llm_selector_label":   "AI Model",
        "llm_no_key":           "{provider} API key is not set.",
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
        # pca
        "tab_pca":                 "Price Profile PCA",
        "pca_title":               "Hourly Price Profile — Principal Component Analysis",
        "pca_caption":             "Eigendecomposition of the daily RT-price covariance matrix (rows = trading days, cols = hours 0–23). Loadings normalised so mean = 1.0 (sum = 24).",
        "pca_province":            "Province",
        "pca_compare":             "Compare multiple provinces (overlay PC1–PC4)",
        "pca_no_data":             "No price data available for this province / period.",
        "pca_not_enough":          "Fewer than 30 trading days — PCA results may be unreliable.",
        "pca_days":                "Trading days in sample",
        "pca_mean_title":          "Mean Daily Price Profile (¥/kWh)",
        "pca_var_title":           "Variance Explained by PC",
        "pca_loading_label":       "PC{n} — {pct}% variance",
        "pca_hour":                "Hour of day",
        "pca_loading_y":           "Normalised loading",
        "pca_cumvar":              "Cumulative",
        # bess demand
        "tab_demand":              "BESS Demand",
        "demand_title":            "BESS Demand Analysis",
        "demand_caption":          "Two complementary sizing methods: (1) spot-market arbitrage from intraday bidding-space swing; (2) frequency-response reserve from provincial rules on installed renewable capacity.",
        "demand_province":         "Province",
        "demand_arb_title":        "① Arbitrage Sizing — Intraday Bidding Space",
        "demand_arb_caption":      "Bidding space = Total Load − Renewable generation − Must-run generation. Max BESS power for arbitrage = (daily max − min) ÷ 2. Note: for major hydro-export provinces (云南, 四川, 贵州), bidding space includes inter-provincial DC export flows and can exceed provincial consumption.",
        "demand_profile_title":    "Mean Intraday Bidding Space Profile (MW)",
        "demand_swing_title":      "Daily BESS Arbitrage Sizing (MW)",
        "demand_swing_y":          "Max BESS power (MW)",
        "demand_fr_title":         "② System FR Capacity Requirement",
        "demand_fr_caption":       "Frequency response capacity required by the grid operator per provincial FM market rules. Formula: % of peak load + % of installed renewables + floor (MW). Sources: provincial FM market rules and GB/T 45905.6-2025.",
        "demand_fr_rule":          "FR rule / formula",
        "demand_fr_pct":           "Effective % of Renewables",
        "demand_fr_peak_load":     "Peak Load (MW)",
        "demand_fr_wind":          "Wind (万kW)",
        "demand_fr_solar":         "Solar (万kW)",
        "demand_fr_renewable":     "Total Renewable (万kW)",
        "demand_fr_req_mw":        "FR Requirement (MW)",
        "demand_fr_bess_cap":      "BESS Installed (万kW)",
        "demand_compare_title":    "③ Combined: Arbitrage vs Frequency Response vs Existing BESS",
        "demand_compare_caption":  "Recommended BESS = max(Arbitrage sizing, FR requirement). Gap = Recommended − Existing BESS installed.",
        # system operation fee
        "tab_sysopfee":            "System Op Fee 系统运行费",
        "sysopfee_title":          "Provincial Grid System Operation Fee (系统运行费)",
        "sysopfee_caption":        "Monthly 系统运行费 (yuan/kWh) per province. Higher fee = greater grid balancing cost, a proxy for flexibility demand.",
        "sysopfee_heatmap_title":  "System Operation Fee Heatmap (¥/kWh)",
        "sysopfee_line_title":     "Monthly Trend by Province (¥/kWh)",
        "sysopfee_no_data":        "No system operation fee data in database. Upload 各省市电网系统运行费用_YYYYMM.xlsx via Hermes to populate.",
        "sysopfee_province":       "Filter provinces",
        # capacity compensation + FR market
        "tab_aux":                 "Cap Comp + FR Market",
        "aux_title":               "Capacity Compensation & Frequency Regulation Market",
        "aux_caption":             "Per-province 容量补偿标准 and 调频市场 policy data (latest confirmed values)",
        "aux_cap_section":         "Capacity Compensation (容量补偿)",
        "aux_fr_section":          "Frequency Regulation Market (调频市场)",
        "aux_conflict_section":    "⚠️ Data Conflicts — please review",
        "aux_province_filter":     "Filter provinces",
        "aux_year_filter":         "Effective year",
        "aux_refresh_btn":         "🔄 Refresh data (internet search)",
        "aux_refresh_started":     "Background search started. Data will update as provinces are scanned.",
        "aux_no_data":             "No data yet. Click 'Refresh data' to start an internet search.",
        "aux_scanning":            "Scanning provinces… {done}/{total} done · Current: {province}",
        "aux_scan_results":        "Found: {cap} cap comp · {fr} FR market",
        "aux_confirm_btn":         "✅ Use this value",
        "aux_cap_rate":            "Cap Comp (¥/kW)",
        "aux_peak_hours":          "Peak Duration (h)",
        "aux_notes":               "Scheme Notes",
        "aux_fr_price":            "FR Price (¥/kW·h)",
        "aux_fr_pool":             "FR Pool (亿¥/yr)",
        "aux_source":              "Source",
        "aux_eff_date":            "Effective date",
        "aux_status":              "Status",
        "aux_bess_section":        "BESS Installed Capacity (储能装机)",
        "aux_bess_mw":             "BESS (MW)",
        "aux_bess_total":          "Total Capacity (MW)",
        "aux_year_month":          "Data Month",
        "aux_bess_source":         "Source",
        "aux_fr_history_note":     "All historical records per province",
        "demand_arb_p50":          "Arbitrage p50 (MW)",
        "demand_arb_p90":          "Arbitrage p90 (MW)",
        "demand_recommended":      "Recommended BESS (MW)",
        "demand_bess_installed":   "Existing BESS (MW)",
        "demand_bess_gap":         "Gap (Demand − Existing, MW)",
        "demand_waterfall_p50_title": "③a BESS Demand Sizing — Arbitrage P50 + FR",
        "demand_waterfall_p90_title": "③b BESS Demand Sizing — Arbitrage P90 + FR",
        "demand_waterfall_cap":    "Each province: [Arb → +FR → −BESS → Net demand]. "
                                   "Flexible Thermal = total thermal capacity minus the average intraday minimum "
                                   "bidding-space floor (must-run thermal). "
                                   "It shows how much thermal can still be backed down — the real competition for BESS.",
        "demand_flex_thermal":     "Flex Thermal (MW)",
        "demand_net_demand":       "Net BESS Demand",
        "demand_no_fund":          "Market fundamentals not available — FR sizing unavailable.",
        "demand_no_hourly":        "No hourly fundamentals data for this province/period.",
        "demand_source_note":      "⚠️ FR capacity requirements from provincial/regional FM market rules and national standard GB/T 45905.6-2025. Formula: FR = pct_load × peak load + pct_renew × installed renewables + floor. 'Confirmed' = official documents; 'Estimate' = derived from regional rules or national baseline. Co-location mandates (配储比例) abolished by No. 136 policy and are NOT used here.",
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
        "irr_components_title": "收入/成本明细",
        "irr_fr_util":          "调频利用率 (%)",
        "irr_cf_spot":          "现货套利",
        "irr_cf_fr":            "调频",
        "irr_cf_cap_comp":      "容量补偿",
        "irr_cf_sysopfee":      "系统运行费",
        "geo_extra_items":      "额外收入/成本项（叠加至回收期）",
        "geo_extra_sysopfee":   "系统运行费（成本）",
        "geo_extra_cap_comp":   "容量补偿（收益）",
        "geo_extra_fr":         "调频（收益）",
        "geo_fr_util":          "调频利用率 (%)",
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
        "mgmt_col_missing_dates":      "现货价格断档",
        "mgmt_col_missing_fund_dates": "基本面数据断档",
        "data_ops_log_title":   "数据操作日志",
        "mgmt_batch_title":     "批量补录",
        "mgmt_batch_caption":   "对过旧或缺失数据的省份执行自动下载、导入和捕获流程。",
        "mgmt_batch_start":     "开始日期",
        "mgmt_batch_end":       "结束日期",
        "mgmt_batch_markets":   "待补录省份",
        "mgmt_batch_btn":       "运行批量补录",
        "mgmt_batch_no_creds":  "未设置 LINGFENG_USERNAME / LINGFENG_PASSWORD，批量下载将失败。",
        "mgmt_advanced_title":  "手动数据操作（高级）",
        "agent_title":          "储能市场量化分析师",
        "agent_caption":        "询问省份储能经济性、IRR情景或调度表现。",
        "agent_welcome":        "您好！我可以查询储能经济数据、调度数据，并为任意省份计算IRR。请问您想了解什么？",
        "agent_placeholder":    "例如：在600元/kWh资本支出下，哪个省份的4h储能IRR最高？",
        "agent_thinking":       "思考中...",
        "agent_tool_call":      "工具调用：{tool}",
        "agent_tool_result":    "结果（{n}行）",
        "agent_no_key":         "未配置LLM（请设置 ANTHROPIC_API_KEY 或 BEDROCK_REGION）。",
        "agent_clear":          "清空对话",
        "agent_error":          "助手错误：{err}",
        "llm_selector_label":   "AI 模型",
        "llm_no_key":           "{provider} API Key未设置。",
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
        # pca
        "tab_pca":                 "价格曲线主成分",
        "pca_title":               "小时电价曲线主成分分析",
        "pca_caption":             "日内实时电价协方差矩阵的特征分解（行=交易日，列=0–23时）。载荷已归一化，均值=1.0（总和=24）。",
        "pca_province":            "省份",
        "pca_compare":             "多省对比（叠加PC1–PC4）",
        "pca_no_data":             "该省份/时段无可用价格数据。",
        "pca_not_enough":          "交易日数少于30天 — 主成分结果可能不可靠。",
        "pca_days":                "样本交易日数",
        "pca_mean_title":          "日均电价曲线（元/千瓦时）",
        "pca_var_title":           "各主成分方差解释比例",
        "pca_loading_label":       "PC{n} — {pct}%方差",
        "pca_hour":                "小时",
        "pca_loading_y":           "归一化载荷",
        "pca_cumvar":              "累计",
        # bess demand
        "tab_demand":              "储能需求",
        "demand_title":            "储能需求分析",
        "demand_caption":          "两种互补的容量估算方法：①日内竞价空间波动的现货套利需求；②各省对装机可再生能源的调频储备规定。",
        "demand_province":         "省份",
        "demand_arb_title":        "① 套利容量 — 日内竞价空间",
        "demand_arb_caption":      "竞价空间 = 总负荷 − 可再生能源出力 − 必开机组出力。套利最大储能功率 = （日内最大值 − 最小值）÷ 2。注：对于云南、四川、贵州等水电外送大省，竞价空间含跨省直流外送电量，可能高于省内负荷。",
        "demand_profile_title":    "日均竞价空间曲线（MW）",
        "demand_swing_title":      "每日储能套利容量（MW）",
        "demand_swing_y":          "最大储能功率（MW）",
        "demand_fr_title":         "② 系统调频容量需求",
        "demand_fr_caption":       "电网调度机构依据各省调频辅助服务市场规则计算的系统调频容量需求。公式：最高负荷×比例 + 新能源装机×比例 + 兜底容量（MW）。来源：各省调频辅助服务市场实施细则及GB/T 45905.6-2025。",
        "demand_fr_rule":          "调频规则/计算公式",
        "demand_fr_pct":           "有效配储比例（%）",
        "demand_fr_peak_load":     "最高负荷（MW）",
        "demand_fr_wind":          "风电装机（万千瓦）",
        "demand_fr_solar":         "光伏装机（万千瓦）",
        "demand_fr_renewable":     "可再生能源合计（万千瓦）",
        "demand_fr_req_mw":        "调频容量需求（MW）",
        "demand_fr_bess_cap":      "储能装机（万千瓦）",
        "demand_compare_title":    "③ 综合对比：套利 vs 调频容量需求 vs 已有储能装机",
        "demand_compare_caption":  "建议储能容量 = max（套利容量, 调频容量需求）。缺口 = 建议容量 − 已有储能装机。",
        "demand_arb_p50":          "套利中位数（MW）",
        "demand_arb_p90":          "套利P90（MW）",
        "demand_recommended":      "建议储能容量（MW）",
        "demand_bess_installed":   "已有储能装机（MW）",
        "demand_bess_gap":         "缺口（需求-装机，MW）",
        "demand_waterfall_p50_title": "③a 储能需求测算 — 套利P50 + 调频",
        "demand_waterfall_p90_title": "③b 储能需求测算 — 套利P90 + 调频",
        "demand_waterfall_cap":    "每省：[套利 → +调频 → −已有储能 → 净需求]。"
                                   "灵活热电容量 = 热电总装机 − 日内平均最低竞价空间（必开机组基准）。"
                                   "反映热电可向下调节的空间，是储能的真实竞争来源。",
        "demand_flex_thermal":     "灵活热电（MW）",
        "demand_net_demand":       "净储能需求",
        "demand_no_fund":          "市场基础数据不可用 — 无法计算调频需求。",
        "demand_no_hourly":        "该省份/时段无小时基础数据。",
        "demand_source_note":      "⚠️ 调频容量需求来源于各省/区域调频辅助服务市场实施细则及GB/T 45905.6-2025国家标准。公式：调频需求 = 最高负荷×比例 + 新能源装机×比例 + 兜底容量。[已确认]=来自官方文件；[估算]=参考区域规则或国家基准值。注：强制配储比例（配储比例）已由136号文废除，本处不适用。",
        # system operation fee
        "tab_sysopfee":            "系统运行费",
        "sysopfee_title":          "各省电网系统运行费",
        "sysopfee_caption":        "各省市月度系统运行费（元/kWh）。费用越高代表电网调节成本越大，是储能灵活性需求的重要参考指标。",
        "sysopfee_heatmap_title":  "系统运行费热力图（元/kWh）",
        "sysopfee_line_title":     "各省月度趋势（元/kWh）",
        "sysopfee_no_data":        "数据库中暂无系统运行费数据。请通过Hermes上传 各省市电网系统运行费用_YYYYMM.xlsx 文件以导入数据。",
        "sysopfee_province":       "筛选省份",
        # capacity compensation + FR market
        "tab_aux":                 "容量补偿+辅助服务",
        "aux_title":               "储能容量补偿 & 调频辅助服务市场",
        "aux_caption":             "各省储能容量补偿标准（元/kW）及调频市场数据（最新已确认值）",
        "aux_cap_section":         "储能容量补偿（容量补偿）",
        "aux_fr_section":          "调频辅助服务市场",
        "aux_conflict_section":    "⚠️ 数据冲突 — 请核实",
        "aux_province_filter":     "筛选省份",
        "aux_year_filter":         "生效年份",
        "aux_refresh_btn":         "🔄 刷新数据（联网搜索）",
        "aux_refresh_started":     "后台搜索已启动，数据将随省份扫描逐步更新。",
        "aux_no_data":             "暂无数据。点击「刷新数据」以启动联网搜索。",
        "aux_scanning":            "正在扫描… {done}/{total} 已完成 · 当前：{province}",
        "aux_scan_results":        "已找到：{cap} 条容量补偿 · {fr} 条调频数据",
        "aux_confirm_btn":         "✅ 使用此值",
        "aux_cap_rate":            "容量补偿（元/kW）",
        "aux_peak_hours":          "峰值时段（小时）",
        "aux_notes":               "补偿机制说明",
        "aux_fr_price":            "调频价格（元/kW·h）",
        "aux_fr_pool":             "调频资金池（亿元/年）",
        "aux_source":              "来源",
        "aux_eff_date":            "生效日期",
        "aux_status":              "状态",
        "aux_bess_section":        "储能装机容量",
        "aux_bess_mw":             "储能装机（MW）",
        "aux_bess_total":          "合计装机（MW）",
        "aux_year_month":          "数据月份",
        "aux_bess_source":         "数据来源",
        "aux_fr_history_note":     "各省全部历史记录",
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
def load_avg_economics(_eng_key, province: str, duration_h: float, model: str = "ols_rt_time_v1"):
    # Theoretical is model-agnostic; realized/capture are scoped to the selected model.
    sql = sql_text("""
        SELECT t.theo_per_mwh_day, r.real_per_mwh_day, r.capture_rate
        FROM (
            SELECT AVG(theoretical_profit_per_mwh_day) AS theo_per_mwh_day
            FROM marketdata.bess_capture_daily
            WHERE province = :p AND ABS(duration_h - :d) < 0.01
        ) t
        CROSS JOIN (
            SELECT AVG(realized_profit_per_mwh_day)              AS real_per_mwh_day,
                   AVG(NULLIF(capture_rate, 'NaN'::double precision)) AS capture_rate
            FROM marketdata.bess_capture_daily
            WHERE province = :p AND ABS(duration_h - :d) < 0.01
              AND model = :model
        ) r
    """)
    row = pd.read_sql(sql, _eng(), params={"p": province, "d": duration_h, "model": model}).iloc[0]
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
            SELECT province, datetime::date AS day, duration_h, power_mw,
                   SUM(GREATEST(dispatch_grid_mw, 0)) AS daily_discharge
            FROM marketdata.spot_dispatch_hourly_theoretical
            WHERE datetime BETWEEN :start AND :end
            GROUP BY province, datetime::date, duration_h, power_mw
        ) t
        GROUP BY province, duration_h
        ORDER BY province, duration_h
    """)
    return pd.read_sql(sql, _eng(), params={"start": start, "end": end})

@st.cache_data(ttl=3600)
def load_coverage(_eng_key):
    sql = sql_text("""
        SELECT h.province,
               h.last_hourly,
               c.last_capture,
               f.last_fund
        FROM (
            SELECT province, MAX(datetime)::date AS last_hourly
            FROM marketdata.spot_prices_hourly
            GROUP BY province
        ) h
        LEFT JOIN (
            SELECT province, MAX(date) AS last_capture
            FROM marketdata.bess_capture_daily
            GROUP BY province
        ) c USING (province)
        LEFT JOIN (
            SELECT province, MAX(datetime)::date AS last_fund
            FROM marketdata.spot_fundamentals_hourly
            GROUP BY province
        ) f USING (province)
        ORDER BY h.province
    """)
    return pd.read_sql(sql, _eng(), parse_dates=["last_hourly", "last_capture", "last_fund"])

def _compress_dates(dates):
    """Compress a list/array of date objects into a compact range string like '2026-01-31~2026-02-04'."""
    if dates is None or len(dates) == 0:
        return ""
    dates = sorted(dates)
    ranges, s, e = [], dates[0], dates[0]
    for d in dates[1:]:
        if (d - e).days == 1:
            e = d
        else:
            ranges.append(str(s) if s == e else f"{s}~{e}")
            s = e = d
    ranges.append(str(s) if s == e else f"{s}~{e}")
    return ", ".join(ranges)

@st.cache_data(ttl=3600)
def load_coverage_gaps(_eng_key):
    """Return {province: compressed_gap_string} for gaps within spot_prices_hourly date range."""
    sql = sql_text("""
        WITH date_series AS (
            SELECT province, MIN(datetime)::date AS fd, MAX(datetime)::date AS ld
            FROM marketdata.spot_prices_hourly GROUP BY province
        ),
        all_dates AS (
            SELECT ds.province,
                   generate_series(ds.fd, ds.ld, interval '1 day')::date AS d
            FROM date_series ds
        ),
        present AS (
            SELECT province, datetime::date AS d
            FROM marketdata.spot_prices_hourly
            GROUP BY province, datetime::date
        )
        SELECT a.province, array_agg(a.d ORDER BY a.d) AS missing_dates
        FROM all_dates a
        LEFT JOIN present p USING (province, d)
        WHERE p.d IS NULL
        GROUP BY a.province
    """)
    try:
        df = pd.read_sql(sql, _eng())
        return {row["province"]: _compress_dates(row["missing_dates"]) for _, row in df.iterrows()}
    except Exception:
        return {}

@st.cache_data(ttl=3600)
def load_fundamentals_gaps(_eng_key):
    """Return {province: compressed_gap_string} for days with zero/missing bidding_space_mw."""
    sql = sql_text("""
        WITH date_series AS (
            SELECT province, MIN(datetime)::date AS fd, MAX(datetime)::date AS ld
            FROM marketdata.spot_fundamentals_hourly GROUP BY province
        ),
        all_dates AS (
            SELECT ds.province,
                   generate_series(ds.fd, ds.ld, interval '1 day')::date AS d
            FROM date_series ds
        ),
        present AS (
            SELECT province, datetime::date AS d
            FROM marketdata.spot_fundamentals_hourly
            WHERE load_mw > 0
            GROUP BY province, datetime::date
        )
        SELECT a.province, array_agg(a.d ORDER BY a.d) AS missing_dates
        FROM all_dates a
        LEFT JOIN present p USING (province, d)
        WHERE p.d IS NULL
        GROUP BY a.province
    """)
    try:
        df = pd.read_sql(sql, _eng())
        return {row["province"]: _compress_dates(row["missing_dates"]) for _, row in df.iterrows()}
    except Exception:
        return {}

@st.cache_data(ttl=120)
def load_scraping_progress(_eng_key) -> pd.DataFrame:
    """
    Monthly fundamentals coverage per province from Dec 2025 to current month.
    Returns long-form: province | month_start | expected_days | days_present | latest_date
    """
    sql = sql_text("""
        WITH months AS (
            SELECT generate_series(
                DATE '2025-12-01',
                date_trunc('month', CURRENT_DATE)::date,
                '1 month'::interval
            )::date AS month_start
        ),
        province_list AS (
            SELECT DISTINCT province FROM marketdata.spot_fundamentals_hourly
            WHERE datetime::date >= '2025-12-01'
        ),
        grid AS (
            SELECT p.province, m.month_start,
                   GREATEST(0,
                       LEAST(
                           (m.month_start + interval '1 month - 1 day')::date,
                           (CURRENT_DATE - 1)::date
                       ) - m.month_start + 1
                   ) AS expected_days
            FROM province_list p CROSS JOIN months m
        ),
        present AS (
            SELECT province,
                   date_trunc('month', datetime)::date AS month_start,
                   COUNT(DISTINCT datetime::date) AS days_present,
                   MAX(datetime::date)             AS latest_date
            FROM marketdata.spot_fundamentals_hourly
            WHERE load_mw IS NOT NULL
              AND datetime::date >= '2025-12-01'
            GROUP BY province, month_start
        )
        SELECT g.province, g.month_start, g.expected_days,
               COALESCE(p.days_present, 0) AS days_present,
               p.latest_date
        FROM grid g
        LEFT JOIN present p USING (province, month_start)
        ORDER BY g.province, g.month_start
    """)
    try:
        return pd.read_sql(sql, _eng())
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_data_ops_log(_eng_key):
    """Recent 48-hour data operations log, newest first."""
    from shared.data_ops.status import get_recent_ops
    try:
        return get_recent_ops(_eng(), hours=48)
    except Exception:
        return pd.DataFrame()


# ── PCA helpers ───────────────────────────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_pca_hourly(_eng_key, province: str, start: str, end: str) -> pd.DataFrame:
    """Return a (n_days × 24) DataFrame of hourly RT prices for one province."""
    sql = sql_text("""
        SELECT datetime::date AS trading_date,
               EXTRACT(hour FROM datetime)::int AS hour,
               AVG(rt_price) AS rt_price
        FROM marketdata.spot_prices_hourly
        WHERE province = :p
          AND datetime::date BETWEEN :s AND :e
        GROUP BY trading_date, hour
        ORDER BY trading_date, hour
    """)
    df = pd.read_sql(sql, _eng(), params={"p": province, "s": start, "e": end})
    if df.empty:
        return pd.DataFrame()
    # Pivot to wide: rows = dates, columns = hours 0-23
    pivot = df.pivot(index="trading_date", columns="hour", values="rt_price")
    # Keep only days that have all 24 hours
    pivot = pivot.dropna(axis=0)
    pivot = pivot[[c for c in range(24) if c in pivot.columns]]
    pivot = pivot.dropna(axis=0)
    return pivot


@st.cache_data(ttl=3600)
def load_pca_fund_hourly(_eng_key, province: str, start: str, end: str) -> pd.DataFrame:
    """Return mean hourly profile (hour 0–23) of fundamentals variables for one province."""
    sql = sql_text("""
        SELECT EXTRACT(hour FROM datetime)::int AS hour,
               AVG(load_mw)            AS load_mw,
               AVG(renewable_total_mw) AS renewable_total_mw,
               AVG(bidding_space_mw)   AS bidding_space_mw,
               AVG(wind_mw)            AS wind_mw,
               AVG(solar_mw)           AS solar_mw
        FROM marketdata.spot_fundamentals_hourly
        WHERE province = :p
          AND datetime::date BETWEEN :s AND :e
          AND load_mw IS NOT NULL
        GROUP BY hour
        ORDER BY hour
    """)
    try:
        return pd.read_sql(sql, _eng(), params={"p": province, "s": start, "e": end})
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def load_pca_fund_daily(_eng_key, province: str, start: str, end: str) -> pd.DataFrame:
    """Return daily aggregates of fundamentals variables for PC score correlation."""
    sql = sql_text("""
        SELECT datetime::date AS day,
               AVG(load_mw)                     AS load_mw,
               AVG(renewable_total_mw)           AS renewable_total_mw,
               AVG(COALESCE(bidding_space_mw,0)) AS bidding_space_mw,
               AVG(COALESCE(wind_mw, 0))         AS wind_mw,
               AVG(COALESCE(solar_mw, 0))        AS solar_mw,
               MAX(COALESCE(solar_mw, 0))        AS solar_peak_mw
        FROM marketdata.spot_fundamentals_hourly
        WHERE province = :p
          AND datetime::date BETWEEN :s AND :e
          AND load_mw IS NOT NULL
        GROUP BY day
        ORDER BY day
    """)
    try:
        return pd.read_sql(sql, _eng(), params={"p": province, "s": start, "e": end})
    except Exception:
        return pd.DataFrame()


def compute_pca(price_matrix: pd.DataFrame, n_pcs: int = 4) -> dict:
    """
    PCA on a (days × 24) price matrix using covariance matrix eigendecomposition.
    Loadings are normalised to sum=24 (mean=1.0) — same convention as reference model.
    Returns dict with keys: loadings, eigenvalues, variance_explained, mean_profile,
                            n_days, scores (n_days × n_pcs, raw projection), dates.
    """
    X = price_matrix.values.astype(float)
    mean_profile = X.mean(axis=0)          # shape (24,)
    X_centered = X - mean_profile          # mean-centre each day

    cov_mat = np.cov(X_centered.T)         # 24×24 covariance matrix
    # eigh for symmetric real matrix — guaranteed real eigenvalues, stable
    eig_vals, eig_vecs = np.linalg.eigh(cov_mat)

    # Sort descending by eigenvalue magnitude
    order = np.argsort(np.abs(eig_vals))[::-1]
    eig_vals = eig_vals[order]
    eig_vecs = eig_vecs[:, order]          # columns are eigenvectors

    total_var = eig_vals.sum()
    variance_explained = (eig_vals / total_var * 100) if total_var > 0 else eig_vals * 0

    n_keep = min(n_pcs, eig_vecs.shape[1])
    loadings = []
    for i in range(n_keep):
        vec = eig_vecs[:, i].copy()
        s = vec.sum()
        if abs(s) > 1e-10:
            vec = vec / s * 24.0           # normalise: mean=1, sum=24
        else:
            vec = vec / (np.abs(vec).sum() + 1e-10) * 24.0
        loadings.append(vec)

    # Daily PC scores: project mean-centred price vectors onto raw eigenvectors
    scores = X_centered @ eig_vecs[:, :n_keep]  # shape (n_days, n_keep)

    return {
        "loadings": loadings,              # list of n_pcs arrays, each shape (24,)
        "eigenvalues": eig_vals,
        "variance_explained": variance_explained,
        "mean_profile": mean_profile,
        "n_days": len(X),
        "scores": scores,                  # ndarray (n_days, n_pcs)
        "dates": list(price_matrix.index), # trading dates aligned with scores
    }


# ── BESS Demand: Mandatory co-location ratios (配储比例) ──────────────────────
# Source: provincial energy bureau development plans, grid-connection rules, and
#         operator ancillary-service documents (confirmed via policy research).
# Regime: Mandatory BESS co-location for NEW wind/solar projects (配储比例).
#         Applied here to total installed renewable capacity as a proxy for the
#         aggregate provincial BESS demand implied by the build-out pipeline.
# Basis: fraction of wind + solar installed capacity (万kW × 10 → MW).
# FR capacity requirement rules per province.
# Format: (description, pct_of_peak_load, pct_of_installed_renew_mw, floor_mw)
# FR_mw = max(floor_mw, peak_load_mw * pct_load + renew_installed_mw * pct_renew)
# Source labels: [Confirmed] = from official FM market rule documents;
#                [Estimate]  = derived from regional rules or national standard.
# NOTE: verify against latest regulations before use in investment decisions.
_FR_RULES: dict[str, tuple[str, float, float, float]] = {
    # ── Confirmed from official FM market rule documents ──────────────────────
    "陕西":   ("[Confirmed] 2.5%×peak load + 10%×max wind forecast "
               "(Shaanxi FM Market Rule V2, Art.17, 2025)",                  0.025, 0.025, 0.0),
    "江西":   ("[Confirmed] 2–5%×peak load (mid-range 3.5%) "
               "(Jiangxi FM Market Rule, 华中区域)",                          0.035, 0.0,   0.0),
    "湖北":   ("[Confirmed] 2–5%×peak load (mid-range 3%) "
               "(Central China regional FM rule, 华中区域)",                  0.030, 0.0,   0.0),
    "重庆":   ("[Confirmed] 2–5%×peak load (mid-range 3%) "
               "(Central China regional FM rule, 华中区域)",                  0.030, 0.0,   0.0),
    "云南":   ("[Confirmed] 0.6%×load + 0.6%×renewables + 450 MW floor "
               "(Yunnan FM Market Rule, Appendix 3)",                         0.006, 0.006, 450.0),
    "广东":   ("[Confirmed] ~1.5%×peak load + ~1.5%×renewables "
               "(South Grid FM Rule 2025, R1=0.8–1.5%, R2=0.8–3%)",         0.015, 0.015, 0.0),
    "广西":   ("[Confirmed] ~1.5%×peak load + ~1.5%×renewables "
               "(South Grid FM Rule 2025)",                                   0.015, 0.015, 0.0),
    "贵州":   ("[Confirmed] ~1.5%×peak load + ~1.5%×renewables "
               "(South Grid FM Rule 2025)",                                   0.015, 0.015, 0.0),
    "海南":   ("[Confirmed] ~1.5%×peak load + ~1.5%×renewables "
               "(South Grid FM Rule 2025)",                                   0.015, 0.015, 0.0),
    # ── Estimates based on regional grid rules / national standard ────────────
    "甘肃":   ("[Estimate] ~2.5%×peak load + ~5%×renewables "
               "(high RE penetration; Northwest Grid)",                       0.025, 0.050, 0.0),
    "新疆":   ("[Estimate] ~2.5%×peak load + ~3%×renewables "
               "(Northwest Grid; high wind penetration)",                     0.025, 0.030, 0.0),
    "宁夏":   ("[Estimate] ~2.5%×peak load + ~3%×renewables "
               "(Northwest Grid)",                                            0.025, 0.030, 0.0),
    "青海":   ("[Estimate] ~2.5%×peak load + ~3%×renewables "
               "(high RE penetration; Northwest Grid)",                       0.025, 0.030, 0.0),
    "蒙西":   ("[Estimate] ~2.5%×peak load + ~3%×renewables "
               "(high wind penetration; Inner Mongolia West Grid)",           0.025, 0.030, 0.0),
    "蒙东":   ("[Estimate] ~2.5%×peak load + ~2.5%×renewables "
               "(Northeast Grid)",                                            0.025, 0.025, 0.0),
    "山东":   ("[Estimate] ~2%×peak load + ~2%×renewables "
               "(North China Grid; GB/T 45905.6-2025 baseline)",             0.020, 0.020, 0.0),
    "山西":   ("[Estimate] ~2.5%×peak load + ~2%×renewables "
               "(North China Grid)",                                          0.025, 0.020, 0.0),
    "冀北":   ("[Estimate] ~2%×peak load + ~2%×renewables "
               "(North China Grid)",                                          0.020, 0.020, 0.0),
    "冀南":   ("[Estimate] ~2%×peak load + ~2%×renewables "
               "(North China Grid)",                                          0.020, 0.020, 0.0),
    "河南":   ("[Estimate] ~2.5%×peak load + ~1.5%×renewables "
               "(Central China Grid)",                                        0.025, 0.015, 0.0),
    "湖南":   ("[Estimate] ~3%×peak load + ~1%×renewables "
               "(Central China Grid)",                                        0.030, 0.010, 0.0),
    "江苏":   ("[Estimate] ~2%×peak load + ~1%×renewables "
               "(East China Grid)",                                           0.020, 0.010, 0.0),
    "浙江":   ("[Estimate] ~2%×peak load + ~1%×renewables "
               "(Zhejiang FM rule draft, 2024)",                              0.020, 0.010, 0.0),
    "安徽":   ("[Estimate] ~2%×peak load + ~1%×renewables "
               "(East China Grid)",                                           0.020, 0.010, 0.0),
    "福建":   ("[Estimate] ~2%×peak load + ~1%×renewables "
               "(East China Grid)",                                           0.020, 0.010, 0.0),
    "辽宁":   ("[Estimate] ~2.5%×peak load + ~2%×renewables "
               "(Northeast Grid)",                                            0.025, 0.020, 0.0),
    "吉林":   ("[Estimate] ~2.5%×peak load + ~2%×renewables "
               "(Northeast Grid)",                                            0.025, 0.020, 0.0),
    "黑龙江": ("[Estimate] ~2.5%×peak load + ~2%×renewables "
               "(Northeast Grid)",                                            0.025, 0.020, 0.0),
    "四川":   ("[Estimate] ~1%×peak load + ~0.5%×renewables "
               "(hydro-dominant grid; hydro provides primary FR)",            0.010, 0.005, 0.0),
}
_FR_DEFAULT: tuple[str, float, float, float] = (
    "[Estimate] ~2%×peak load + ~1.5%×renewables (national standard GB/T 45905.6-2025 baseline)",
    0.020, 0.015, 0.0,
)


@st.cache_data(ttl=3600)
def load_demand_hourly(_eng_key, provinces: tuple, start: str, end: str) -> pd.DataFrame:
    """
    Daily bidding-space swing per province.
    Returns: province | date | max_bs | min_bs | swing | bess_arb_mw
    """
    sql = sql_text("""
        SELECT province,
               datetime::date                               AS date,
               MAX(bidding_space_mw)                        AS max_bs,
               MIN(bidding_space_mw)                        AS min_bs,
               MAX(bidding_space_mw) - MIN(bidding_space_mw) AS swing
        FROM marketdata.spot_fundamentals_hourly
        WHERE province = ANY(:provs)
          AND datetime::date BETWEEN :s AND :e
          AND bidding_space_mw > 0
        GROUP BY province, datetime::date
        ORDER BY province, date
    """)
    df = pd.read_sql(sql, _eng(), params={"provs": list(provinces), "s": start, "e": end})
    if not df.empty:
        df["bess_arb_mw"] = df["swing"] / 2.0
    return df


@st.cache_data(ttl=3600)
def load_demand_intraday_profile(_eng_key, province: str, start: str, end: str) -> pd.DataFrame:
    """
    Mean ± std of bidding_space_mw and its components by hour of day.
    Returns: hour | load_mean | renewable_mean | bs_mean | bs_std
    """
    sql = sql_text("""
        SELECT EXTRACT(hour FROM datetime)::int AS hour,
               AVG(load_mw)              AS load_mean,
               AVG(renewable_total_mw)   AS renewable_mean,
               AVG(bidding_space_mw)     AS bs_mean,
               STDDEV(bidding_space_mw)  AS bs_std
        FROM marketdata.spot_fundamentals_hourly
        WHERE province = :p
          AND datetime::date BETWEEN :s AND :e
          AND bidding_space_mw > 0
          AND load_mw > 0
        GROUP BY hour
        ORDER BY hour
    """)
    return pd.read_sql(sql, _eng(), params={"p": province, "s": start, "e": end})


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
                           title: str | None = None,
                           extra_rev_map: "dict[str, float] | None" = None) -> plt.Figure:
    """Choropleth coloured by simple capex payback period (years).

    extra_rev_map: optional {province_name: annual ¥/MWh adjustment}.
    Positive = additional revenue (shorter payback), negative = cost (longer payback).
    """
    sub = rank_df[abs(rank_df["duration_h"] - duration_h) < 0.01].copy()
    sub["adcode"] = sub["province"].map(_ZH_PROV_ADCODE)
    sub = sub.dropna(subset=["adcode", col])

    # Build province → adcode lookup for applying extra_rev_map
    prov_to_adcode: dict[str, int] = {
        row["province"]: int(row["adcode"])
        for _, row in sub.iterrows()
        if pd.notna(row.get("adcode"))
    }
    extra_by_adcode: dict[int, float] = {}
    if extra_rev_map:
        for prov, adj in extra_rev_map.items():
            acode = prov_to_adcode.get(prov)
            if acode is not None:
                extra_by_adcode[acode] = adj

    rev_map: dict[int, float | None] = {}
    label_map: dict[int, str] = {}
    for _, row in sub.iterrows():
        acode = int(row["adcode"])
        rev = float(row[col]) if pd.notna(row[col]) else None
        adj = extra_by_adcode.get(acode, 0.0)
        adj_rev = (rev + adj) if rev is not None else None

        # Use adjusted revenue for colour
        rev_map[acode] = adj_rev

        if rev is not None and rev > 0:
            orig_pb = capex_per_kwh * 1000.0 / rev
            if adj_rev is not None and adj_rev > 0 and adj != 0.0:
                adj_pb = capex_per_kwh * 1000.0 / adj_rev
                sign = "+" if adj >= 0 else ""
                label_map[acode] = (
                    f"{rev:,.0f} ({sign}{adj:,.0f})\n"
                    f"({orig_pb:.1f}yr→{adj_pb:.1f}yr)"
                )
            else:
                label_map[acode] = f"{rev:,.0f}\n({orig_pb:.1f}yr)"
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


# ── IRR computation — functions imported from irr_helpers ─────────────────────

# ── sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title(_t("app_title"))
    if "lang_radio" not in st.session_state:
        st.session_state["lang_radio"] = "中文"
    lang = st.radio(_t("lang_label"), ["中文", "English"], key="lang_radio", horizontal=True)
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
tab_ranking, tab_geo, tab_pca, tab_demand, tab_sysopfee, tab_aux, tab_dispatch, tab_irr, tab_mgmt, tab_agent = st.tabs([
    _t("tab_ranking"), _t("tab_geo"), _t("tab_pca"), _t("tab_demand"),
    _t("tab_sysopfee"), _t("tab_aux"), _t("tab_dispatch"), _t("tab_irr"), _t("tab_mgmt"), _t("tab_agent"),
])

_ENG_KEY = "bess_map"   # hashable cache-bust token (stable)

# ── Session state init for aux tab ────────────────────────────────────────────
if "aux_provinces" not in st.session_state:
    st.session_state["aux_provinces"] = []
if "aux_year" not in st.session_state:
    st.session_state["aux_year"] = dt.datetime.now().year


@st.cache_data(ttl=1800)
def load_cap_comp(_eng_key):
    """Load province_cap_comp rows (confirmed + conflict) from DB."""
    import pandas as _pd
    dsn = os.environ.get("PGURL", "")
    if not dsn:
        return _pd.DataFrame()
    try:
        import psycopg2 as _pg
        conn = _pg.connect(dsn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, province, effective_date, cap_comp_yuan_kw,
                       peak_duration_hours, source, status, notes
                FROM marketdata.province_cap_comp
                WHERE status IN ('confirmed', 'conflict')
                ORDER BY province, effective_date DESC, ingested_at DESC
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        conn.close()
        return _pd.DataFrame(rows, columns=cols) if rows else _pd.DataFrame(columns=cols)
    except Exception as _e:
        return _pd.DataFrame()


@st.cache_data(ttl=1800)
def load_fr_market(_eng_key):
    """Load province_fr_market rows (confirmed + conflict) from DB."""
    import pandas as _pd
    dsn = os.environ.get("PGURL", "")
    if not dsn:
        return _pd.DataFrame()
    try:
        import psycopg2 as _pg
        conn = _pg.connect(dsn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, province, effective_date, fr_price_yuan_kw_h,
                       fr_pool_billion_yuan, source, status
                FROM marketdata.province_fr_market
                WHERE status IN ('confirmed', 'conflict')
                ORDER BY province, effective_date DESC
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        conn.close()
        return _pd.DataFrame(rows, columns=cols) if rows else _pd.DataFrame(columns=cols)
    except Exception as _e:
        return _pd.DataFrame()


@st.cache_data(ttl=1800)
def load_sysopfee(_eng_key) -> "pd.DataFrame":
    """Load all province_sysopfee_monthly data."""
    try:
        sql = sql_text("""
            SELECT province, year_month, fee_yuan_kwh
            FROM province_sysopfee_monthly
            ORDER BY year_month, province
        """)
        df = pd.read_sql(sql, _eng())
        df["year_month"] = pd.to_datetime(df["year_month"])
        return df
    except Exception:
        return pd.DataFrame(columns=["province", "year_month", "fee_yuan_kwh"])


@st.cache_data(ttl=1800)
def load_installed_capacity(_eng_key):
    """Load province_installed_monthly — all rows with bess_mw, ordered by province + month DESC."""
    import pandas as _pd
    dsn = os.environ.get("PGURL", "")
    if not dsn:
        return _pd.DataFrame()
    try:
        import psycopg2 as _pg
        conn = _pg.connect(dsn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT province, year_month, wind_mw, solar_mw, thermal_mw,
                       hydro_mw, nuclear_mw, bess_mw, total_mw, source_file
                FROM marketdata.province_installed_monthly
                WHERE bess_mw IS NOT NULL
                ORDER BY province, year_month DESC
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        conn.close()
        return _pd.DataFrame(rows, columns=cols) if rows else _pd.DataFrame(columns=cols)
    except Exception:
        return _pd.DataFrame()


@st.cache_data(ttl=1800)
def load_shandong_ancillary(_eng_key):
    """Load 山东 monthly ancillary cost data from staging.exchange_excel_metrics."""
    import pandas as _pd
    dsn = os.environ.get("PGURL", "")
    if not dsn:
        return _pd.DataFrame()
    try:
        import psycopg2 as _pg
        conn = _pg.connect(dsn)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT report_month,
                       fr_pool_million_yuan,
                       peak_shaving_million_yuan,
                       renewable_deviation_million_yuan,
                       total_ancillary_million_yuan
                FROM staging.exchange_excel_metrics
                WHERE province = '山东'
                  AND total_ancillary_million_yuan IS NOT NULL
                ORDER BY report_month
            """)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
        conn.close()
        return _pd.DataFrame(rows, columns=cols) if rows else _pd.DataFrame(columns=cols)
    except Exception:
        return _pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def load_monthly_gaps(_eng_key):
    """
    Return three DataFrames (capcomp, fr_market, installed) with
    province × YYYY-MM coverage for the last 12 months.
    Each df has columns: province, year_month (str), has_data (bool).
    """
    import pandas as _pd
    import datetime as _dt
    dsn = os.environ.get("PGURL", "")
    if not dsn:
        empty = _pd.DataFrame(columns=["province", "year_month", "has_data"])
        return empty, empty, empty

    today = _dt.date.today()
    months = []
    for i in range(12):
        d = today.replace(day=1)
        # subtract i months manually
        m = today.month - i
        y = today.year
        while m <= 0:
            m += 12
            y -= 1
        months.append(f"{y:04d}-{m:02d}")

    queries = {
        "capcomp": """
            SELECT province, TO_CHAR(effective_date, 'YYYY-MM') AS year_month
            FROM marketdata.province_cap_comp
            WHERE status IN ('confirmed', 'conflict')
        """,
        "fr_market": """
            SELECT province, TO_CHAR(effective_date, 'YYYY-MM') AS year_month
            FROM marketdata.province_fr_market
            WHERE status IN ('confirmed', 'conflict')
        """,
        "installed": """
            SELECT province,
                   TO_CHAR(TO_DATE(year_month::text, 'YYYYMM'), 'YYYY-MM') AS year_month
            FROM marketdata.province_installed_monthly
            WHERE bess_mw IS NOT NULL
        """,
    }

    results = {}
    for key, sql in queries.items():
        try:
            import psycopg2 as _pg
            conn = _pg.connect(dsn)
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
            conn.close()
            df = _pd.DataFrame(rows, columns=cols) if rows else _pd.DataFrame(columns=["province", "year_month"])
            df["has_data"] = True
            provinces = sorted(df["province"].unique()) if not df.empty else []
            if provinces:
                import itertools
                grid = list(itertools.product(provinces, months))
                full = _pd.DataFrame(grid, columns=["province", "year_month"])
                full = full.merge(df[["province", "year_month", "has_data"]].drop_duplicates(),
                                  on=["province", "year_month"], how="left")
                full["has_data"] = full["has_data"].fillna(False)
            else:
                full = _pd.DataFrame(columns=["province", "year_month", "has_data"])
            results[key] = full
        except Exception:
            results[key] = _pd.DataFrame(columns=["province", "year_month", "has_data"])
    return results["capcomp"], results["fr_market"], results["installed"]


# Pre-load market data used across multiple tabs (sysopfee, aux, irr, geo)
_sof_df = load_sysopfee(_ENG_KEY)
_cc_df  = load_cap_comp(_ENG_KEY)
_fr_df  = load_fr_market(_ENG_KEY)

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

    # ── Extra overlay items ───────────────────────────────────────────────────
    _geo_extra_options = {
        "sysopfee": _t("geo_extra_sysopfee"),
        "cap_comp":  _t("geo_extra_cap_comp"),
        "fr":        _t("geo_extra_fr"),
    }
    _geo_sel_labels = st.multiselect(
        _t("geo_extra_items"),
        options=list(_geo_extra_options.values()),
        default=[],
        key="geo_extra_sel",
    )
    # Map labels back to keys
    _label_to_key = {v: k for k, v in _geo_extra_options.items()}
    _geo_sel_keys = [_label_to_key[lbl] for lbl in _geo_sel_labels]

    _geo_fr_util = 0.30
    if "fr" in _geo_sel_keys:
        _geo_fr_util = st.slider(_t("geo_fr_util"), 5, 80, 30, step=5, key="geo_fr_util") / 100.0

    # Build extra_rev_map from already-loaded DataFrames
    _geo_extra_map = _build_extra_rev_map(
        _sof_df, _cc_df, _fr_df,
        duration_h=4.0,
        selected_items=_geo_sel_keys,
        fr_util_pct=_geo_fr_util,
    ) if _geo_sel_keys else {}

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
                extra_rev_map=_geo_extra_map if _geo_extra_map else None,
            )
            st.pyplot(fig_geo2, use_container_width=True)
            plt.close(fig_geo2)
        with col_4h:
            st.subheader(_t("geo_4h_title"))
            fig_geo4 = chart_bess_revenue_map(
                geo_rank_df, 4.0, rank_annual_col, _geojson_bess,
                capex_per_kwh=geo_capex,
                title=_t("geo_4h_title"),
                extra_rev_map=_geo_extra_map if _geo_extra_map else None,
            )
            st.pyplot(fig_geo4, use_container_width=True)
            plt.close(fig_geo4)

        st.caption(
            f"Revenue basis: **{_t('forecast_theoretical') if rank_annual_col == 'annual_theo' else _t('forecast_realized')}** · "
            f"{sel_start} → {sel_end} · Payback = capex ({geo_capex} ¥/kWh × 1000) ÷ annual rev"
        )
    elif geo_rank_df.empty:
        st.warning("No ranking data available for this period.")

# ── Tab 3: Price Profile PCA ──────────────────────────────────────────────────
_PC_COLORS = ["#4C78A8", "#F58518", "#54A24B", "#E45756",
              "#B279A2", "#9D755D", "#BAB0AC", "#72B7B2"]
_PC_FILL   = ["rgba(76,120,168,0.12)", "rgba(245,133,24,0.12)",
              "rgba(84,162,75,0.12)",  "rgba(228,87,86,0.12)"]

with tab_pca:
    st.subheader(_t("pca_title"))
    st.caption(_t("pca_caption"))

    all_provs_pca = load_province_list(_ENG_KEY)

    if "pca_province_single" not in st.session_state and all_provs_pca:
        st.session_state["pca_province_single"] = all_provs_pca[0]
    if "pca_provinces_multi" not in st.session_state and all_provs_pca:
        st.session_state["pca_provinces_multi"] = all_provs_pca[:3] if len(all_provs_pca) >= 3 else all_provs_pca

    pca_col_left, pca_col_right = st.columns([1, 3])
    with pca_col_left:
        compare_mode = st.checkbox(_t("pca_compare"), value=False, key="pca_compare")
        if compare_mode:
            pca_provinces = st.multiselect(
                _t("pca_province"),
                options=all_provs_pca,
                key="pca_provinces_multi",
            )
        else:
            pca_prov_single = st.selectbox(
                _t("pca_province"),
                options=all_provs_pca,
                key="pca_province_single",
            )
            pca_provinces = [pca_prov_single] if pca_prov_single else []
        n_pcs = st.slider("PCs to show", min_value=2, max_value=6, value=4, key="pca_n_pcs")
        use_l2 = st.checkbox(
            "Normalise for comparison (L2 unit vector)",
            value=compare_mode,
            key="pca_l2_norm",
            help="L2-normalise each loading to unit length so all provinces share the same scale. "
                 "Useful for shape comparison. Default (unchecked) uses sum=24 normalisation from the reference model.",
        )

    # helper: apply the chosen normalisation
    def _norm_loading(vec: np.ndarray) -> np.ndarray:
        if use_l2:
            n = np.linalg.norm(vec)
            return vec / n if n > 1e-10 else vec
        return vec  # already sum=24 from compute_pca

    _ref_y    = 0.0 if use_l2 else 1.0
    _y_label  = ("L2-normalised loading" if use_l2 else _t("pca_loading_y"))

    if not pca_provinces:
        st.info(_t("pca_no_data"))
    else:
        # ── Load & compute for each province ──────────────────────────────
        pca_results: dict[str, dict] = {}
        for prov in pca_provinces:
            mat = load_pca_hourly(_ENG_KEY, prov, sel_start, sel_end)
            if mat.empty or len(mat) < 10:
                continue
            pca_results[prov] = compute_pca(mat, n_pcs=n_pcs)

        if not pca_results:
            st.warning(_t("pca_no_data"))
        elif not compare_mode:
            # ── Single-province detail view ────────────────────────────────
            prov  = pca_provinces[0]
            res   = pca_results.get(prov)
            if res is None:
                st.warning(_t("pca_no_data"))
            else:
                if res["n_days"] < 30:
                    st.warning(_t("pca_not_enough"))
                st.caption(f"{_t('pca_days')}: **{res['n_days']}**")

                hours = list(range(24))
                var_exp = res["variance_explained"]

                # Row A: mean profile + variance bar
                rA1, rA2 = st.columns(2)
                with rA1:
                    fig_mean = go.Figure()
                    fig_mean.add_trace(go.Scatter(
                        x=hours, y=res["mean_profile"],
                        mode="lines+markers",
                        line=dict(color=_PC_COLORS[0], width=2),
                        fill="tozeroy", fillcolor=_PC_FILL[0],
                        name="mean",
                    ))
                    fig_mean.update_layout(
                        title=f"{prov} — {_t('pca_mean_title')}",
                        xaxis_title=_t("pca_hour"), yaxis_title="¥/kWh",
                        height=280, margin=dict(t=40, b=30, l=40, r=10),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_mean, use_container_width=True, key=f"pca_mean_{prov}")

                with rA2:
                    n_show = min(n_pcs, len(var_exp))
                    cumvar = [float(var_exp[:i+1].sum()) for i in range(n_show)]
                    fig_var = go.Figure()
                    fig_var.add_trace(go.Bar(
                        x=[f"PC{i+1}" for i in range(n_show)],
                        y=[float(var_exp[i]) for i in range(n_show)],
                        marker_color=_PC_COLORS[:n_show],
                        name="% var",
                    ))
                    fig_var.add_trace(go.Scatter(
                        x=[f"PC{i+1}" for i in range(n_show)],
                        y=cumvar,
                        mode="lines+markers",
                        line=dict(color="#555", width=1.5, dash="dot"),
                        name=_t("pca_cumvar"),
                        yaxis="y2",
                    ))
                    fig_var.update_layout(
                        title=_t("pca_var_title"),
                        yaxis=dict(title="% variance"),
                        yaxis2=dict(title="cumulative %", overlaying="y", side="right",
                                    range=[0, 105]),
                        height=280, margin=dict(t=40, b=30, l=40, r=50),
                        legend=dict(orientation="h", y=-0.25),
                    )
                    st.plotly_chart(fig_var, use_container_width=True, key=f"pca_var_{prov}")

                # Row B+: PC loading charts in 2-column grid
                ncols = 2
                loadings = res["loadings"]
                for row_start in range(0, len(loadings), ncols):
                    cols = st.columns(ncols)
                    for col_idx, pc_idx in enumerate(range(row_start, min(row_start + ncols, len(loadings)))):
                        with cols[col_idx]:
                            loading = _norm_loading(loadings[pc_idx])
                            pct = f"{var_exp[pc_idx]:.1f}"
                            label = _t("pca_loading_label", n=pc_idx + 1, pct=pct)
                            fig_pc = go.Figure()
                            fig_pc.add_hline(y=_ref_y, line_dash="dot",
                                             line_color="rgba(0,0,0,0.25)", line_width=1)
                            fig_pc.add_trace(go.Scatter(
                                x=hours + hours[::-1],
                                y=list(loading) + [_ref_y] * 24,
                                fill="toself",
                                fillcolor=_PC_FILL[pc_idx % len(_PC_FILL)],
                                line=dict(width=0),
                                showlegend=False,
                                hoverinfo="skip",
                            ))
                            fig_pc.add_trace(go.Scatter(
                                x=hours, y=loading.tolist(),
                                mode="lines+markers",
                                line=dict(color=_PC_COLORS[pc_idx % len(_PC_COLORS)], width=2),
                                marker=dict(size=5),
                                name=label,
                            ))
                            fig_pc.update_layout(
                                title=label,
                                xaxis=dict(title=_t("pca_hour"), dtick=2),
                                yaxis=dict(title=_y_label),
                                height=280, margin=dict(t=40, b=30, l=40, r=10),
                                showlegend=False,
                            )
                            st.plotly_chart(fig_pc, use_container_width=True,
                                            key=f"pca_pc{pc_idx+1}_{prov}")

                # ── PC Interpretation: intraday shape & daily score correlations ──
                st.divider()
                st.markdown("#### PC Interpretation — Intraday Correlations with Market Variables")
                st.caption(
                    "**Shape correlation** (left): Pearson r between each PC's hourly loading pattern "
                    "and the mean hourly profile of each variable.  "
                    "**Score correlation** (right): Pearson r between each day's PC score "
                    "and the daily average of each variable.  "
                    "Strong |r| ≥ 0.5 shown in bold."
                )

                _fund_hour = load_pca_fund_hourly(_ENG_KEY, prov, sel_start, sel_end)
                _fund_daily = load_pca_fund_daily(_ENG_KEY, prov, sel_start, sel_end)

                _var_display = {
                    "Bidding Space": "bidding_space_mw",
                    "Solar":         "solar_mw",
                    "Wind":          "wind_mw",
                    "Load":          "load_mw",
                    "Renewable":     "renewable_total_mw",
                }
                _n_interp = min(4, len(loadings))

                _corr_interp_ok = (
                    not _fund_hour.empty
                    and len(_fund_hour) == 24
                    and not _fund_daily.empty
                )

                if not _corr_interp_ok:
                    st.info("Fundamentals data not available for this province/period — correlation analysis skipped.")
                else:
                    _fund_hour_idx = _fund_hour.set_index("hour")
                    _fund_daily["day"] = pd.to_datetime(_fund_daily["day"]).dt.date

                    # ── Shape correlation (loading shape vs mean hourly variable) ──
                    _shape_rows = []
                    for _pci in range(_n_interp):
                        _loading_raw = res["loadings"][_pci]  # always sum=24 for shape corr
                        _row = {}
                        for _vlabel, _vcol in _var_display.items():
                            if _vcol in _fund_hour_idx.columns:
                                _vvec = _fund_hour_idx[_vcol].values.astype(float)
                                if not np.isnan(_vvec).all() and _vvec.std() > 1e-10:
                                    _row[_vlabel] = float(np.corrcoef(_loading_raw, _vvec)[0, 1])
                        _shape_rows.append(_row)
                    _shape_df = pd.DataFrame(
                        _shape_rows,
                        index=[f"PC{i+1} ({res['variance_explained'][i]:.1f}%)" for i in range(_n_interp)],
                    )

                    # ── Score correlation (daily PC score vs daily variable avg) ──
                    _scores_arr = res.get("scores")  # (n_days, n_pcs)
                    _dates_arr  = res.get("dates", [])
                    _score_rows = []
                    if _scores_arr is not None and len(_dates_arr) > 0:
                        _score_df_idx = pd.DataFrame(
                            {f"PC{i+1}": _scores_arr[:, i] for i in range(_n_interp)},
                            index=pd.to_datetime(_dates_arr).date,
                        )
                        _merged = _score_df_idx.join(
                            _fund_daily.set_index("day")[list(_var_display.values())],
                            how="inner",
                        )
                        for _pci in range(_n_interp):
                            _sc_col = f"PC{_pci+1}"
                            _row = {}
                            for _vlabel, _vcol in _var_display.items():
                                if _vcol in _merged.columns:
                                    _s = _merged[_sc_col].values.astype(float)
                                    _v = _merged[_vcol].values.astype(float)
                                    _valid = ~(np.isnan(_s) | np.isnan(_v))
                                    if _valid.sum() >= 10 and _v[_valid].std() > 1e-10:
                                        _row[_vlabel] = float(np.corrcoef(_s[_valid], _v[_valid])[0, 1])
                            _score_rows.append(_row)
                    _score_df = pd.DataFrame(
                        _score_rows,
                        index=[f"PC{i+1} ({res['variance_explained'][i]:.1f}%)" for i in range(len(_score_rows))],
                    ) if _score_rows else pd.DataFrame()

                    # ── Heatmap helper ──────────────────────────────────────────
                    def _corr_heatmap(df: pd.DataFrame, title: str, key_sfx: str):
                        if df.empty:
                            st.info(f"{title}: no data.")
                            return
                        _z    = df.values.tolist()
                        _text = [[f"{v:.2f}" if not np.isnan(v) else "" for v in row] for row in df.values]
                        _fig  = go.Figure(go.Heatmap(
                            z=_z,
                            x=list(df.columns),
                            y=list(df.index),
                            text=_text,
                            texttemplate="%{text}",
                            textfont=dict(size=12),
                            colorscale=[
                                [0.0, "#2166ac"], [0.35, "#92c5de"],
                                [0.5, "#f7f7f7"],
                                [0.65, "#f4a582"], [1.0, "#d6604d"],
                            ],
                            zmid=0, zmin=-1, zmax=1,
                            colorbar=dict(title="r", thickness=12, len=0.8),
                        ))
                        _fig.update_layout(
                            title=title,
                            height=160 + _n_interp * 40,
                            margin=dict(t=40, b=20, l=160, r=40),
                            yaxis=dict(autorange="reversed"),
                        )
                        st.plotly_chart(_fig, use_container_width=True, key=f"pca_corr_{key_sfx}_{prov}")

                    _ic1, _ic2 = st.columns(2)
                    with _ic1:
                        _corr_heatmap(_shape_df, "Shape Correlation (loading ↔ hourly mean)", "shape")
                    with _ic2:
                        _corr_heatmap(_score_df, "Score Correlation (daily score ↔ daily avg)", "score")

                    # ── Per-PC interpretation summary ──────────────────────────
                    st.markdown("**Interpretation summary**")
                    _interp_cols = st.columns(_n_interp)
                    for _pci in range(_n_interp):
                        with _interp_cols[_pci]:
                            _pct_v = f"{res['variance_explained'][_pci]:.1f}%"
                            # Pick top shape and score driver
                            _shape_r = _shape_df.iloc[_pci] if not _shape_df.empty else pd.Series(dtype=float)
                            _score_r = _score_df.iloc[_pci] if not _score_df.empty and _pci < len(_score_df) else pd.Series(dtype=float)
                            _shape_best = _shape_r.abs().idxmax() if not _shape_r.empty else None
                            _score_best = _score_r.abs().idxmax() if not _score_r.empty else None
                            _shape_val  = _shape_r[_shape_best] if _shape_best else float("nan")
                            _score_val  = _score_r[_score_best] if _score_best else float("nan")
                            _sign_s = "↑" if _shape_val > 0 else "↓"
                            _sign_c = "↑" if _score_val > 0 else "↓"
                            st.metric(f"PC{_pci+1}", _pct_v)
                            if _shape_best and abs(_shape_val) >= 0.3:
                                st.caption(f"Shape: {_sign_s} {_shape_best} (r={_shape_val:.2f})")
                            else:
                                st.caption("Shape: no strong driver")
                            if _score_best and abs(_score_val) >= 0.3:
                                st.caption(f"Days: {_sign_c} {_score_best} (r={_score_val:.2f})")
                            else:
                                st.caption("Days: no strong driver")

        else:
            # ── Multi-province comparison: one chart per PC ────────────────
            n_show = min(n_pcs, min(len(r["loadings"]) for r in pca_results.values()))
            hours  = list(range(24))

            # PC loading overlay charts
            for pc_idx in range(n_show):
                fig_cmp = go.Figure()
                fig_cmp.add_hline(y=_ref_y, line_dash="dot",
                                  line_color="rgba(0,0,0,0.25)", line_width=1)
                for p_idx, (prov, res) in enumerate(pca_results.items()):
                    loading = _norm_loading(res["loadings"][pc_idx])
                    pct     = f"{res['variance_explained'][pc_idx]:.1f}"
                    fig_cmp.add_trace(go.Scatter(
                        x=hours, y=loading.tolist(),
                        mode="lines+markers",
                        line=dict(color=_PC_COLORS[p_idx % len(_PC_COLORS)], width=2),
                        marker=dict(size=4),
                        name=f"{prov} ({pct}%)",
                    ))
                fig_cmp.update_layout(
                    title=f"PC{pc_idx+1} — {_y_label} by province",
                    xaxis=dict(title=_t("pca_hour"), dtick=2),
                    yaxis=dict(title=_y_label),
                    height=300, margin=dict(t=40, b=30, l=40, r=10),
                    legend=dict(orientation="h", y=-0.3),
                )
                st.plotly_chart(fig_cmp, use_container_width=True, key=f"pca_cmp_pc{pc_idx+1}")

            # Variance-explained comparison table
            st.markdown("**Variance explained (%) by province**")
            ve_rows = {}
            for prov, res in pca_results.items():
                ve_rows[prov] = {f"PC{i+1}": f"{res['variance_explained'][i]:.1f}%"
                                 for i in range(n_show)}
            st.dataframe(pd.DataFrame(ve_rows).T, use_container_width=True)

# ── Tab 4: BESS Demand Analysis ───────────────────────────────────────────────
with tab_demand:
    st.subheader(_t("demand_title"))
    st.caption(_t("demand_caption"))

    _all_provs_d = load_province_list(_ENG_KEY)
    # Provinces with LingFeng fundamentals data — used as the default selection.
    # Excludes markets that have no spot_fundamentals_hourly rows (e.g. 冀北, 广州).
    _DEFAULT_DEMAND_PROVS = [
        "云南", "吉林", "四川", "宁夏", "安徽", "山东", "山西", "广东",
        "广西", "新疆", "江苏", "江西", "河北南网", "河南", "浙江", "海南",
        "湖北", "湖南", "甘肃", "福建",
        "蒙东", "蒙西", "贵州", "辽宁", "陕西", "青海", "黑龙江",
    ]
    _default_d = [p for p in _DEFAULT_DEMAND_PROVS if p in _all_provs_d]
    if "demand_provinces" not in st.session_state:
        _dp_saved = [p for p in st.query_params.get("dp", "").split(",") if p in _all_provs_d]
        st.session_state["demand_provinces"] = _dp_saved if _dp_saved else _default_d
    _d_col_sel, _d_col_yr = st.columns([3, 1])
    with _d_col_sel:
        _d_provs = st.multiselect(
            _t("demand_province"),
            options=_all_provs_d,
            key="demand_provinces",
        )
    with _d_col_yr:
        _cap_src = st.radio(
            "Installed capacity source",
            ["Monthly (latest)", "Annual 2025", "Annual 2024"],
            key="demand_cap_src",
            horizontal=False,
        )
        _fund_yr = 2025 if _cap_src != "Annual 2024" else 2024
    # Persist demand province selection to URL
    if _d_provs:
        st.query_params["dp"] = ",".join(_d_provs)
    elif "dp" in st.query_params:
        del st.query_params["dp"]

    if not _d_provs:
        st.info(_t("demand_no_hourly"))
    else:
        # ── ① Arbitrage Sizing ───────────────────────────────────────────
        st.markdown(f"### {_t('demand_arb_title')}")
        st.caption(_t("demand_arb_caption"))

        _swing_df = load_demand_hourly(_ENG_KEY, tuple(sorted(_d_provs)), sel_start, sel_end)

        if _swing_df.empty:
            st.warning(_t("demand_no_hourly"))
        else:
            # Single-province intraday profile picker
            _d_prov_single = st.selectbox(
                "Province for intraday profile",
                options=_d_provs,
                key="demand_profile_prov",
            )
            _profile_df = load_demand_intraday_profile(
                _ENG_KEY, _d_prov_single, sel_start, sel_end)

            _arb_col1, _arb_col2 = st.columns(2)

            with _arb_col1:
                # Mean intraday profile: load / renewable / bidding space
                if not _profile_df.empty:
                    _hours = _profile_df["hour"].tolist()
                    fig_profile = go.Figure()
                    # Load
                    if _profile_df["load_mean"].notna().any():
                        fig_profile.add_trace(go.Scatter(
                            x=_hours, y=_profile_df["load_mean"].tolist(),
                            name="Load", mode="lines",
                            line=dict(color="#9E9E9E", width=2, dash="dot"),
                        ))
                    # Renewable
                    if _profile_df["renewable_mean"].notna().any():
                        fig_profile.add_trace(go.Scatter(
                            x=_hours, y=_profile_df["renewable_mean"].tolist(),
                            name="Renewable", mode="lines",
                            line=dict(color="#54A24B", width=2),
                            fill="tozeroy", fillcolor="rgba(84,162,75,0.10)",
                        ))
                    # Bidding space ± 1 std band
                    _bs_mean = _profile_df["bs_mean"].tolist()
                    _bs_std  = _profile_df["bs_std"].fillna(0).tolist()
                    _bs_hi   = [m + s for m, s in zip(_bs_mean, _bs_std)]
                    _bs_lo   = [max(m - s, 0) for m, s in zip(_bs_mean, _bs_std)]
                    fig_profile.add_trace(go.Scatter(
                        x=_hours + _hours[::-1],
                        y=_bs_hi + _bs_lo[::-1],
                        fill="toself",
                        fillcolor="rgba(76,120,168,0.15)",
                        line=dict(width=0), showlegend=False, hoverinfo="skip",
                    ))
                    fig_profile.add_trace(go.Scatter(
                        x=_hours, y=_bs_mean,
                        name="Bidding Space", mode="lines",
                        line=dict(color="#4C78A8", width=2.5),
                    ))
                    # Annotate the swing: peak and trough
                    _pk_idx = int(np.argmax(_bs_mean))
                    _tr_idx = int(np.argmin(_bs_mean))
                    _swing_val = _bs_mean[_pk_idx] - _bs_mean[_tr_idx]
                    fig_profile.add_annotation(
                        x=_hours[_pk_idx], y=_bs_mean[_pk_idx],
                        text=f"↑ {_bs_mean[_pk_idx]:,.0f} MW",
                        showarrow=True, arrowhead=2, ax=0, ay=-30,
                        font=dict(size=11, color="#4C78A8"),
                    )
                    fig_profile.add_annotation(
                        x=_hours[_tr_idx], y=_bs_mean[_tr_idx],
                        text=f"↓ {_bs_mean[_tr_idx]:,.0f} MW",
                        showarrow=True, arrowhead=2, ax=0, ay=30,
                        font=dict(size=11, color="#E45756"),
                    )
                    fig_profile.update_layout(
                        title=f"{_d_prov_single} — {_t('demand_profile_title')}<br>"
                              f"<sup>Swing = {_swing_val:,.0f} MW → Max BESS = {_swing_val/2:,.0f} MW</sup>",
                        xaxis=dict(title="Hour", dtick=2),
                        yaxis=dict(title="MW"),
                        height=340, margin=dict(t=60, b=30, l=50, r=10),
                        legend=dict(orientation="h", y=-0.25),
                    )
                    st.plotly_chart(fig_profile, use_container_width=True, key="demand_profile_chart")
                else:
                    st.warning(_t("demand_no_hourly"))

            with _arb_col2:
                # Time series of daily BESS_arb_mw for all selected provinces
                fig_swing = go.Figure()
                for _p_idx, _prov in enumerate(_d_provs):
                    _pdf = _swing_df[_swing_df["province"] == _prov]
                    if _pdf.empty:
                        continue
                    fig_swing.add_trace(go.Scatter(
                        x=_pdf["date"].tolist(),
                        y=_pdf["bess_arb_mw"].tolist(),
                        name=_prov, mode="lines",
                        line=dict(color=_PC_COLORS[_p_idx % len(_PC_COLORS)], width=1.5),
                    ))
                fig_swing.update_layout(
                    title=_t("demand_swing_title"),
                    xaxis_title="Date",
                    yaxis_title=_t("demand_swing_y"),
                    height=340, margin=dict(t=40, b=30, l=50, r=10),
                    legend=dict(orientation="h", y=-0.3),
                )
                st.plotly_chart(fig_swing, use_container_width=True, key="demand_swing_ts")

        # ── ② Frequency Response Sizing ──────────────────────────────────
        st.markdown(f"### {_t('demand_fr_title')}")
        st.caption(_t("demand_fr_caption"))
        st.caption(_t("demand_source_note"))

        try:
            from services.market_fundamentals.loader import (
                load_province_data_from_db as _load_fund_db,
                load_latest_installed_monthly as _load_monthly,
            )
            _dsn = (
                os.environ.get("PGURL")
                or os.environ.get("DATABASE_URL")
                or "postgresql://postgres:root@127.0.0.1:5433/marketdata"
            )
            _fund_all = _load_fund_db(_dsn)
            _monthly_all = _load_monthly(_dsn) if _cap_src == "Monthly (latest)" else {}
        except Exception:
            _fund_all = {}
            _monthly_all = {}

        if not _fund_all:
            st.warning(_t("demand_no_fund"))
        else:
            # Show data source label
            if _monthly_all:
                _sample_ym = next(iter(_monthly_all.values()), {}).get("year_month")
                _src_label = f"Monthly data ({_sample_ym})" if _sample_ym else "Monthly data"
            else:
                _src_label = f"Annual {_fund_yr} fundamentals"
            st.caption(f"Installed capacity source: **{_src_label}**")

            _FR_FUND_ALIAS = {"河北南网": "冀南", "冀南": "河北南网"}
            _fr_rows = []
            for _prov in _d_provs:
                _pdata = _fund_all.get(_prov) or _fund_all.get(_FR_FUND_ALIAS.get(_prov, ""), {})
                # Peak load always comes from annual fundamentals (not in monthly files)
                _pl = _pdata.get("peak_load", {}).get(_fund_yr, {})
                _peak_load_mw = max((_pl.get("summer") or 0), (_pl.get("winter") or 0))

                _mon = _monthly_all.get(_prov) or _monthly_all.get(_FR_FUND_ALIAS.get(_prov, ""))
                if _mon:
                    # Monthly data in MW directly
                    _wind_mw     = _mon.get("wind_mw")    or 0.0
                    _solar_mw    = _mon.get("solar_mw")   or 0.0
                    _storage_mw  = _mon.get("bess_mw")    or 0.0
                    _renew_mw    = _wind_mw + _solar_mw
                    # Display in 万kW for table (÷10)
                    _wind_wkw    = _wind_mw  / 10.0
                    _solar_wkw   = _solar_mw / 10.0
                    _storage_wkw = _storage_mw / 10.0
                    _renew_wkw   = _renew_mw / 10.0
                else:
                    # Fall back to annual fundamentals (万kW → MW via ×10)
                    _cap_yr = _pdata.get("capacity", {}).get(_fund_yr, {})
                    _wind_wkw    = (_cap_yr.get("风电", {}) or {}).get("value") or 0.0
                    _solar_wkw   = (_cap_yr.get("光伏", {}) or {}).get("value") or 0.0
                    _storage_wkw = (_cap_yr.get("储能", {}) or {}).get("value") or 0.0
                    _renew_wkw   = _wind_wkw + _solar_wkw
                    _wind_mw     = _wind_wkw    * 10
                    _solar_mw    = _solar_wkw   * 10
                    _storage_mw  = _storage_wkw * 10
                    _renew_mw    = _renew_wkw   * 10

                _rule_desc, _pct_load, _pct_renew_inst, _floor_mw = _FR_RULES.get(_prov, _FR_DEFAULT)
                _fr_mw = max(_floor_mw, _peak_load_mw * _pct_load + _renew_mw * _pct_renew_inst)
                _eff_pct = (_fr_mw / _renew_mw * 100) if _renew_mw > 0 else 0.0
                _fr_rows.append({
                    _t("demand_province"):     _prov,
                    _t("demand_fr_peak_load"): round(_peak_load_mw, 0),
                    _t("demand_fr_wind"):      round(_wind_wkw,  1),
                    _t("demand_fr_solar"):     round(_solar_wkw, 1),
                    _t("demand_fr_renewable"): round(_renew_wkw, 1),
                    _t("demand_fr_bess_cap"):  round(_storage_wkw, 1),
                    _t("demand_fr_pct"):       f"{_eff_pct:.1f}%",
                    _t("demand_fr_req_mw"):    round(_fr_mw, 0),
                    _t("demand_fr_rule"):      _rule_desc,
                })
            if _fr_rows:
                _fr_demand_df = pd.DataFrame(_fr_rows).set_index(_t("demand_province"))
                st.dataframe(_fr_demand_df, use_container_width=True)

                # Bar chart of FR requirement
                fig_fr = go.Figure(go.Bar(
                    x=_fr_demand_df.index.tolist(),
                    y=_fr_demand_df[_t("demand_fr_req_mw")].tolist(),
                    marker_color="#F58518",
                    text=[f"{v:,.0f}" for v in _fr_demand_df[_t("demand_fr_req_mw")]],
                    textposition="outside",
                ))
                fig_fr.update_layout(
                    title=f"{_t('demand_fr_title')} — {_fund_yr} installed capacity",
                    xaxis_title=_t("demand_province"),
                    yaxis_title="MW",
                    height=320, margin=dict(t=40, b=60, l=50, r=10),
                )
                st.plotly_chart(fig_fr, use_container_width=True, key="demand_fr_bar")

        # ── ③ Combined comparison ─────────────────────────────────────────
        if not _swing_df.empty and _fund_all:
            st.markdown(f"### {_t('demand_compare_title')}")
            st.caption(_t("demand_compare_caption"))

            _cmp_rows = []
            for _prov in _d_provs:
                _pdf = _swing_df[_swing_df["province"] == _prov]["bess_arb_mw"]
                _arb_p50 = float(_pdf.quantile(0.50)) if not _pdf.empty else 0.0
                _arb_p90 = float(_pdf.quantile(0.90)) if not _pdf.empty else 0.0

                # province_fundamentals uses 冀南, but spot_prices_hourly/demand uses 河北南网.
                # Resolve alias so thermal/peak data is found for both names.
                _FUND_ALIAS = {"河北南网": "冀南", "冀南": "河北南网"}
                _pdata = _fund_all.get(_prov) or _fund_all.get(_FUND_ALIAS.get(_prov, ""), {})
                _pl = _pdata.get("peak_load", {}).get(_fund_yr, {})
                _peak_load_mw = max((_pl.get("summer") or 0), (_pl.get("winter") or 0))
                _mon = _monthly_all.get(_prov) or _monthly_all.get(_FUND_ALIAS.get(_prov, ""))
                if _mon:
                    _renew_mw   = _mon.get("renew_mw")  or 0.0
                    _storage_mw = _mon.get("bess_mw")   or 0.0
                else:
                    _cap_yr = _pdata.get("capacity", {}).get(_fund_yr, {})
                    _wind_wkw    = (_cap_yr.get("风电", {}) or {}).get("value") or 0.0
                    _solar_wkw   = (_cap_yr.get("光伏", {}) or {}).get("value") or 0.0
                    _storage_wkw = (_cap_yr.get("储能", {}) or {}).get("value") or 0.0
                    _renew_mw    = (_wind_wkw + _solar_wkw) * 10
                    _storage_mw  = _storage_wkw * 10
                _, _pct_load, _pct_renew_inst, _floor_mw = _FR_RULES.get(_prov, _FR_DEFAULT)
                _fr_mw = max(_floor_mw, _peak_load_mw * _pct_load + _renew_mw * _pct_renew_inst)

                # Thermal: annual fundamentals first (most reliable), fall back to monthly installed data
                _cap_yr_fund = _pdata.get("capacity", {}).get(_fund_yr, {})
                _thermal_wkw = (_cap_yr_fund.get("火电", {}) or {}).get("value") or 0.0
                _thermal_mw  = _thermal_wkw * 10
                if _thermal_mw == 0 and _mon and _mon.get("thermal_mw"):
                    _thermal_mw = float(_mon["thermal_mw"])

                # Flexible thermal = total thermal − avg daily min bidding-space
                # (min bidding-space ≈ must-run thermal floor; what remains can be dispatched flexibly)
                _prov_sf = _swing_df[_swing_df["province"] == _prov]
                _avg_min_bs   = float(_prov_sf["min_bs"].mean()) if not _prov_sf.empty else 0.0
                _flex_thermal = max(_thermal_mw - _avg_min_bs, 0.0)

                _recommended = max(_arb_p90, _fr_mw)
                _gap_mw = max(_recommended - _storage_mw, 0.0)
                _cmp_rows.append({
                    "Province":                    _prov,
                    "_sort_arb":                   _arb_p50,
                    _t("demand_arb_p50"):          round(_arb_p50,      0),
                    _t("demand_arb_p90"):          round(_arb_p90,      0),
                    _t("demand_fr_req_mw"):        round(_fr_mw,        0),
                    _t("demand_recommended"):      round(_recommended,  0),
                    _t("demand_bess_installed"):   round(_storage_mw,   0),
                    _t("demand_bess_gap"):         round(_gap_mw,       0),
                    _t("demand_flex_thermal"):     round(_flex_thermal,  0),
                })
            if _cmp_rows:
                _cmp_df = (
                    pd.DataFrame(_cmp_rows)
                    .sort_values("_sort_arb", ascending=False)
                    .drop(columns=["_sort_arb"])
                    .set_index("Province")
                )

                _provs_list       = _cmp_df.index.tolist()
                _arb_p50_vals     = _cmp_df[_t("demand_arb_p50")].tolist()
                _arb_p90_vals     = _cmp_df[_t("demand_arb_p90")].tolist()
                _fr_vals          = _cmp_df[_t("demand_fr_req_mw")].tolist()
                _bess_vals        = _cmp_df[_t("demand_bess_installed")].tolist()
                _flex_thermal_vals = _cmp_df[_t("demand_flex_thermal")].tolist()

                # ── colour palette ─────────────────────────────────────────
                _C_ARB     = "#4C78A8"   # blue  – arbitrage
                _C_FR      = "#F58518"   # orange – FR requirement
                _C_BESS    = "#E45756"   # red    – existing BESS deduction
                _C_NET     = "#2c3e50"   # dark   – net demand total
                _C_THERM   = "rgba(180,140,70,0.65)"  # amber – flexible thermal

                def _waterfall_chart(arb_vals, arb_label, title, chart_key):
                    """
                    Waterfall-style chart built from individual go.Bar traces so each
                    component gets its own distinct colour (go.Waterfall only supports
                    increasing/decreasing/totals colouring which forces Arb and FR to share
                    the same blue).

                    Layout per province (5 sequential columns):
                      Col 1  Arb   — blue,   base=0,          y=arb
                      Col 2  +FR   — orange, base=arb,        y=fr
                      Col 3  −BESS — red,    base=net,        y=min(bess, arb+fr)
                      Col 4  Net   — dark,   base=0,          y=max(arb+fr−bess, 0)
                      Col 5  Thml  — amber,  base=0,          y=flex_thermal
                    """
                    _n = len(_provs_list)
                    _step_arb   = arb_label
                    _step_fr    = f"+ {_t('demand_fr_req_mw')}"
                    _step_bess  = f"− {_t('demand_bess_installed')}"
                    _step_net   = _t("demand_net_demand")
                    _step_therm = _t("demand_flex_thermal")

                    # Pre-compute per-province positions for each Bar trace
                    _net_vals   = []
                    _fr_base    = []   # FR starts on top of Arb
                    _bess_base  = []   # BESS bar starts at net (bottom of deduction)
                    _bess_y     = []   # BESS bar height = amount deducted (capped at arb+fr)

                    for arb, fr, bess in zip(arb_vals, _fr_vals, _bess_vals):
                        net = max(arb + fr - bess, 0.0)
                        _net_vals.append(net)
                        _fr_base.append(arb)
                        _bess_base.append(net)                      # bottom of red bar
                        _bess_y.append(min(bess, arb + fr))         # height of red bar

                    def _mx(step):
                        """Multi-level x for a single-step Bar trace: all provinces."""
                        return [_provs_list, [step] * _n]

                    fig = go.Figure()

                    # Col 1 — Arbitrage (blue, base=0)
                    fig.add_trace(go.Bar(
                        x=_mx(_step_arb), y=arb_vals, base=[0] * _n,
                        name=arb_label, marker_color=_C_ARB,
                        text=[f"{v:,.0f}" for v in arb_vals],
                        textposition="outside", textfont=dict(size=9),
                    ))

                    # Col 2 — FR requirement (orange, stacks on Arb)
                    fig.add_trace(go.Bar(
                        x=_mx(_step_fr), y=_fr_vals, base=_fr_base,
                        name=_step_fr, marker_color=_C_FR,
                        text=[f"+{v:,.0f}" for v in _fr_vals],
                        textposition="outside", textfont=dict(size=9),
                    ))

                    # Col 3 — Existing BESS deduction (red, fills from net up to arb+fr)
                    fig.add_trace(go.Bar(
                        x=_mx(_step_bess), y=_bess_y, base=_bess_base,
                        name=_step_bess, marker_color=_C_BESS,
                        text=[f"−{v:,.0f}" if v > 0 else "0" for v in _bess_vals],
                        textposition="outside", textfont=dict(size=9),
                    ))

                    # Col 4 — Net BESS demand (dark, standalone from 0)
                    fig.add_trace(go.Bar(
                        x=_mx(_step_net), y=_net_vals, base=[0] * _n,
                        name=_step_net, marker_color=_C_NET,
                        text=[f"<b>{v:,.0f}</b>" for v in _net_vals],
                        textposition="outside", textfont=dict(size=10),
                    ))

                    # Col 5 — Flexible thermal reference (amber, standalone from 0)
                    if _show_thermal:
                        fig.add_trace(go.Bar(
                            x=_mx(_step_therm), y=_flex_thermal_vals, base=[0] * _n,
                            name=_step_therm, marker_color=_C_THERM, opacity=0.70,
                            text=[f"{v:,.0f}" for v in _flex_thermal_vals],
                            textposition="outside", textfont=dict(size=9),
                        ))

                    fig.update_layout(
                        title=dict(text=title, font=dict(size=14)),
                        xaxis=dict(tickangle=-40, tickfont=dict(size=10)),
                        yaxis=dict(title="MW", rangemode="tozero"),
                        height=520,
                        margin=dict(t=60, b=120, l=65, r=15),
                        legend=dict(orientation="h", y=-0.40, font=dict(size=11)),
                        plot_bgcolor="rgba(0,0,0,0)",
                        paper_bgcolor="rgba(0,0,0,0)",
                    )
                    st.plotly_chart(fig, use_container_width=True, key=chart_key)

                st.caption(_t("demand_waterfall_cap"))
                _show_thermal = st.checkbox(
                    _t("demand_flex_thermal"),
                    value=False,
                    key="demand_show_thermal",
                )
                _waterfall_chart(
                    _arb_p50_vals,
                    _t("demand_arb_p50"),
                    _t("demand_waterfall_p50_title"),
                    "demand_wf_p50",
                )
                _waterfall_chart(
                    _arb_p90_vals,
                    _t("demand_arb_p90"),
                    _t("demand_waterfall_p90_title"),
                    "demand_wf_p90",
                )
                st.dataframe(_cmp_df, use_container_width=True)

# ── Tab 5: System Operation Fee ───────────────────────────────────────────────

with tab_sysopfee:
    st.subheader(_t("sysopfee_title"))
    st.caption(_t("sysopfee_caption"))

    if _sof_df.empty:
        st.info(_t("sysopfee_no_data"))
    else:
        _all_provs_sof = sorted(_sof_df["province"].unique().tolist())

        # Province filter with persistent session state + URL params
        if "sysopfee_provinces" not in st.session_state:
            _sp_saved = [p for p in st.query_params.get("sp", "").split(",") if p in _all_provs_sof]
            st.session_state["sysopfee_provinces"] = _sp_saved if _sp_saved else _all_provs_sof

        _sof_provs = st.multiselect(
            _t("sysopfee_province"),
            options=_all_provs_sof,
            key="sysopfee_provinces",
        )
        if _sof_provs:
            st.query_params["sp"] = ",".join(_sof_provs)
        elif "sp" in st.query_params:
            del st.query_params["sp"]

        if not _sof_provs:
            st.info("Select at least one province.")
        else:
            _sof_filtered = _sof_df[_sof_df["province"].isin(_sof_provs)].copy()
            _sof_filtered["ym_str"] = _sof_filtered["year_month"].dt.strftime("%Y-%m")

            # ── Heatmap ──────────────────────────────────────────────────────
            _sof_pivot = (
                _sof_filtered
                .pivot_table(index="province", columns="ym_str", values="fee_yuan_kwh", aggfunc="mean")
                .reindex(columns=sorted(_sof_filtered["ym_str"].unique()))
            )
            # Sort provinces by mean fee descending
            _sof_pivot = _sof_pivot.loc[
                _sof_pivot.mean(axis=1).sort_values(ascending=False).index
            ]

            _z = _sof_pivot.values.tolist()
            _z_text = [
                [f"{v:.4f}" if v is not None and not (v != v) else "" for v in row]
                for row in _z
            ]

            _cols = _sof_pivot.columns.tolist()

            fig_sof_heat = go.Figure(go.Heatmap(
                z=_z,
                x=_cols,
                y=_sof_pivot.index.tolist(),
                text=_z_text,
                texttemplate="%{text}",
                textfont=dict(size=10),
                colorscale="RdYlGn_r",
                zmid=float(pd.DataFrame(_z).stack().median()),
                colorbar=dict(title="¥/kWh", thickness=14),
                hovertemplate="%{y}  %{x}<br>%{z:.4f} ¥/kWh<extra></extra>",
            ))

            # ── Default view: 2025-01 onward (scrollable to older data) ──────
            _idx_2025 = next((i for i, c in enumerate(_cols) if c >= "2025-01"), 0)
            _x_range = [_idx_2025 - 0.5, len(_cols) - 0.5]

            # ── Actual / Forecast divider ─────────────────────────────────────
            _today_ym = dt.datetime.now().strftime("%Y-%m")
            _fcast_idx = next((i for i, c in enumerate(_cols) if c >= _today_ym), None)
            _heat_shapes, _heat_annots = [], []
            if _fcast_idx is not None and 0 < _fcast_idx < len(_cols):
                # Use paper coords (0–1) to avoid corrupting the categorical x-axis
                _frac = _fcast_idx / len(_cols)
                _heat_shapes.append(dict(
                    type="line", xref="paper", yref="paper",
                    x0=_frac, x1=_frac, y0=0, y1=1,
                    line=dict(color="white", width=2, dash="dash"),
                ))
                _heat_annots += [
                    dict(xref="paper", yref="paper",
                         x=_frac / 2, y=1.04,
                         text="◀ Actual", showarrow=False,
                         font=dict(size=11, color="#444"), xanchor="center"),
                    dict(xref="paper", yref="paper",
                         x=_frac + (1 - _frac) / 2, y=1.04,
                         text="Forecast ▶", showarrow=False,
                         font=dict(size=11, color="#888", style="italic"), xanchor="center"),
                ]

            fig_sof_heat.update_layout(
                title=_t("sysopfee_heatmap_title"),
                xaxis=dict(title="Month", tickangle=-45, range=_x_range),
                yaxis=dict(title="Province", autorange="reversed"),
                height=max(320, 22 * len(_sof_provs) + 100),
                margin=dict(t=60, b=80, l=120, r=20),
                shapes=_heat_shapes,
                annotations=_heat_annots,
            )
            st.plotly_chart(fig_sof_heat, use_container_width=True, key="sof_heatmap")

            # ── Line chart ───────────────────────────────────────────────────
            fig_sof_line = go.Figure()
            for prov in _sof_pivot.index:
                prov_data = _sof_filtered[_sof_filtered["province"] == prov].sort_values("year_month")
                fig_sof_line.add_trace(go.Scatter(
                    name=prov,
                    x=prov_data["year_month"].tolist(),
                    y=prov_data["fee_yuan_kwh"].tolist(),
                    mode="lines+markers",
                    marker=dict(size=5),
                ))
            _cutoff_dt = dt.datetime.now().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            _cutoff_str = _cutoff_dt.strftime("%Y-%m-%d")
            fig_sof_line.add_vline(
                x=_cutoff_str,
                line=dict(color="gray", width=1.5, dash="dash"),
            )
            fig_sof_line.add_annotation(
                x=_cutoff_str, xref="x", yref="paper",
                y=1.02, xanchor="left", yanchor="bottom", showarrow=False,
                text="Forecast →",
                font=dict(size=11, color="gray"),
            )
            fig_sof_line.update_layout(
                title=_t("sysopfee_line_title"),
                xaxis_title="Month",
                yaxis_title="¥/kWh",
                height=380,
                margin=dict(t=40, b=60, l=60, r=10),
                legend=dict(orientation="h", y=-0.3),
                hovermode="x unified",
            )
            st.plotly_chart(fig_sof_line, use_container_width=True, key="sof_line")


# ── Tab 6: Capacity Compensation + FR Market ──────────────────────────────────
with tab_aux:
    import requests as _requests

    st.subheader(_t("aux_title"))
    st.caption(_t("aux_caption"))

    # Refresh button + live scan progress
    _hermes_url = os.environ.get("HERMES_URL", "")

    # Poll scan status (lightweight GET, no DB)
    _scan_st: dict = {}
    if _hermes_url:
        try:
            _sr = _requests.get(_hermes_url.rstrip("/") + "/hermes/capcomp/status", timeout=3)
            if _sr.status_code == 200:
                _scan_st = _sr.json()
        except Exception:
            pass

    _scan_running = bool(_scan_st.get("running"))

    _btn_col, _status_col = st.columns([2, 5])
    with _btn_col:
        if st.button(_t("aux_refresh_btn"), key="aux_refresh", disabled=_scan_running):
            if not _hermes_url:
                st.warning("HERMES_URL not configured in environment.")
            else:
                try:
                    _resp = _requests.post(_hermes_url.rstrip("/") + "/hermes/capcomp/scan", timeout=5)
                    if _resp.status_code == 200:
                        st.success(_t("aux_refresh_started"))
                        st.rerun()
                    else:
                        st.warning(f"Hermes returned {_resp.status_code}: {_resp.text[:100]}")
                except Exception as _re:
                    st.warning(f"Could not reach Hermes: {_re}")

    if _scan_running:
        _done = int(_scan_st.get("provinces_done", 0))
        _total = int(_scan_st.get("provinces_total", len([])) or 32)
        _prov = _scan_st.get("current_province", "…")
        _cap_n = int(_scan_st.get("cap_comp_found", 0))
        _fr_n = int(_scan_st.get("fr_found", 0))
        with _status_col:
            st.progress(_done / _total if _total else 0,
                        text=_t("aux_scanning").format(done=_done, total=_total, province=_prov))
            st.caption(_t("aux_scan_results").format(cap=_cap_n, fr=_fr_n))
        # Auto-refresh every 8 seconds while scan is running
        import time as _time
        _time.sleep(8)
        st.cache_data.clear()
        st.rerun()

    # Load data
    _inst_df = load_installed_capacity(_ENG_KEY)

    # Province filter (union of all three tables)
    _aux_all_provs = sorted(set(
        list(_cc_df["province"].unique() if not _cc_df.empty else []) +
        list(_fr_df["province"].unique() if not _fr_df.empty else []) +
        list(_inst_df["province"].unique() if not _inst_df.empty else [])
    ))
    if _aux_all_provs and not st.session_state["aux_provinces"]:
        _ap_saved = [p for p in st.query_params.get("ap", "").split(",") if p in _aux_all_provs]
        st.session_state["aux_provinces"] = _ap_saved if _ap_saved else _aux_all_provs

    _aux_years = sorted(set(
        list(_cc_df["effective_date"].apply(lambda d: d.year).unique() if not _cc_df.empty else []) +
        list(_fr_df["effective_date"].apply(lambda d: d.year).unique() if not _fr_df.empty else [])
    ), reverse=True) or [dt.datetime.now().year]

    _aux_col1, _aux_col2 = st.columns([3, 1])
    with _aux_col1:
        _aux_sel_provs = st.multiselect(
            _t("aux_province_filter"),
            options=_aux_all_provs,
            key="aux_provinces",
        )
        if _aux_sel_provs:
            st.query_params["ap"] = ",".join(_aux_sel_provs)
        elif "ap" in st.query_params:
            del st.query_params["ap"]
    with _aux_col2:
        _aux_sel_year = st.selectbox(
            _t("aux_year_filter"),
            options=_aux_years,
            key="aux_year",
        )

    import pandas as _pd

    def _style_conflicts(df):
        """Highlight conflict rows in orange."""
        def _row_style(row):
            if row.get("status") == "conflict":
                return ["background-color: #fff3cd"] * len(row)
            return [""] * len(row)
        return df.style.apply(_row_style, axis=1)

    # ── Section 1: 容量补偿 ──────────────────────────────────────────────
    st.markdown(f"### {_t('aux_cap_section')}")
    if _cc_df.empty:
        st.info(_t("aux_no_data"))
    else:
        _cc_filt = _cc_df.copy()
        if _aux_sel_provs:
            _cc_filt = _cc_filt[_cc_filt["province"].isin(_aux_sel_provs)]
        _cc_filt = _cc_filt[_cc_filt["effective_date"].apply(
            lambda x: x.year if hasattr(x, "year") else 0) == _aux_sel_year]
        # Latest confirmed per province + all conflicts
        _cc_conf = _cc_filt[_cc_filt["status"] == "confirmed"].drop_duplicates(
            subset=["province"], keep="first")
        _cc_conf_prov = _cc_conf["province"].tolist() if not _cc_conf.empty else []
        _cc_confl = _cc_filt[_cc_filt["status"] == "conflict"]
        _cc_show = _pd.concat([_cc_conf, _cc_confl], ignore_index=True) if not _cc_confl.empty else _cc_conf
        if _cc_show.empty:
            st.info(_t("aux_no_data"))
        else:
            _cc_disp = _cc_show[["province", "cap_comp_yuan_kw", "peak_duration_hours",
                                  "effective_date", "notes", "source", "status"]].copy()
            _cc_disp["source"] = _cc_disp["source"].apply(lambda s: str(s)[:60] if s else "")
            _cc_disp["notes"] = _cc_disp["notes"].fillna("")
            _cc_disp = _cc_disp.rename(columns={
                "province":            _t("rank_col_province"),
                "cap_comp_yuan_kw":    _t("aux_cap_rate"),
                "peak_duration_hours": _t("aux_peak_hours"),
                "effective_date":      _t("aux_eff_date"),
                "notes":               _t("aux_notes"),
                "source":              _t("aux_source"),
                "status":              _t("aux_status"),
            })
            st.dataframe(_style_conflicts(_cc_disp), use_container_width=True, hide_index=True)

    # ── Section 2: 调频市场 (all historical rows) ────────────────────────
    st.markdown(f"### {_t('aux_fr_section')}")
    st.caption(_t("aux_fr_history_note"))
    if _fr_df.empty:
        st.info(_t("aux_no_data"))
    else:
        _fr_filt = _fr_df.copy()
        if _aux_sel_provs:
            _fr_filt = _fr_filt[_fr_filt["province"].isin(_aux_sel_provs)]
        # Show ALL historical rows (no year filter, no dedup) — FR data changes monthly
        _fr_filt = _fr_filt.sort_values(["province", "effective_date"], ascending=[True, False])
        if _fr_filt.empty:
            st.info(_t("aux_no_data"))
        else:
            _fr_disp = _fr_filt[["province", "fr_price_yuan_kw_h", "fr_pool_billion_yuan",
                                   "effective_date", "source", "status"]].copy()
            _fr_disp["source"] = _fr_disp["source"].apply(lambda s: str(s)[:60] if s else "")
            _fr_disp = _fr_disp.rename(columns={
                "province":             _t("rank_col_province"),
                "fr_price_yuan_kw_h":   _t("aux_fr_price"),
                "fr_pool_billion_yuan": _t("aux_fr_pool"),
                "effective_date":       _t("aux_eff_date"),
                "source":               _t("aux_source"),
                "status":               _t("aux_status"),
            })
            st.dataframe(_style_conflicts(_fr_disp), use_container_width=True, hide_index=True)

    # ── 山东 ancillary cost trend ─────────────────────────────────────────
    _sd_anc_df = load_shandong_ancillary(_ENG_KEY)
    if not _sd_anc_df.empty:
        _sd_anc_df["month"] = _pd.to_datetime(_sd_anc_df["report_month"]).dt.strftime("%Y-%m")
        _sd_anc_df = _sd_anc_df.set_index("month")
        _fig_anc = go.Figure()
        _anc_series = {
            "fr_pool_million_yuan":               "调频 (M¥)",
            "peak_shaving_million_yuan":          "调峰 (M¥)",
            "renewable_deviation_million_yuan":   "偏差考核 (M¥)",
            "total_ancillary_million_yuan":       "总辅助服务 (M¥)",
        }
        for _col, _label in _anc_series.items():
            if _col in _sd_anc_df.columns and _sd_anc_df[_col].notna().any():
                _fig_anc.add_trace(go.Scatter(
                    x=list(_sd_anc_df.index),
                    y=list(_sd_anc_df[_col]),
                    name=_label,
                    mode="lines+markers",
                ))
        _fig_anc.update_layout(
            title="山东 月度辅助服务费用 (百万元/月)",
            xaxis_title="月份",
            yaxis_title="百万元 (M¥)",
            height=320,
            margin=dict(l=0, r=0, t=36, b=0),
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(_fig_anc, use_container_width=True)

    # ── Section 3: BESS Installed Capacity ──────────────────────────────
    st.markdown(f"### {_t('aux_bess_section')}")
    if _inst_df.empty:
        st.info(_t("aux_no_data"))
    else:
        _inst_filt = _inst_df.copy()
        if _aux_sel_provs:
            _inst_filt = _inst_filt[_inst_filt["province"].isin(_aux_sel_provs)]
        # Latest row per province
        _inst_latest = _inst_filt.drop_duplicates(subset=["province"], keep="first")
        if _inst_latest.empty:
            st.info(_t("aux_no_data"))
        else:
            _inst_disp = _inst_latest[["province", "year_month", "bess_mw", "wind_mw",
                                        "solar_mw", "total_mw", "source_file"]].copy()
            _inst_disp["year_month"] = _inst_disp["year_month"].apply(
                lambda d: d.strftime("%Y-%m") if hasattr(d, "strftime") else str(d)[:7])
            _inst_disp["source_file"] = _inst_disp["source_file"].apply(
                lambda s: str(s)[:60] if s else "")
            _inst_disp = _inst_disp.rename(columns={
                "province":    _t("rank_col_province"),
                "year_month":  _t("aux_year_month"),
                "bess_mw":     _t("aux_bess_mw"),
                "wind_mw":     "Wind (MW)",
                "solar_mw":    "Solar (MW)",
                "total_mw":    _t("aux_bess_total"),
                "source_file": _t("aux_bess_source"),
            })
            st.dataframe(_inst_disp, use_container_width=True, hide_index=True)

        # ── Conflict resolution ──────────────────────────────────────────────
        _cc_conflicts = _cc_df[_cc_df["status"] == "conflict"] if not _cc_df.empty else _pd.DataFrame()
        _fr_conflicts = _fr_df[_fr_df["status"] == "conflict"] if not _fr_df.empty else _pd.DataFrame()
        _n_conflicts = len(_cc_conflicts) + len(_fr_conflicts)

        if _n_conflicts > 0:
            with st.expander(f"{_t('aux_conflict_section')} ({_n_conflicts} items)"):
                if not _hermes_url:
                    st.warning("HERMES_URL not configured — cannot resolve conflicts from UI.")

                def _resolve_btn(table, row_keep, row_drop, label):
                    btn_key = f"resolve_{table}_{row_keep}_{row_drop}"
                    if st.button(f"{_t('aux_confirm_btn')} — {label}", key=btn_key):
                        if _hermes_url:
                            try:
                                _r = _requests.post(
                                    _hermes_url.rstrip("/") + "/hermes/capcomp/resolve",
                                    json={"table": table, "row_id_keep": row_keep, "row_id_drop": row_drop},
                                    timeout=5,
                                )
                                if _r.status_code == 200:
                                    st.success(f"Resolved: kept row {row_keep}")
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.error(f"Error: {_r.text[:100]}")
                            except Exception as _re:
                                st.error(f"Request failed: {_re}")
                        else:
                            st.warning("HERMES_URL not configured.")

                # Group cap_comp conflicts by (province, effective_date)
                if not _cc_conflicts.empty:
                    st.markdown("**容量补偿 conflicts:**")
                    for (prov, eff_dt), grp in _cc_conflicts.groupby(["province", "effective_date"]):
                        st.markdown(f"*{prov} — {eff_dt}*")
                        _grp_rows = grp.reset_index(drop=True)
                        _cols = st.columns(len(_grp_rows))
                        for i, (_, row) in enumerate(_grp_rows.iterrows()):
                            with _cols[i]:
                                st.write(f"**{row.get('cap_comp_yuan_kw')} ¥/kW**")
                                st.caption(str(row.get("source", ""))[:80])
                                # resolve against all other rows in group
                                for _, other_row in _grp_rows.iterrows():
                                    if other_row["id"] != row["id"]:
                                        _resolve_btn(
                                            "province_cap_comp",
                                            int(row["id"]), int(other_row["id"]),
                                            f"{row.get('cap_comp_yuan_kw')} ¥/kW",
                                        )

                # Group fr_market conflicts by (province, effective_date)
                if not _fr_conflicts.empty:
                    st.markdown("**调频市场 conflicts:**")
                    for (prov, eff_dt), grp in _fr_conflicts.groupby(["province", "effective_date"]):
                        st.markdown(f"*{prov} — {eff_dt}*")
                        _grp_rows = grp.reset_index(drop=True)
                        _cols = st.columns(len(_grp_rows))
                        for i, (_, row) in enumerate(_grp_rows.iterrows()):
                            with _cols[i]:
                                st.write(f"**{row.get('fr_price_yuan_kw_h')} ¥/kW·h**")
                                st.caption(str(row.get("source", ""))[:80])
                                for _, other_row in _grp_rows.iterrows():
                                    if other_row["id"] != row["id"]:
                                        _resolve_btn(
                                            "province_fr_market",
                                            int(row["id"]), int(other_row["id"]),
                                            f"{row.get('fr_price_yuan_kw_h')} ¥/kW·h",
                                        )

    # ── Data Gaps expander ────────────────────────────────────────────────────
    with st.expander("📋 Data Gaps — 容量补偿 / 调频市场 / 装机容量", expanded=False):
        import plotly.express as _px
        import pandas as _gap_pd

        _hermes_url_fill = os.environ.get("HERMES_URL", "").rstrip("/")

        _gap_cc, _gap_fr, _gap_inst = load_monthly_gaps(_ENG_KEY)

        _gap_tabs = st.tabs(["容量补偿", "调频市场", "装机容量"])
        _gap_data = [
            (_gap_tabs[0], _gap_cc,   "province_cap_comp",          "cap_comp_yuan_kw",   "容量补偿标准 (¥/kW·年)"),
            (_gap_tabs[1], _gap_fr,   "province_fr_market",         "fr_price_yuan_kw_h", "调频容量价格 (¥/kW·h)"),
            (_gap_tabs[2], _gap_inst, "province_installed_monthly", "installed_mw",       "储能装机 (MW)"),
        ]

        for _gap_tab, _df, _fill_table, _field1, _field1_label in _gap_data:
            with _gap_tab:
                if _df.empty:
                    st.info("暂无数据")
                    continue

                _pivot = _df.pivot(index="province", columns="year_month", values="has_data").fillna(False)
                _z = _pivot.values.astype(int)
                _fig = _px.imshow(
                    _z,
                    x=list(_pivot.columns),
                    y=list(_pivot.index),
                    color_continuous_scale=[[0, "#ef4444"], [1, "#22c55e"]],
                    zmin=0, zmax=1,
                    aspect="auto",
                    labels={"color": "有数据"},
                )
                _fig.update_layout(
                    height=max(250, len(_pivot.index) * 22),
                    margin=dict(t=10, b=40, l=140, r=10),
                    coloraxis_showscale=False,
                )
                _fig.update_xaxes(tickangle=-45)
                st.plotly_chart(_fig, use_container_width=True, key=f"gap_{_fill_table}")

                _missing_rows = _df[~_df["has_data"].astype(bool)]
                if not _missing_rows.empty and _hermes_url_fill:
                    st.caption(f"🔴 {len(_missing_rows)} 条数据缺失。可在下方手动填写：")
                    with st.form(key=f"fill_form_{_fill_table}"):
                        _col1, _col2, _col3 = st.columns(3)
                        _provs_missing = sorted(_missing_rows["province"].unique())
                        _months_missing = sorted(_missing_rows["year_month"].unique(), reverse=True)
                        with _col1:
                            _sel_prov = st.selectbox("省份", _provs_missing, key=f"fp_{_fill_table}")
                        with _col2:
                            _sel_month = st.selectbox("月份", _months_missing, key=f"fm_{_fill_table}")
                        with _col3:
                            _val1 = st.number_input(_field1_label, min_value=0.0, step=0.01,
                                                     key=f"fv1_{_fill_table}")
                        _submitted = st.form_submit_button("提交")
                        if _submitted:
                            import requests as _rq
                            try:
                                _resp = _rq.post(
                                    f"{_hermes_url_fill}/hermes/patrol/fill",
                                    json={
                                        "fill_table":    _fill_table,
                                        "fill_province": _sel_prov,
                                        "fill_month":    _sel_month,
                                        _field1:         _val1,
                                    },
                                    timeout=10,
                                )
                                if _resp.status_code == 200 and _resp.json().get("status") == "ok":
                                    st.success(f"✅ {_sel_prov} {_sel_month} 数据已提交。")
                                    st.cache_data.clear()
                                    st.rerun()
                                else:
                                    st.warning(f"提交失败：{_resp.text[:100]}")
                            except Exception as _fe:
                                st.warning(f"无法连接到 Hermes：{_fe}")
                elif _missing_rows.empty:
                    st.success("✅ 所有数据均已覆盖（近12个月）")
                else:
                    st.caption("请通过 Feishu 的 `/datacheck` 命令填写缺失数据，或配置 HERMES_URL。")


# ── Tab 7: Dispatch & Economics ──────────────────────────────────────────────
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
        econ = load_avg_economics(_ENG_KEY, irr_prov, irr_dur_h, sel_model)
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
        dgrad   = st.slider(_t("irr_degradation"),  0,   5,   2,  step=1) / 100.0

        # ── Revenue/cost components expander ─────────────────────────────────
        with st.expander(_t("irr_components_title"), expanded=False):
            _irr_fr_util = st.slider(
                _t("irr_fr_util"), 5, 80, 30, step=5, key="irr_fr_util"
            ) / 100.0
            _defs = _irr_defaults_for_province(
                irr_prov, irr_dur_h, _sof_df, _cc_df, _fr_df,
                rte=rte_pct / 100.0, fr_util_pct=_irr_fr_util,
            )
            sysopfee_input = st.number_input(
                f"系统运行费 ¥/MWh/day  [{_defs['sysopfee_src']}]",
                value=round(_defs["sysopfee_day"], 4),
                step=0.01, format="%.4f", key="irr_sysopfee",
            )
            cap_comp_input = st.number_input(
                f"容量补偿 ¥/MWh/day  [{_defs['cap_comp_src']}]",
                value=round(_defs["cap_comp_day"], 4),
                step=0.01, format="%.4f", key="irr_cap_comp",
            )
            fr_input = st.number_input(
                f"调频 ¥/MWh/day  [{_defs['fr_src']}]",
                value=round(_defs["fr_day"], 4),
                step=0.01, format="%.4f", key="irr_fr",
            )

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
                degradation=dgrad,
                equity_pct=equity,
                loan_rate=lr_pct,
                loan_tenure=tenure,
                project_life=life,
                sysopfee_per_mwh_day=sysopfee_input,
                cap_comp_per_mwh_day=cap_comp_input,
                fr_per_mwh_day=fr_input,
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
            spot_s    = [bd[y]["spot"]     for y in years]
            cap_s     = [bd[y]["cap_comp"] for y in years]
            fr_s      = [bd[y]["fr"]       for y in years]
            sof_s     = [bd[y]["sysopfee"] for y in years]  # already negative
            om_s      = [-bd[y]["om"]      for y in years]
            debt_s    = [-bd[y]["debt_svc"] for y in years]
            net_s     = [bd[y]["net"]      for y in years]

            st.subheader(_t("irr_cashflow_title"))
            fig_cf = go.Figure()
            fig_cf.add_bar(x=years, y=spot_s, name=_t("irr_cf_spot"),
                           marker_color="#2ecc71")
            fig_cf.add_bar(x=years, y=cap_s,  name=_t("irr_cf_cap_comp"),
                           marker_color="#1abc9c")
            fig_cf.add_bar(x=years, y=fr_s,   name=_t("irr_cf_fr"),
                           marker_color="#27ae60")
            fig_cf.add_bar(x=years, y=sof_s,  name=_t("irr_cf_sysopfee"),
                           marker_color="#e67e22")
            fig_cf.add_bar(x=years, y=om_s,   name=_t("irr_cf_om"),
                           marker_color="#e74c3c")
            fig_cf.add_bar(x=years, y=debt_s, name=_t("irr_cf_debt"),
                           marker_color="#c0392b")
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
                        capture_rate=irr_cap_rate,
                        duration_h=irr_dur_h,
                        capex_per_kwh=cx,
                        rte=rte_pct / 100.0,
                        om_per_kw_yr=om,
                        degradation=dgrad,
                        equity_pct=equity,
                        loan_rate=lr_pct,
                        loan_tenure=tenure,
                        project_life=life,
                        sysopfee_per_mwh_day=sysopfee_input,
                        cap_comp_per_mwh_day=cap_comp_input,
                        fr_per_mwh_day=fr_input,
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

    # ── DB Coverage ───────────────────────────────────────────────────────────
    st.subheader(_t("mgmt_coverage_title"))
    cov = load_coverage(_ENG_KEY)
    _today_dt = dt.date.today()

    # ── Backfill Progress Grid ─────────────────────────────────────────────────
    _prog = load_scraping_progress(_ENG_KEY)
    if not _prog.empty:
        import calendar as _cal

        # Build pivot tables: display text + numeric pct for coloring
        _months = sorted(_prog["month_start"].unique())
        _month_labels = [pd.Timestamp(m).strftime("%b '%y") for m in _months]

        _prov_order = (
            _prog.groupby("province")["days_present"].sum()
            .sort_values(ascending=False).index.tolist()
        )

        _display_rows = {}
        _pct_rows     = {}
        _latest_rows  = {}
        _total_rows   = {}

        for prov in _prov_order:
            sub = _prog[_prog["province"] == prov].set_index("month_start")
            row_txt, row_pct = {}, {}
            total_present, total_expected = 0, 0
            latest = None
            for m, lbl in zip(_months, _month_labels):
                if m in sub.index:
                    dp = int(sub.loc[m, "days_present"])
                    de = int(sub.loc[m, "expected_days"])
                    ld = sub.loc[m, "latest_date"]
                    if pd.notna(ld) and (latest is None or ld > latest):
                        latest = ld
                else:
                    dp, de = 0, 0
                de = max(de, 1)
                row_txt[lbl] = f"{dp}/{de}"
                row_pct[lbl] = round(dp / de * 100)
                total_present += dp
                total_expected += de
            _display_rows[prov] = row_txt
            _pct_rows[prov]     = row_pct
            _latest_rows[prov]  = str(latest) if latest else "—"
            _total_rows[prov]   = round(total_present / max(total_expected, 1) * 100)

        _disp_df  = pd.DataFrame(_display_rows).T
        _disp_df.index.name = "Province"
        _disp_df = _disp_df[_month_labels]
        _disp_df["Latest"]    = pd.Series(_latest_rows)
        _disp_df["Total %"]   = pd.Series(_total_rows)

        _pct_df = pd.DataFrame(_pct_rows).T[_month_labels]

        def _color_cell(v):
            try:
                pct = int(v)
            except (ValueError, TypeError):
                return ""
            if pct >= 90:
                return "background-color:#d4edda;color:#155724"
            elif pct >= 50:
                return "background-color:#fff3cd;color:#856404"
            elif pct >= 10:
                return "background-color:#fde8d8;color:#9e4a00"
            else:
                return "background-color:#f8d7da;color:#721c24"

        def _style_grid(df):
            # Apply color based on pct_df for month columns only
            styles = pd.DataFrame("", index=df.index, columns=df.columns)
            for lbl in _month_labels:
                if lbl in df.columns:
                    styles[lbl] = _pct_df[lbl].map(_color_cell)
            # Color Total % column
            if "Total %" in df.columns:
                styles["Total %"] = df["Total %"].map(_color_cell)
            return styles

        st.caption("Fundamentals scraping coverage (days filled / days expected). "
                   "🟢 ≥90% · 🟡 50–89% · 🟠 10–49% · 🔴 <10%")
        st.dataframe(
            _disp_df.style.apply(_style_grid, axis=None),
            use_container_width=True,
        )
        st.divider()

    # ── Original detailed coverage table (collapsed) ───────────────────────────
    with st.expander("Full coverage detail", expanded=False):
        if not cov.empty:
            def _status(row):
                if pd.isna(row["last_capture"]):
                    return _t("mgmt_status_missing")
                lag = ((_today_dt - row["last_capture"].date()).days
                       if pd.notna(row["last_capture"]) else 999)
                return _t("mgmt_status_ok") if lag <= 30 else _t("mgmt_status_stale")
            cov["status"] = cov.apply(_status, axis=1)
            gaps      = load_coverage_gaps(_ENG_KEY)
            fund_gaps = load_fundamentals_gaps(_ENG_KEY)
            cov["missing_dates"]      = cov["province"].map(gaps).fillna("")
            cov["missing_fund_dates"] = cov["province"].map(fund_gaps).fillna("")
            cov_display = cov.copy()
            cov_display.columns = [_t("mgmt_col_province"), _t("mgmt_col_last_hourly"),
                                    _t("mgmt_col_last_capture"), _t("mgmt_col_last_fund"),
                                    _t("mgmt_col_status"), _t("mgmt_col_missing_dates"),
                                    _t("mgmt_col_missing_fund_dates")]
            st.dataframe(cov_display, use_container_width=True, hide_index=True)

    # ── Batch Backfill ────────────────────────────────────────────────────────
    st.divider()
    st.subheader(_t("mgmt_batch_title"))
    st.caption(_t("mgmt_batch_caption"))

    # Pre-select stale/missing provinces (last_hourly older than 2 days ago or absent)
    _threshold = _today_dt - dt.timedelta(days=2)
    if not cov.empty:
        _stale_mask = (
            cov["last_hourly"].isna() |
            (cov["last_hourly"].notna() &
             (cov["last_hourly"].dt.date < _threshold))
        )
        _default_backfill = cov.loc[_stale_mask, "province"].tolist()
        _all_provs_cov    = cov["province"].tolist()
    else:
        _default_backfill = []
        _all_provs_cov    = []

    _batch_markets = st.multiselect(
        _t("mgmt_batch_markets"),
        options=_all_provs_cov,
        default=_default_backfill,
        key="batch_markets_sel",
    )
    _bf_col1, _bf_col2 = st.columns(2)
    _batch_start = _bf_col1.date_input(
        _t("mgmt_batch_start"),
        value=_today_dt - dt.timedelta(days=7),
        key="batch_start_date",
    )
    _batch_end = _bf_col2.date_input(
        _t("mgmt_batch_end"),
        value=_today_dt,
        key="batch_end_date",
    )
    _batch_models = st.multiselect(
        _t("model_selector_label"),
        options=list(_MODEL_OPTS.keys()),
        default=list(_MODEL_OPTS.keys()),
        format_func=lambda k: _MODEL_OPTS[k],
        key="batch_models_sel",
    )

    _has_creds = bool(
        os.environ.get("LINGFENG_USERNAME") and os.environ.get("LINGFENG_PASSWORD")
    )
    if not _has_creds:
        st.warning(_t("mgmt_batch_no_creds"))

    if st.button(_t("mgmt_batch_btn"), type="primary",
                 disabled=(not _batch_markets), key="batch_run_btn"):
        _run_daily_script = _REPO / "services" / "lingfeng" / "run_daily.py"
        if not _run_daily_script.exists():
            st.error(f"Script not found: {_run_daily_script}")
        else:
            _batch_cmd = [
                sys.executable, str(_run_daily_script),
                "--markets",    ",".join(_batch_markets),
                "--start-date", str(_batch_start),
                "--end-date",   str(_batch_end),
                "--models",     ",".join(_batch_models),
            ]
            st.caption(f"Running: {' '.join(_batch_cmd)}")
            _batch_log = st.empty()
            _batch_proc = subprocess.Popen(
                _batch_cmd,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, cwd=str(_REPO),
                env={**os.environ, "PYTHONPATH": str(_REPO)},
            )
            _batch_buf = ""
            for _ln in _batch_proc.stdout:
                _batch_buf += _ln
                _batch_log.code(_batch_buf[-8000:])
            _batch_proc.wait()
            if _batch_proc.returncode != 0:
                st.error(f"Batch backfill failed (rc={_batch_proc.returncode})")
            else:
                st.success("Batch backfill complete.")
            load_coverage.clear()
            st.cache_data.clear()

    # ── Manual Data Steps (Advanced) ─────────────────────────────────────────
    st.divider()
    with st.expander(_t("mgmt_advanced_title"), expanded=False):
        if "mgmt_uploaded_names" not in st.session_state:
            st.session_state["mgmt_uploaded_names"] = []
        _local_upload_dir = Path(tempfile.gettempdir()) / "bess_uploads"
        _uploaded_names   = st.session_state["mgmt_uploaded_names"]

        # Upload
        st.subheader(_t("mgmt_upload_title"))
        uploaded = st.file_uploader(
            _t("mgmt_upload_help"), type="xlsx", accept_multiple_files=True,
            key="mgmt_upload",
        )
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
            _uploaded_names = st.session_state["mgmt_uploaded_names"]

        # Ingest uploaded files → DB
        st.divider()
        st.subheader(_t("mgmt_ingest_title"))
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

        # Fundamentals ingest
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

    # ── Data Operations Log ──────────────────────────────────────────────────
    st.divider()
    st.subheader(_t("data_ops_log_title"))

    _ops_df = load_data_ops_log(_ENG_KEY)
    if not _ops_df.empty:
        _disp_cols = [c for c in ["op_name", "market", "date_range", "status", "message",
                                   "started_at", "finished_at", "duration_s"]
                      if c in _ops_df.columns]
        st.dataframe(_ops_df[_disp_cols], use_container_width=True, hide_index=True)
    else:
        st.caption("No operations logged yet.")

# ── Tab 6: Agent ──────────────────────────────────────────────────────────────
with tab_agent:
    _ensure_memory_table()  # deferred: runs once, only when agent tab is visited

    from shared.anthropic_client import make_client as _make_anthropic_client, is_llm_available as _is_llm_available
    import json as _json

    # ── LLM provider selector ─────────────────────────────────────────────────
    _LLM_OPTIONS = {
        "Claude (Anthropic)": ("anthropic", "claude-sonnet-4-6"),
        "GPT-4o (OpenAI)":    ("openai",    "gpt-4o"),
        "DeepSeek":           ("deepseek",  "deepseek-chat"),
    }
    _llm_label = st.radio(
        _t("llm_selector_label"),
        list(_LLM_OPTIONS.keys()),
        horizontal=True,
        key="llm_provider_sel",
    )
    _llm_provider, _llm_model = _LLM_OPTIONS[_llm_label]

    # ── initialise LLM client ─────────────────────────────────────────────────
    # Anthropic client is always available (used for memory extraction with Haiku)
    _ant_client = _make_anthropic_client(os.environ.get("ANTHROPIC_API_KEY", ""))

    if _llm_provider == "anthropic":
        if not _is_llm_available(os.environ.get("ANTHROPIC_API_KEY", "")):
            st.error(_t("agent_no_key"))
            st.stop()
        _chat_client = None  # use _ant_client directly
    elif _llm_provider == "openai":
        _oai_key = os.environ.get("OPENAI_API_KEY")
        if not _oai_key:
            st.error(_t("llm_no_key", provider="OPENAI_API_KEY"))
            st.stop()
        from openai import OpenAI as _OAI
        _chat_client = _OAI(api_key=_oai_key)
    else:  # deepseek
        _ds_key = os.environ.get("DEEPSEEK_API_KEY")
        if not _ds_key:
            st.error(_t("llm_no_key", provider="DEEPSEEK_API_KEY"))
            st.stop()
        from openai import OpenAI as _OAI
        _chat_client = _OAI(api_key=_ds_key, base_url="https://api.deepseek.com")

    # ── session state init ────────────────────────────────────────────────────
    # Clear history when user switches LLM provider (incompatible tool formats)
    if st.session_state.get("_last_llm_provider") != _llm_provider:
        st.session_state["bess_agent_msgs"] = []
        st.session_state["bess_mem_suggestions"] = []
        st.session_state["_last_llm_provider"] = _llm_provider
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
        {
            "name": "get_sysop_fee",
            "description": (
                "Get monthly grid system operation fees (系统运行费, ¥/kWh) by province. "
                "This is a COST charged by the grid on every MWh discharged — reduces actual BESS revenue. "
                "Higher fee = higher grid balancing cost = stronger BESS flexibility demand. "
                "Use to adjust IRR calculations: pass the fee as a negative subsidy_per_mwh in get_irr_estimate."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "province": {"type": "string", "description": "Chinese province name, e.g. '山西'. Omit for all provinces."},
                    "start_ym": {"type": "string", "description": "YYYY-MM filter start (inclusive). Omit for all history."},
                    "end_ym":   {"type": "string", "description": "YYYY-MM filter end (inclusive). Omit for all history."},
                },
                "required": [],
            },
        },
        {
            "name": "get_capacity_compensation",
            "description": (
                "Get province capacity compensation (容量补偿/容量电价) policy data: ¥/kW rate and qualifying duration. "
                "This is a REVENUE stream paid to BESS owners for providing capacity. "
                "Use to add to IRR: annualised ¥/kW ÷ duration_h ÷ 1000 converts to ¥/MWh for subsidy_per_mwh in get_irr_estimate."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "province": {"type": "string", "description": "Chinese province name. Omit for all provinces."},
                },
                "required": [],
            },
        },
        {
            "name": "get_freq_reg_market",
            "description": (
                "Get province frequency regulation (调频辅助服务) market data: price in ¥/kW·h and annual pool size (亿元/年). "
                "This is a REVENUE stream for BESS providing AGC/secondary regulation. "
                "Use together with get_capacity_compensation and get_sysop_fee for a complete IRR picture."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "province": {"type": "string", "description": "Chinese province name. Omit for all provinces."},
                },
                "required": [],
            },
        },
    ]

    # OpenAI-format tools (used for GPT-4o and DeepSeek)
    _OAI_TOOLS = [
        {"type": "function", "function": {
            "name": t["name"],
            "description": t["description"],
            "parameters": t["input_schema"],
        }}
        for t in _TOOLS
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
                                      float(inp.get("duration_h", 4.0)), sel_model)
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

        elif name == "get_sysop_fee":
            df = load_sysopfee(_ENG_KEY)
            if inp.get("province"):
                df = df[df["province"] == inp["province"]]
            if inp.get("start_ym"):
                df = df[df["year_month"] >= inp["start_ym"]]
            if inp.get("end_ym"):
                df = df[df["year_month"] <= inp["end_ym"]]
            if df.empty:
                return "No system operation fee data found for the given filters."
            # Return summary: latest fee per province + average
            summary = (
                df.sort_values("year_month")
                .groupby("province")
                .agg(
                    latest_ym=("year_month", "max"),
                    latest_fee_yuan_kwh=("fee_yuan_kwh", "last"),
                    avg_fee_yuan_kwh=("fee_yuan_kwh", "mean"),
                    months_count=("fee_yuan_kwh", "count"),
                )
                .reset_index()
                .round(4)
            )
            return summary.to_json(orient="records", default_handler=str)

        elif name == "get_capacity_compensation":
            df = load_cap_comp(_ENG_KEY)
            if inp.get("province"):
                df = df[df["province"] == inp["province"]]
            if df.empty:
                return "No capacity compensation data found."
            keep = ["province", "effective_date", "cap_comp_yuan_kw", "peak_duration_hours", "status", "notes"]
            return df[[c for c in keep if c in df.columns]].to_json(orient="records", default_handler=str)

        elif name == "get_freq_reg_market":
            df = load_fr_market(_ENG_KEY)
            if inp.get("province"):
                df = df[df["province"] == inp["province"]]
            if df.empty:
                return "No frequency regulation market data found."
            keep = ["province", "effective_date", "fr_price_yuan_kw_h", "fr_pool_billion_yuan", "status"]
            return df[[c for c in keep if c in df.columns]].to_json(orient="records", default_handler=str)

        return "Unknown tool"

    # ── auto-extract helper ────────────────────────────────────────────────────
    def _extract_memories(user_msg: str, agent_reply: str) -> list[dict]:
        """Ask Haiku to extract saveable facts from this exchange. Returns list of dicts."""
        try:
            extract_resp = _ant_client.messages.create(
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

                if _llm_provider == "anthropic":
                    # ── Anthropic Claude ──────────────────────────────────────
                    while True:
                        resp = _ant_client.messages.create(
                            model=_llm_model,
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

                else:
                    # ── OpenAI-compatible (GPT-4o / DeepSeek) ────────────────
                    _oai_history = [{"role": "system", "content": _sys}] + _history
                    while True:
                        resp = _chat_client.chat.completions.create(
                            model=_llm_model,
                            max_tokens=4096,
                            tools=_OAI_TOOLS,
                            messages=_oai_history,
                        )
                        msg = resp.choices[0].message
                        if msg.tool_calls:
                            _oai_history.append(msg)
                            for tc in msg.tool_calls:
                                with _status:
                                    st.caption(_t("agent_tool_call", tool=tc.function.name))
                                result = _dispatch_tool(
                                    tc.function.name,
                                    _json.loads(tc.function.arguments),
                                )
                                with _status:
                                    st.caption(_t("agent_tool_result", n=len(result)//50))
                                _oai_history.append({
                                    "role": "tool",
                                    "tool_call_id": tc.id,
                                    "content": result,
                                })
                        else:
                            _reply_parts.append(msg.content or "")
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
