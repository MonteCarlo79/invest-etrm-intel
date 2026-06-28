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
import re as _re

import json
import requests

import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.font_manager as _mfm
from matplotlib.patches import Polygon as MplPolygon

# Pick the first CJK-capable font available on this system.
# Linux (Docker) typically has Noto CJK; Windows has YaHei/SimHei.
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

# File-path fallback: scan all system font files for Noto/WQY CJK fonts.
# Handles cases where the font is installed but not yet in the family-name index.
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
        "tab_fundamentals":     "Market Fundamentals",
        "tab_agent":            "Strategist",
        "tab_news":             "📡 News Sources",
        "tab_mgmt":             "Data Management",
        "tab_intraday":         "Intraday Analysis",
        # intraday
        "intraday_shape_title": "Average Hourly Price Shape",
        "intraday_spread_title":"Intraday Spread Ranking (¥/kWh)",
        "intraday_spread_help": "Max − Min of hourly avg prices per province. Higher = more BESS arbitrage potential.",
        "intraday_pdc_title":   "Price Duration Curve",
        "intraday_heat_title":  "Hour × Province Heatmap",
        "intraday_price_type":  "Price type",
        "intraday_select_prov": "Select province for duration curve",
        # intraday fundamentals
        "intraday_fund_title":  "Fundamentals — Market Drivers",
        "intraday_bid_title":   "Bidding Space vs RT Price",
        "intraday_bid_caption": "Each point = one hour. Higher bidding space → lower RT price (supply glut suppresses clearing price).",
        "intraday_wap_title":   "Wind & Solar WAP vs Avg RT Price",
        "intraday_wap_caption": "WAP = Σ(generation_MW × price) / Σ(generation_MW). WAP below avg RT = renewable generation concentrated in low-price hours.",
        "intraday_no_fund":     "No fundamentals data found in DB for the selected period. Use bess-map Data Management → Run Fundamentals Ingest to populate.",
        # market fundamentals
        "fund_provinces":       "Provinces",
        "fund_year":            "Year",
        "fund_capacity_title":  "Installed Capacity by Fuel Type (万kW)",
        "fund_generation_title":"Generation by Fuel Type (亿kWh)",
        "fund_peak_title":      "Peak Load (MW)",
        "fund_summer_peak":     "Summer Peak",
        "fund_winter_peak":     "Winter Peak",
        "fund_other_peak":      "Off-Peak",
        "fund_renewables_share":"Renewables share",
        "fund_lf_title":        "Load Factor by Fuel Type (%)",
        "fund_lf_caption":      "LF = Generation (亿kWh) ÷ Capacity (万kW) ÷ 8760. All provinces ranked.",
        "fund_lf_sort":         "Sort ranking by",
        "fund_tightness_title": "System Tightness",
        "fund_tightness_caption": "Effective capacity = Σ(Installed capacity × standard EOH ÷ 8760). Green = surplus, red = deficit.",
        "fund_eff_cap":         "Eff. Cap (MW)",
        "fund_avg_demand":      "Avg Demand (MW)",
        "fund_tight_avg":       "vs Avg Demand (MW)",
        "fund_tight_summer":    "vs Summer Peak (MW)",
        "fund_tight_winter":    "vs Winter Peak (MW)",
        "fund_no_data":         "No market fundamentals data available. Ensure the Excel file is in data/market-fundamentals/.",
        "fund_total":           "Total",
        "fund_share_label":     "Share of capacity (%)",
        "fund_capacity_unit":   "万kW",
        "fund_generation_unit": "亿kWh",
        "fund_select_prompt":   "Select at least one province.",
        # agent
        "agent_title":          "Spot Market AI Agent",
        "agent_caption":        "Ask questions about prices, trends, inter-provincial flows, or trigger ingestion.",
        "agent_welcome":        "Hi! I can query spot market prices, inter-provincial flows, and daily summaries from the database, and run the ingestion pipeline for new PDFs. What would you like to know?",
        "agent_placeholder":    "e.g. What were Shandong DA prices in April 2026?",
        "agent_thinking":       "Thinking...",
        "agent_tool_call":      "Tool call: {tool}",
        "agent_tool_result":    "Result ({n} rows)",
        "agent_no_key":         "ANTHROPIC_API_KEY is not set. Please add it to your .env file.",
        "agent_clear":          "Clear chat",
        "agent_error":          "Agent error: {err}",
        # knowledge base
        "kb_title":             "Knowledge Base",
        "kb_caption":           "Upload market rules, annual reports, and policy documents for the agent to reference.",
        "kb_upload_label":      "Upload documents (PDF / PPTX / Word / Excel / TXT / Image)",
        "kb_category_label":    "Category override",
        "kb_category_auto":     "Auto-detect",
        "kb_upload_btn":        "Process & Add",
        "kb_success":           "{n} document(s) added to knowledge base.",
        "kb_duplicate":         "{fname} is already in the knowledge base.",
        "kb_failed":            "Failed to process {fname}: {err}",
        "kb_doc_list_title":    "Registered Reference Documents",
        "kb_doc_list_empty":    "No reference documents uploaded yet.",
        "kb_delete":            "Remove",
        "kb_pages":             "{n} pages",
        "kb_chunks":            "chunks indexed",
        # memory
        "mem_saved_ok":         "Saved {n} memory item(s).",
        "mem_confirm_title":    "💡 Suggested memories from this conversation",
        "mem_save_selected":    "Save selected",
        "mem_dismiss":          "Dismiss",
        "mem_manage":           "Memory Management",
        "mem_caption":          "Active memories are injected into every conversation as domain context.",
        "mem_empty":            "No memories stored yet.",
        "mem_delete":           "Delete",
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
        "interprov_price_hi":   "Peak Avg (最高均价)",
        "interprov_price_lo":   "Floor Avg (最低均价)",
        "interprov_prov_leaders":"Province Leaders — Peak Avg Price",
        "hover_province":       "Province",
        "hover_chg":            "Day-on-day",
        "hover_period":         "Active period",
        "hover_share":          "Market share",
        "hover_volume":         "Volume",
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
        "upload_pdf":           "Upload PDF report(s)",
        "upload_help":          "Upload PDFs here when running on AWS (no local data folder). Files are stored in S3 and immediately available for ingestion.",
        "upload_btn":           "Upload {n} file(s) to S3",
        "upload_success":       "Uploaded {n} file(s) to S3.",
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
        # knowledge base sync
        "kb_sync_title":        "Sync Knowledge Base from Market Fundamentals",
        "kb_sync_caption":      "Scans data/market-fundamentals/ and ingests any new documents into the knowledge pool. Already-ingested files are skipped automatically.",
        "kb_sync_unavailable":  "Knowledge base sync is only available in local mode (data/market-fundamentals/ not found).",
        "kb_sync_no_files":     "No new files found — knowledge base is up to date.",
        "kb_sync_btn":          "Sync {n} new file(s)",
        "kb_sync_progress":     "Ingesting {i}/{n}: {fname}…",
        "kb_sync_done":         "Sync complete — added: {added}  skipped: {skipped}  errors: {errors}",
        # data export
        "export_title":         "Download Price Data",
        "export_caption":       "Export DA / RT average prices from the database to Excel (.xlsx). "
                                "Rows = dates, columns = provinces (Chinese names), units = ¥/kWh.",
        "export_btn":           "Download Excel",
        "export_filename":      "电力现货市场日均价格.xlsx",
        # wechat batch ingest
        "wechat_title":         "WeChat Article Batch Import",
        "wechat_caption":       "Paste one WeChat article URL per line. The app fetches each article server-side and ingests the text into the Strategist knowledge pool.",
        "wechat_url_label":     "Article URLs (one per line)",
        "wechat_url_placeholder": "https://mp.weixin.qq.com/s/...\nhttps://mp.weixin.qq.com/s/...",
        "wechat_run_btn":       "Fetch & Ingest {n} article(s)",
        "wechat_no_urls":       "Paste at least one URL above.",
        "wechat_fetching":      "Fetching {i}/{n}: {url}…",
        "wechat_done":          "Done — added: {added}  skipped: {skipped}  errors: {errors}",
        "wechat_digest_btn":    "Digest articles → Insights",
        # daily report
        "report_section_title": "Daily Market Report",
        "report_section_caption": "Scheduled daily PDF report of DA/RT provincial prices with AI commentary. "
                                  "Sent via email at 06:00 SGT and optionally to WeCom groups.",
        "report_schedule_status": "Scheduler status",
        "report_next_run":      "Next run",
        "report_send_now":      "Send Report Now",
        "report_send_success":  "Report sent — {size:,} bytes, date: {rdate}",
        "report_send_wecom":    "WeCom: {result}",
        "report_send_error":    "Report failed: {err}",
        "report_email_label":   "Recipient email(s)",
        "report_email_help":    "Comma-separated. Defaults to REPORT_TO_EMAIL env var.",
        "report_webhook_title": "WeCom Webhook Groups",
        "report_webhook_caption": "Add multiple WeCom bot webhook URLs. Saved to DB and survive restarts.",
        "report_webhook_add_label": "Webhook URL",
        "report_webhook_add_label_label": "Label (optional)",
        "report_webhook_add_btn": "Add Webhook",
        "report_webhook_added": "Webhook added.",
        "report_webhook_empty": "No webhooks configured — WeCom delivery disabled.",
        "report_webhook_delete": "Remove",
        "report_webhook_enabled": "Enabled",
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
        "tab_fundamentals":     "市场基础数据",
        "tab_agent":            "策略分析师",
        "tab_news":             "📡 新闻来源",
        "tab_mgmt":             "数据管理",
        "tab_intraday":         "日内价格分析",
        # intraday
        "intraday_shape_title": "平均小时价格曲线",
        "intraday_spread_title":"日内价差排名 (元/千瓦时)",
        "intraday_spread_help": "各省小时均价最大值减最小值。越高代表储能套利潜力越大。",
        "intraday_pdc_title":   "价格持续时间曲线",
        "intraday_heat_title":  "小时×省份热力图",
        "intraday_price_type":  "价格类型",
        "intraday_select_prov": "选择省份（持续时间曲线）",
        # intraday fundamentals
        "intraday_fund_title":  "基本面——市场驱动因素",
        "intraday_bid_title":   "竞价空间 vs 实时价格",
        "intraday_bid_caption": "每点代表一小时。竞价空间越大，实时价格越低（供给宽松压低出清价格）。",
        "intraday_wap_title":   "风光加权平均价 vs 均价",
        "intraday_wap_caption": "加权均价 = Σ(发电量×价格) / Σ(发电量)。加权均价低于均价说明新能源发电集中于低价时段。",
        "intraday_no_fund":     "所选时段数据库中暂无基本面数据，请在储能地图→数据管理→运行基本面导入后再查看。",
        # market fundamentals
        "fund_provinces":       "省份",
        "fund_year":            "年份",
        "fund_capacity_title":  "各燃料类型装机容量（万千瓦）",
        "fund_generation_title":"各燃料类型发电量（亿千瓦时）",
        "fund_peak_title":      "最大负荷（兆瓦）",
        "fund_summer_peak":     "度夏峰值",
        "fund_winter_peak":     "度冬峰值",
        "fund_other_peak":      "其余月份",
        "fund_renewables_share":"可再生能源占比",
        "fund_lf_title":        "各类型负荷率（%）",
        "fund_lf_caption":      "负荷率 = 年发电量（亿千瓦时）÷ 装机容量（万千瓦）÷ 8760。显示全部省份排名。",
        "fund_lf_sort":         "排序依据",
        "fund_tightness_title": "电力系统紧张程度",
        "fund_tightness_caption": "有效容量 = Σ（装机容量 × 标准利用小时 ÷ 8760）。绿色 = 盈余，红色 = 缺口。",
        "fund_eff_cap":         "有效容量（兆瓦）",
        "fund_avg_demand":      "平均负荷（兆瓦）",
        "fund_tight_avg":       "对均值盈余（兆瓦）",
        "fund_tight_summer":    "对度夏盈余（兆瓦）",
        "fund_tight_winter":    "对度冬盈余（兆瓦）",
        "fund_no_data":         "暂无市场基础数据。请确保Excel文件位于 data/market-fundamentals/ 目录下。",
        "fund_total":           "合计",
        "fund_share_label":     "装机占比（%）",
        "fund_capacity_unit":   "万千瓦",
        "fund_generation_unit": "亿千瓦时",
        "fund_select_prompt":   "请至少选择一个省份。",
        # agent
        "agent_title":          "现货市场策略分析师",
        "agent_caption":        "查询价格、走势、省间交易数据，或触发PDF导入流程。",
        "agent_welcome":        "您好！我可以查询现货市场价格、省间交易数据及每日市场摘要，也可以为新PDF运行导入流程。请问您想了解什么？",
        "agent_placeholder":    "例如：2026年4月山东日前价格是多少？",
        "agent_thinking":       "思考中…",
        "agent_tool_call":      "工具调用：{tool}",
        "agent_tool_result":    "结果（{n} 行）",
        "agent_no_key":         "未设置 ANTHROPIC_API_KEY，请在 .env 文件中添加。",
        "agent_clear":          "清除对话",
        "agent_error":          "助手出错：{err}",
        # knowledge base
        "kb_title":             "知识库",
        "kb_caption":           "上传交易规则、年度报告、政策文件等，供智能助手参考使用。",
        "kb_upload_label":      "上传文档（PDF / PPTX / Word / Excel / TXT / 图片）",
        "kb_category_label":    "手动指定类别",
        "kb_category_auto":     "自动识别",
        "kb_upload_btn":        "处理并添加",
        "kb_success":           "已添加 {n} 个文档至知识库。",
        "kb_duplicate":         "{fname} 已存在于知识库中。",
        "kb_failed":            "处理 {fname} 失败：{err}",
        "kb_doc_list_title":    "已注册的参考文档",
        "kb_doc_list_empty":    "暂无参考文档，请上传。",
        "kb_delete":            "删除",
        "kb_pages":             "{n} 页",
        "kb_chunks":            "个片段已索引",
        # memory
        "mem_saved_ok":         "已保存 {n} 条记忆。",
        "mem_confirm_title":    "💡 本次对话中发现的记忆建议",
        "mem_save_selected":    "保存所选",
        "mem_dismiss":          "忽略",
        "mem_manage":           "记忆管理",
        "mem_caption":          "已激活的记忆将在每次对话开始时注入系统提示。",
        "mem_empty":            "暂无已保存的记忆。",
        "mem_delete":           "删除",
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
        "interprov_price_hi":   "最高均价",
        "interprov_price_lo":   "最低均价",
        "interprov_prov_leaders":"每日最高均价省份",
        "hover_province":       "省份",
        "hover_chg":            "日环比",
        "hover_period":         "活跃时段",
        "hover_share":          "市场占比",
        "hover_volume":         "电量",
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
        "upload_pdf":           "上传PDF报告",
        "upload_help":          "在AWS环境下（无本地数据文件夹时）上传PDF。文件保存至S3后即可导入。",
        "upload_btn":           "上传 {n} 个文件至S3",
        "upload_success":       "已上传 {n} 个文件至S3。",
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
        # knowledge base sync
        "kb_sync_title":        "从市场基础数据同步知识库",
        "kb_sync_caption":      "扫描 data/market-fundamentals/ 文件夹，将新文档录入知识库。已录入的文件自动跳过。",
        "kb_sync_unavailable":  "知识库同步仅在本地模式下可用（未找到 data/market-fundamentals/ 文件夹）。",
        "kb_sync_no_files":     "未发现新文件——知识库已是最新。",
        "kb_sync_btn":          "同步 {n} 个新文件",
        "kb_sync_progress":     "正在录入 {i}/{n}：{fname}…",
        "kb_sync_done":         "同步完成——新增：{added}  跳过：{skipped}  错误：{errors}",
        # data export
        "export_title":         "下载价格数据",
        "export_caption":       "将数据库中的日前/实时均价导出为 Excel (.xlsx)。行=日期，列=省份（中文），单位=元/千瓦时。",
        "export_btn":           "下载 Excel",
        "export_filename":      "电力现货市场日均价格.xlsx",
        # wechat batch ingest
        "wechat_title":         "微信文章批量导入",
        "wechat_caption":       "每行粘贴一个微信公众号文章链接，系统将自动抓取正文并录入策略师知识库。",
        "wechat_url_label":     "文章链接（每行一个）",
        "wechat_url_placeholder": "https://mp.weixin.qq.com/s/...\nhttps://mp.weixin.qq.com/s/...",
        "wechat_run_btn":       "抓取并录入 {n} 篇文章",
        "wechat_no_urls":       "请在上方粘贴至少一个链接。",
        "wechat_fetching":      "正在处理 {i}/{n}：{url}…",
        "wechat_done":          "完成——新增：{added}  跳过：{skipped}  错误：{errors}",
        "wechat_digest_btn":    "摘要文章 → 洞察",
        # daily report
        "report_section_title": "每日市场报告",
        "report_section_caption": "每日自动生成含AI分析的日前/实时省份价格PDF报告，新加坡时间06:00发送邮件，并可推送至企业微信群。",
        "report_schedule_status": "调度状态",
        "report_next_run":      "下次运行",
        "report_send_now":      "立即发送报告",
        "report_send_success":  "报告已发送 — {size:,} 字节，日期：{rdate}",
        "report_send_wecom":    "企业微信：{result}",
        "report_send_error":    "报告发送失败：{err}",
        "report_email_label":   "收件人邮箱",
        "report_email_help":    "多个地址用逗号分隔。默认使用 REPORT_TO_EMAIL 环境变量。",
        "report_webhook_title": "企业微信群机器人",
        "report_webhook_caption": "配置多个企业微信群机器人 Webhook，保存至数据库（重启后不丢失）。",
        "report_webhook_add_label": "Webhook 链接",
        "report_webhook_add_label_label": "标签（可选）",
        "report_webhook_add_btn": "添加",
        "report_webhook_added": "Webhook 已添加。",
        "report_webhook_empty": "暂无 Webhook，企业微信推送已禁用。",
        "report_webhook_delete": "删除",
        "report_webhook_enabled": "启用",
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


# ── Process-level caches for knowledge-pool calls that open fresh connections ──
# knowledge_pool/db.py's get_conn() opens a NEW TCP connection on every call.
# These @st.cache_data wrappers limit that to once per 5 minutes per process,
# regardless of how many Streamlit sessions are active.  Without this, each new
# browser session triggered 2-3 fresh DB connections before tab_mgmt could render.
@st.cache_data(ttl=300, show_spinner=False)
def _cached_memory_stats() -> dict:
    try:
        from services.knowledge_pool.expert_memory import get_memory_stats as _gms
        return _gms()
    except Exception:
        return {}


@st.cache_data(ttl=300, show_spinner=False)
def _cached_kb_docs() -> list:
    try:
        from services.knowledge_pool.knowledge_docs import list_knowledge_docs as _lkd
        return _lkd()
    except Exception:
        return []


@st.cache_data(ttl=300, show_spinner=False)
def _spot_kb_stats() -> dict:
    """Return spot market KB + insight statistics for the metrics dashboard."""
    docs_row = _query(
        "SELECT COUNT(*)::int AS total, "
        "COUNT(*) FILTER (WHERE parse_error IS NULL AND active = TRUE)::int AS parsed, "
        "COUNT(*) FILTER (WHERE active = TRUE)::int AS active_docs "
        "FROM staging.spot_knowledge_docs"
    )
    chunks_row = _query("SELECT COUNT(*)::int AS n FROM staging.spot_knowledge_chunks")
    insights_row = _query(
        "SELECT COUNT(*)::int AS n FROM staging.kp_expert_insights WHERE active = TRUE"
    )
    total   = int(docs_row.iloc[0]["total"])       if not docs_row.empty     else 0
    parsed  = int(docs_row.iloc[0]["parsed"])      if not docs_row.empty     else 0
    active  = int(docs_row.iloc[0]["active_docs"]) if not docs_row.empty     else 0
    n_chunks    = int(chunks_row.iloc[0]["n"])     if not chunks_row.empty   else 0
    n_insights  = int(insights_row.iloc[0]["n"])   if not insights_row.empty else 0
    return {
        "total":       total,
        "parsed":      parsed,
        "active":      active,
        "parse_pct":   parsed / total if total > 0 else 0.0,
        "n_chunks":    n_chunks,
        "n_insights":  n_insights,
    }


# ── APScheduler — daily spot market report ────────────────────────────────────
import os as _os
_DEFAULT_RECIPIENT = "chen_dpeng@hotmail.com"

@st.cache_resource
def _start_spot_scheduler():
    """Start background scheduler for daily spot market report (runs once per process)."""
    try:
        from apscheduler.schedulers.background import BackgroundScheduler
    except ImportError:
        return None

    def _daily_spot_report_job():
        """06:00 SGT — generate PDF, email, WeCom."""
        try:
            import importlib.util, pathlib as _pl
            _spec = importlib.util.spec_from_file_location(
                "spot_report",
                _pl.Path(__file__).parent / "spot_report.py",
            )
            _mod = importlib.util.module_from_spec(_spec)
            _spec.loader.exec_module(_mod)
            result = _mod.run_daily_report()
            import logging as _logging
            _logging.getLogger(__name__).info(
                "Scheduled spot report: %s", result
            )
        except Exception as _exc:
            import logging as _logging
            _logging.getLogger(__name__).error(
                "Scheduled spot report failed: %s", _exc, exc_info=True
            )

    scheduler = BackgroundScheduler(timezone="Asia/Singapore")
    scheduler.add_job(
        _daily_spot_report_job,
        "cron", hour=6, minute=0,
        id="spot_daily_report",
        misfire_grace_time=3600,
    )
    scheduler.start()
    return scheduler


_start_spot_scheduler()  # no-op after first call (cache_resource)


@st.cache_resource
def _get_spot_report_mod():
    """Load spot_report module once per process (exec_module is expensive — loads ReportLab etc.)."""
    import importlib.util as _ilu, pathlib as _pl2
    _spec = _ilu.spec_from_file_location(
        "spot_report",
        _pl2.Path(__file__).parent / "spot_report.py",
    )
    _mod = _ilu.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    try:
        _mod.ensure_webhook_table()   # DDL once per process
    except Exception:
        pass
    return _mod


@st.cache_data(ttl=300, show_spinner=False)
def _cached_webhooks() -> list:
    """Cached webhook list — avoids a fresh DB connection on every rerun."""
    try:
        return _get_spot_report_mod().list_webhooks()
    except Exception:
        return []


# ── intraday hourly price loaders ─────────────────────────────────────────────
@st.cache_data(ttl=3600)
def _load_intraday_shape(_conn_fn, provinces: tuple, start: str, end: str, price_col: str):
    sql = f"""
        SELECT province,
               EXTRACT(hour FROM datetime)::int AS hour,
               AVG({price_col}) AS avg_price
        FROM marketdata.spot_prices_hourly
        WHERE province = ANY(%s) AND datetime BETWEEN %s AND %s
        GROUP BY province, hour ORDER BY province, hour
    """
    return pd.read_sql(sql, _conn_fn(), params=[list(provinces), start, end])

@st.cache_data(ttl=3600)
def _load_intraday_spread(_conn_fn, start: str, end: str, price_col: str):
    sql = f"""
        SELECT province, MAX(avg_price) - MIN(avg_price) AS spread
        FROM (
            SELECT province, EXTRACT(hour FROM datetime)::int AS hour,
                   AVG({price_col}) AS avg_price
            FROM marketdata.spot_prices_hourly
            WHERE datetime BETWEEN %s AND %s
            GROUP BY province, hour
        ) t GROUP BY province ORDER BY spread DESC
    """
    return pd.read_sql(sql, _conn_fn(), params=[start, end])

@st.cache_data(ttl=3600)
def _load_hourly_series(_conn_fn, province: str, start: str, end: str, price_col: str):
    sql = f"""
        SELECT {price_col} AS price FROM marketdata.spot_prices_hourly
        WHERE province = %s AND datetime BETWEEN %s AND %s ORDER BY datetime
    """
    return pd.read_sql(sql, _conn_fn(), params=[province, start, end])["price"].dropna()


@st.cache_data(ttl=3600)
def _load_bidding_vs_price(_conn_fn, provinces: tuple, start: str, end: str):
    """Hourly bidding space (RT outturn) vs RT price — for scatter/OLS."""
    sql = """
        SELECT f.province,
               f.bidding_space_mw,
               p.rt_price
        FROM marketdata.spot_fundamentals_hourly f
        JOIN marketdata.spot_prices_hourly p
          ON p.province = f.province AND p.datetime = f.datetime
        WHERE f.province = ANY(%s)
          AND f.datetime BETWEEN %s AND %s
          AND f.bidding_space_mw IS NOT NULL
          AND p.rt_price IS NOT NULL
        ORDER BY f.province, f.datetime
    """
    return pd.read_sql(sql, _conn_fn(), params=[list(provinces), start, end])


@st.cache_data(ttl=3600)
def _load_wap(_conn_fn, provinces: tuple, start: str, end: str):
    """Wind and solar weighted average price vs avg RT price, by province."""
    sql = """
        SELECT f.province,
               SUM(f.wind_mw  * p.rt_price) / NULLIF(SUM(f.wind_mw),  0) AS wind_wap,
               SUM(f.solar_mw * p.rt_price) / NULLIF(SUM(f.solar_mw), 0) AS solar_wap,
               AVG(p.rt_price)                                             AS avg_rt_price
        FROM marketdata.spot_fundamentals_hourly f
        JOIN marketdata.spot_prices_hourly p
          ON p.province = f.province AND p.datetime = f.datetime
        WHERE f.province = ANY(%s)
          AND f.datetime BETWEEN %s AND %s
          AND (f.wind_mw IS NOT NULL OR f.solar_mw IS NOT NULL)
        GROUP BY f.province
        ORDER BY f.province
    """
    return pd.read_sql(sql, _conn_fn(), params=[list(provinces), start, end])


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
def load_interprov(start: date, end: date) -> tuple[pd.DataFrame, str]:
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
        return df, ""
    except Exception as e:
        return pd.DataFrame(), str(e)


@st.cache_data(ttl=60, show_spinner=False)
def load_summaries(start: date, end: date) -> tuple[pd.DataFrame, str]:
    try:
        cur = _conn().cursor()
        cur.execute("""
            SELECT report_date::date, summary_text, model, source_pdf
            FROM staging.spot_report_summaries
            WHERE report_date BETWEEN %s AND %s
            ORDER BY report_date DESC
        """, (start, end))
        cols = [d[0] for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols), ""
    except Exception as e:
        return pd.DataFrame(), str(e)


@st.cache_data(ttl=86400, show_spinner=False)
def _translate_to_zh(text: str) -> str:
    """Translate an English market summary to Chinese. Cached per text (one API call per unique summary)."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        return text
    try:
        import anthropic as _ant_tr
        msg = _ant_tr.Anthropic(api_key=api_key).messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            messages=[{
                "role": "user",
                "content": (
                    "Translate the following China electricity market daily summary to "
                    "Chinese (简体中文). Keep all numbers, units (¥/kWh, MW, GWh, 亿kWh), "
                    "and province names unchanged. Output only the translated text.\n\n"
                    + text
                ),
            }],
        )
        return msg.content[0].text
    except Exception:
        return text


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


def chart_heatmap(df_sel: pd.DataFrame, metric: str) -> go.Figure:
    """Province × Date daily average price heatmap from spot_daily.

    df_sel: DataFrame with columns province_en, report_date, da_avg, rt_avg.
    """
    if df_sel.empty:
        return go.Figure()
    val_col = "da_avg" if metric == "da" else "rt_avg"
    pivot = df_sel.pivot_table(index="province_en", columns="report_date", values=val_col)
    if pivot.empty:
        return go.Figure()

    title_key = "da_heatmap_title" if metric == "da" else "rt_heatmap_title"
    _vals = pivot.values.flatten()
    _vals = _vals[~np.isnan(_vals)]
    zmin = float(np.percentile(_vals, 2)) if len(_vals) else None
    zmax = float(np.percentile(_vals, 98)) if len(_vals) else None

    x_labels = [d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else str(d) for d in pivot.columns]
    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=x_labels,
        y=pivot.index.tolist(),
        colorscale="RdYlGn_r",
        zmin=zmin,
        zmax=zmax,
        colorbar=dict(title=_t("price_unit"), thickness=12),
        hoverongaps=False,
        hovertemplate="Date: %{x}<br>Province: %{y}<br>Avg Price: %{z:.4f} ¥/kWh<extra></extra>",
    ))
    fig.update_layout(
        height=max(350, len(pivot) * 28),
        title=dict(text=_t(title_key), font=dict(size=13)),
        margin=dict(l=120, r=20, t=45, b=60),
        xaxis=dict(tickfont=dict(size=9), tickangle=-45),
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
    _fallback_provs = [p for p in ["Shandong", "Shanxi", "Mengxi", "Guangdong", "Sichuan"]
                       if p in prov_options] or prov_options[:5]
    # Restore saved selection from URL query params (survives browser refresh)
    _qp_raw = st.query_params.get("provs", "")
    _qp_saved = [p for p in _qp_raw.split(",") if p in prov_options] if _qp_raw else []
    default_provs = _qp_saved if _qp_saved else _fallback_provs
    selected_provs = st.multiselect(
        _t("provinces"),
        prov_options,
        default=default_provs,
        help="Select one or more provinces to compare",
    )
    # Persist selection in URL so it survives page reload
    _provs_qp = ",".join(selected_provs)
    if st.query_params.get("provs", "") != _provs_qp:
        st.query_params["provs"] = _provs_qp

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
tab_overview, tab_spread, tab_heatmap, tab_intraday, tab_province, tab_dist, tab_geo, \
tab_interprov, tab_fundamentals, tab_agent, tab_news, tab_library, tab_mgmt = st.tabs([
    _t("tab_overview"), _t("tab_spread"), _t("tab_heatmap"), _t("tab_intraday"),
    _t("tab_province"), _t("tab_dist"), _t("tab_geo"),
    _t("tab_interprov"), _t("tab_fundamentals"), _t("tab_agent"), _t("tab_news"), "Library", _t("tab_mgmt"),
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
    fig_hm = chart_heatmap(df_sel, hm_metric.lower())
    if fig_hm.data:
        st.plotly_chart(fig_hm, use_container_width=True)
    else:
        st.info("No data for selected range / provinces.")

# ── Tab 4: Intraday Analysis ──────────────────────────────────────────────────
with tab_intraday:
    pt_sel = st.radio(_t("intraday_price_type"), ["RT", "DA"], horizontal=True, key="intraday_pt")
    price_col = "rt_price" if pt_sel == "RT" else "da_price"

    h_start = str(df_sel["report_date"].min()) if not df_sel.empty else "2025-01-01"
    h_end   = str(df_sel["report_date"].max()) if not df_sel.empty else "2026-01-31"

    # spot_prices_hourly stores Chinese province names; selected_provs are English.
    # Build a mapping from df (which has both columns) and translate before querying.
    _en_to_zh = (
        df[["province_en", "province_cn"]]
        .drop_duplicates()
        .set_index("province_en")["province_cn"]
        .to_dict()
    ) if not df.empty else {}
    h_provs_zh = tuple(sorted(
        _en_to_zh[p] for p in selected_provs if p in _en_to_zh
    )) if selected_provs else ()

    if not h_provs_zh:
        st.info(_t("select_prov_info"))
    else:
        # 1) Intraday shape
        st.subheader(_t("intraday_shape_title"))
        shape_df = _load_intraday_shape(_conn, h_provs_zh, h_start, h_end, price_col)
        if not shape_df.empty:
            fig_intra = px.line(
                shape_df, x="hour", y="avg_price", color="province",
                labels={"hour": "Hour of day", "avg_price": "Avg price (¥/kWh)",
                        "province": _t("col_province")},
            )
            fig_intra.update_layout(height=340, margin=dict(t=20, b=20))
            st.plotly_chart(fig_intra, use_container_width=True)

        # 2) Intraday spread ranking (all provinces)
        st.subheader(_t("intraday_spread_title"))
        st.caption(_t("intraday_spread_help"))
        spread_df = _load_intraday_spread(_conn, h_start, h_end, price_col)
        if not spread_df.empty:
            fig_sp = px.bar(
                spread_df, x="spread", y="province", orientation="h",
                color="spread", color_continuous_scale="RdYlGn_r",
                labels={"spread": "Intraday spread (¥/kWh)", "province": ""},
            )
            fig_sp.update_layout(
                height=max(300, len(spread_df) * 22),
                margin=dict(t=10, b=10),
                showlegend=False, coloraxis_showscale=False,
                yaxis=dict(autorange="reversed"),
            )
            st.plotly_chart(fig_sp, use_container_width=True)

        # 3) Price Duration Curve + Hour×Province heatmap
        col_pdc, col_heat = st.columns(2)
        with col_pdc:
            st.subheader(_t("intraday_pdc_title"))
            pdc_prov = st.selectbox(_t("intraday_select_prov"), sorted(h_provs_zh), key="intraday_pdc")
            pdc_s = _load_hourly_series(_conn, pdc_prov, h_start, h_end, price_col)
            if not pdc_s.empty:
                sorted_p = pdc_s.sort_values(ascending=False).reset_index(drop=True)
                pct = pd.Series(range(len(sorted_p))) / len(sorted_p) * 100
                fig_pdc = px.line(
                    x=pct, y=sorted_p.values,
                    labels={"x": "Percentile (%)", "y": "Price (¥/kWh)"},
                )
                fig_pdc.update_layout(height=300, margin=dict(t=10, b=10))
                st.plotly_chart(fig_pdc, use_container_width=True)
        with col_heat:
            st.subheader(_t("intraday_heat_title"))
            if not shape_df.empty:
                pivot = shape_df.pivot_table(
                    index="hour", columns="province", values="avg_price", aggfunc="mean"
                )
                fig_heat = px.imshow(
                    pivot.T, aspect="auto", color_continuous_scale="RdYlGn_r",
                    labels=dict(x="Hour", y="", color="¥/kWh"),
                )
                fig_heat.update_layout(height=300, margin=dict(t=10, b=10))
                st.plotly_chart(fig_heat, use_container_width=True)

        # 4) Fundamentals analysis (bidding space + WAP)
        st.divider()
        st.subheader(_t("intraday_fund_title"))
        bid_df = _load_bidding_vs_price(_conn, h_provs_zh, h_start, h_end)

        if bid_df.empty:
            st.info(_t("intraday_no_fund"))
        else:
            # Bidding space vs RT price scatter + OLS trendline
            st.subheader(_t("intraday_bid_title"))
            st.caption(_t("intraday_bid_caption"))
            try:
                fig_bid = px.scatter(
                    bid_df, x="bidding_space_mw", y="rt_price", color="province",
                    opacity=0.35, trendline="ols",
                    labels={"bidding_space_mw": "Bidding Space (MW)",
                            "rt_price": "RT Price (¥/kWh)",
                            "province": _t("col_province")},
                )
            except Exception:
                # statsmodels not installed — fall back to scatter without trendline
                fig_bid = px.scatter(
                    bid_df, x="bidding_space_mw", y="rt_price", color="province",
                    opacity=0.35,
                    labels={"bidding_space_mw": "Bidding Space (MW)",
                            "rt_price": "RT Price (¥/kWh)",
                            "province": _t("col_province")},
                )
            fig_bid.update_layout(height=400, margin=dict(t=20, b=20))
            st.plotly_chart(fig_bid, use_container_width=True)

            # Wind & Solar WAP vs avg RT price
            wap_df = _load_wap(_conn, h_provs_zh, h_start, h_end)
            if not wap_df.empty:
                st.subheader(_t("intraday_wap_title"))
                st.caption(_t("intraday_wap_caption"))
                wap_long = wap_df.melt(
                    id_vars=["province", "avg_rt_price"],
                    value_vars=["wind_wap", "solar_wap"],
                    var_name="type", value_name="wap",
                )
                wap_long["type"] = wap_long["type"].map(
                    {"wind_wap": "Wind WAP", "solar_wap": "Solar WAP"}
                )
                fig_wap = px.bar(
                    wap_long, x="province", y="wap", color="type", barmode="group",
                    labels={"wap": "WAP (¥/kWh)", "province": "", "type": ""},
                    color_discrete_map={"Wind WAP": "#4C9BE8", "Solar WAP": "#F5A623"},
                )
                fig_wap.add_scatter(
                    x=wap_df["province"], y=wap_df["avg_rt_price"],
                    mode="markers",
                    marker=dict(symbol="diamond", size=10, color="black"),
                    name="Avg RT price",
                )
                fig_wap.update_layout(height=360, margin=dict(t=20, b=20))
                st.plotly_chart(fig_wap, use_container_width=True)

# ── Tab 5: Province Deep-Dive ────────────────────────────────────────────────
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
        df_summ, _summ_err = load_summaries(d_start, d_end)
        if _summ_err:
            st.warning(f"summaries query failed: {_summ_err}")
        # Filter to dates where this province has data
        prov_dates = set(sub["report_date"].astype(str))
        if df_summ.empty or "report_date" not in df_summ.columns:
            df_summ_prov = pd.DataFrame()
        else:
            df_summ_prov = df_summ[df_summ["report_date"].astype(str).isin(prov_dates)]
        if df_summ_prov.empty:
            st.info(_t("summaries_no_data"))
        else:
            _is_zh = st.session_state.get("lang_radio") == "中文"
            # Translations stored in session state so they survive rerenders
            # without triggering new API calls on every interaction.
            if "translated_summaries" not in st.session_state:
                st.session_state["translated_summaries"] = {}

            for _, row in df_summ_prov.iterrows():
                _rkey = str(row["report_date"])
                with st.expander(_t("summary_label", date=_rkey)):
                    if _is_zh and _rkey in st.session_state["translated_summaries"]:
                        st.markdown(st.session_state["translated_summaries"][_rkey])
                    else:
                        st.markdown(row["summary_text"] or "")
                        if _is_zh:
                            if st.button("🌐 翻译", key=f"tr_{_rkey}"):
                                with st.spinner("翻译中…"):
                                    _raw = row["summary_text"] or ""
                                    st.session_state["translated_summaries"][_rkey] = (
                                        _translate_to_zh(_raw) if _raw else ""
                                    )
                                st.rerun()
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
            # If this rerun was NOT triggered by the animation loop itself,
            # stop the animation. This prevents the loop from running
            # indefinitely in the background when the user navigates away
            # or interacts with any other widget.
            if not st.session_state.pop("_anim_loop_rerun", False):
                st.session_state["anim_playing"] = False
            # Clamp index in case period changed
            st.session_state["anim_frame_idx"] = (
                st.session_state["anim_frame_idx"] % _n_frames
            )

            # Controls row
            ctrl_c1, ctrl_c2, ctrl_c3 = st.columns([1, 1, 3])
            with ctrl_c1:
                if st.button(_t("anim_play"), key="anim_play_btn"):
                    st.session_state["anim_playing"] = True
                    st.session_state["_anim_loop_rerun"] = True
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

            # Auto-advance: sleep then advance index and rerun.
            # Set _anim_loop_rerun so the next rerun keeps the animation alive.
            if st.session_state["anim_playing"]:
                time.sleep(anim_speed)
                _next_idx = (_cur_idx + 1) % _n_frames
                st.session_state["anim_frame_idx"] = _next_idx
                st.session_state["_anim_loop_rerun"] = True
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

    df_ip, _ip_err = load_interprov(d_start, d_end)

    if _ip_err:
        st.error(f"Query failed: {_ip_err}")
    elif df_ip.empty:
        st.info(_t("interprov_no_data"))
    else:
        _dir_export = "送端"
        _dir_import = "受端"

        # ── Clearing price chart: 最高均价 (solid) + 最低均价 (dashed) ──────────
        _hi_rows  = df_ip[df_ip["metric_type"] == "最高均价"].copy()
        _lo_rows  = df_ip[df_ip["metric_type"] == "最低均价"].copy()
        _has_price = not _hi_rows.empty or not _lo_rows.empty

        if _has_price:
            fig_ip_price = go.Figure()
            _dir_colors = {_dir_export: "#1f77b4", _dir_import: "#ff7f0e"}

            for _dir, _dlabel, _dshort in [
                (_dir_export, _t("direction_export"), "送端"),
                (_dir_import, _t("direction_import"), "受端"),
            ]:
                _clr = _dir_colors[_dir]
                # ── Peak avg (solid) ────────────────────────────────────────
                _sh = _hi_rows[_hi_rows["direction"] == _dir].sort_values("report_date")
                if not _sh.empty:
                    _cd = _sh[["province_cn", "price_chg_pct", "time_period"]].values
                    fig_ip_price.add_trace(go.Scatter(
                        x=_sh["report_date"], y=_sh["price_yuan_kwh"],
                        mode="lines+markers", name=f"{_dlabel} — {_t('interprov_price_hi')}",
                        line=dict(color=_clr, width=2),
                        marker=dict(size=6),
                        customdata=_cd,
                        hovertemplate=(
                            "<b>%{x}</b><br>"
                            f"{_t('hover_province')}: %{{customdata[0]}}<br>"
                            f"{_t('interprov_price_hi')}: %{{y:.4f}} ¥/kWh<br>"
                            f"{_t('hover_chg')}: %{{customdata[1]:.2f}}%<br>"
                            f"{_t('hover_period')}: %{{customdata[2]}}"
                            "<extra></extra>"
                        ),
                    ))
                # ── Floor avg (dashed) ──────────────────────────────────────
                _sl = _lo_rows[_lo_rows["direction"] == _dir].sort_values("report_date")
                if not _sl.empty:
                    _cd2 = _sl[["province_cn", "price_chg_pct", "time_period"]].values
                    fig_ip_price.add_trace(go.Scatter(
                        x=_sl["report_date"], y=_sl["price_yuan_kwh"],
                        mode="lines+markers", name=f"{_dlabel} — {_t('interprov_price_lo')}",
                        line=dict(color=_clr, width=2, dash="dash"),
                        marker=dict(size=5, symbol="diamond"),
                        customdata=_cd2,
                        hovertemplate=(
                            "<b>%{x}</b><br>"
                            f"{_t('hover_province')}: %{{customdata[0]}}<br>"
                            f"{_t('interprov_price_lo')}: %{{y:.4f}} ¥/kWh<br>"
                            f"{_t('hover_chg')}: %{{customdata[1]:.2f}}%<br>"
                            f"{_t('hover_period')}: %{{customdata[2]}}"
                            "<extra></extra>"
                        ),
                    ))

            fig_ip_price.update_layout(
                title=dict(text=_t("interprov_price_trend"), x=0, xanchor="left"),
                xaxis_title="", yaxis_title="¥/kWh",
                height=380,
                margin=dict(l=50, r=20, t=55, b=110),
                legend=dict(
                    orientation="h", xanchor="center", x=0.5,
                    yanchor="top", y=-0.18,
                    font=dict(size=11),
                ),
            )
            st.plotly_chart(fig_ip_price, use_container_width=True)

        # ── Province leaders: which province had the peak avg price each day ─
        if not _hi_rows.empty:
            st.subheader(_t("interprov_prov_leaders"))
            _c1, _c2 = st.columns(2)
            for _col_widget, _dir, _dlabel in [
                (_c1, _dir_export, _t("direction_export")),
                (_c2, _dir_import, _t("direction_import")),
            ]:
                _spl = _hi_rows[_hi_rows["direction"] == _dir].sort_values("report_date")
                if _spl.empty:
                    _col_widget.info(_dlabel + ": —")
                    continue
                _cd_pl = _spl[["price_yuan_kwh", "province_share", "time_period"]].values
                fig_pl = go.Figure(go.Scatter(
                    x=_spl["report_date"],
                    y=_spl["province_cn"],
                    mode="markers",
                    marker=dict(
                        size=10,
                        color=_spl["price_yuan_kwh"],
                        colorscale="RdYlGn_r",
                        showscale=True,
                        colorbar=dict(title="¥/kWh", thickness=10, len=0.8),
                    ),
                    customdata=_cd_pl,
                    hovertemplate=(
                        "<b>%{x}</b> · %{y}<br>"
                        f"{_t('interprov_price_hi')}: %{{customdata[0]:.4f}} ¥/kWh<br>"
                        f"{_t('hover_share')}: %{{customdata[1]:.1f}}%<br>"
                        f"{_t('hover_period')}: %{{customdata[2]}}"
                        "<extra></extra>"
                    ),
                    showlegend=False,
                ))
                fig_pl.update_layout(
                    title=dict(text=_dlabel, x=0, xanchor="left"),
                    xaxis_title="", yaxis_title="",
                    height=300,
                    margin=dict(l=90, r=60, t=45, b=30),
                    yaxis=dict(categoryorder="total ascending"),
                )
                _col_widget.plotly_chart(fig_pl, use_container_width=True)

        # ── Volume trend chart (total_vol_100gwh, one bar per direction/date) ─
        _vol_rows = df_ip[df_ip["total_vol_100gwh"].notna()].copy()
        if not _vol_rows.empty:
            fig_ip_vol = go.Figure()
            for _dir, _label in [(_dir_export, _t("direction_export")),
                                  (_dir_import, _t("direction_import"))]:
                _sv = _vol_rows[_vol_rows["direction"] == _dir].sort_values("report_date")
                if not _sv.empty:
                    # attach province name from 最高均价 row for that date (if available)
                    _sv_cd = _sv[["province_cn"]].values
                    fig_ip_vol.add_trace(go.Bar(
                        x=_sv["report_date"], y=_sv["total_vol_100gwh"],
                        name=_label,
                        customdata=_sv_cd,
                        hovertemplate=(
                            "<b>%{x}</b><br>"
                            f"{_t('hover_volume')}: %{{y:.2f}} 亿kWh<br>"
                            f"{_t('hover_province')}: %{{customdata[0]}}"
                            "<extra></extra>"
                        ),
                    ))
            fig_ip_vol.update_layout(
                title=dict(text=_t("interprov_vol_trend"), x=0, xanchor="left"),
                xaxis_title="", yaxis_title="亿kWh",
                barmode="group", height=300,
                margin=dict(l=50, r=20, t=55, b=90),
                legend=dict(
                    orientation="h", xanchor="center", x=0.5,
                    yanchor="top", y=-0.2,
                    font=dict(size=11),
                ),
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


# ── Tab 8: Market Fundamentals ───────────────────────────────────────────────
with tab_fundamentals:
    from services.market_fundamentals.loader import (
        load_province_data as _load_fund,
        FUEL_EN as _FUEL_EN,
        FUEL_COLORS as _FUEL_COLORS,
        PROVINCE_EN as _PROVINCE_EN,
    )

    _fund_data = _load_fund()

    if not _fund_data:
        st.warning(_t("fund_no_data"))
    else:
        # ── Controls ──────────────────────────────────────────────────────────
        _all_provs_cn = sorted(_fund_data.keys(), key=lambda p: _PROVINCE_EN.get(p, p))
        _all_provs_en = [_PROVINCE_EN.get(p, p) for p in _all_provs_cn]
        _prov_cn_to_en = {p: _PROVINCE_EN.get(p, p) for p in _all_provs_cn}
        _prov_en_to_cn = {v: k for k, v in _prov_cn_to_en.items()}

        _ctrl1, _ctrl2 = st.columns([5, 1])
        with _ctrl1:
            _default_provs_en = [
                _PROVINCE_EN.get(p, p) for p in ["山东", "广东", "江苏", "蒙西", "四川"]
                if p in _fund_data
            ]
            _sel_provs_en = st.multiselect(
                _t("fund_provinces"), _all_provs_en, default=_default_provs_en,
                key="fund_provs",
            )
        with _ctrl2:
            _fund_year = st.radio(_t("fund_year"), [2025, 2024], key="fund_year")

        if not _sel_provs_en:
            st.info(_t("fund_select_prompt"))
        else:
            _sel_provs_cn = [_prov_en_to_cn[e] for e in _sel_provs_en if e in _prov_en_to_cn]

            # ── Helper: build DataFrame for one metric ─────────────────────
            def _fund_df(metric: str, year: int) -> pd.DataFrame:
                """metric = 'capacity' or 'generation'"""
                rows = []
                for pcn in _sel_provs_cn:
                    info  = _fund_data.get(pcn, {})
                    raw   = info.get(metric, {}).get(year, {})
                    pen   = _prov_cn_to_en[pcn]
                    for fuel_cn in ["风电", "光伏", "水电", "核电", "储能", "火电"]:
                        fuel_en = _FUEL_EN[fuel_cn]
                        val = raw.get(fuel_cn, {}).get("value")
                        if val is not None and val > 0:
                            rows.append({"Province": pen, "Fuel": fuel_en, "Value": val})
                return pd.DataFrame(rows)

            # ── Helper: stacked-bar chart (multi-province) ─────────────────
            def _stacked_bar(df: pd.DataFrame, title: str, unit: str) -> go.Figure:
                fuel_order = ["Wind", "Solar", "Hydro", "Nuclear", "Storage", "Thermal"]
                fig = go.Figure()
                for fuel in fuel_order:
                    sub = df[df["Fuel"] == fuel]
                    if sub.empty:
                        continue
                    fig.add_trace(go.Bar(
                        name=fuel,
                        x=sub["Province"],
                        y=sub["Value"],
                        marker_color=_FUEL_COLORS.get(fuel, "#aaa"),
                        hovertemplate=f"<b>%{{x}}</b><br>{fuel}: %{{y:,.1f}} {unit}<extra></extra>",
                    ))
                fig.update_layout(
                    barmode="stack",
                    title=title,
                    xaxis_title="",
                    yaxis_title=unit,
                    legend_title="Fuel Type",
                    height=420,
                    margin=dict(t=50, b=40),
                )
                return fig

            # ── Helper: donut chart (single province) ─────────────────────
            def _donut(df: pd.DataFrame, title: str, unit: str) -> go.Figure:
                fuel_order = ["Wind", "Solar", "Hydro", "Nuclear", "Storage", "Thermal"]
                df_sorted = df.set_index("Fuel").reindex(fuel_order).dropna()
                fig = go.Figure(go.Pie(
                    labels=df_sorted.index,
                    values=df_sorted["Value"],
                    hole=0.45,
                    marker_colors=[_FUEL_COLORS.get(f, "#aaa") for f in df_sorted.index],
                    textinfo="label+percent",
                    hovertemplate="<b>%{label}</b><br>%{value:,.1f} " + unit + "<br>%{percent}<extra></extra>",
                ))
                fig.update_layout(title=title, height=380, margin=dict(t=50, b=10))
                return fig

            # ── Installed Capacity & Generation (side by side) ─────────────
            _df_cap = _fund_df("capacity", _fund_year)
            _df_gen = _fund_df("generation", _fund_year)

            _cap_title = f"{_t('fund_capacity_title')} — {_fund_year}"
            _gen_title = f"{_t('fund_generation_title')} — {_fund_year}"

            _chart_col1, _chart_col2 = st.columns(2)

            if len(_sel_provs_en) == 1 and not _df_cap.empty:
                with _chart_col1:
                    st.plotly_chart(_donut(_df_cap, _cap_title, "万kW"), use_container_width=True)
            elif not _df_cap.empty:
                with _chart_col1:
                    st.plotly_chart(_stacked_bar(_df_cap, _cap_title, "万kW"), use_container_width=True)

            if len(_sel_provs_en) == 1 and not _df_gen.empty:
                with _chart_col2:
                    st.plotly_chart(_donut(_df_gen, _gen_title, "亿kWh"), use_container_width=True)
            elif not _df_gen.empty:
                with _chart_col2:
                    st.plotly_chart(_stacked_bar(_df_gen, _gen_title, "亿kWh"), use_container_width=True)

            # ── Renewables share bar ───────────────────────────────────────
            _renew_fuels = {"Wind", "Solar", "Hydro", "Nuclear"}
            _renew_rows = []
            for pcn in _sel_provs_cn:
                pen = _prov_cn_to_en[pcn]
                raw = _fund_data.get(pcn, {}).get("capacity", {}).get(_fund_year, {})
                total = sum(
                    v["value"] for v in raw.values()
                    if v.get("value") is not None and v["value"] > 0
                )
                renew = sum(
                    raw.get(fc, {}).get("value") or 0
                    for fc, fe in _FUEL_EN.items()
                    if fe in _renew_fuels
                )
                if total > 0:
                    _renew_rows.append({"Province": pen, "Share (%)": round(renew / total * 100, 1)})

            if _renew_rows:
                _df_renew = pd.DataFrame(sorted(_renew_rows, key=lambda r: -r["Share (%)"]))
                _fig_renew = px.bar(
                    _df_renew, x="Province", y="Share (%)",
                    title=f"{_t('fund_renewables_share')} — {_fund_year}",
                    color_discrete_sequence=["#4C9BE8"],
                    text="Share (%)",
                )
                _fig_renew.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
                _fig_renew.update_layout(height=320, margin=dict(t=50, b=40), yaxis_range=[0, 105])
                st.plotly_chart(_fig_renew, use_container_width=True)

            # ── Peak Load Ranking Table (both years, all selected provinces) ──
            st.subheader(_t("fund_peak_title"))
            _peak_rows = []
            for _pcn in _sel_provs_cn:
                _pen = _prov_cn_to_en.get(_pcn, _pcn)
                _row = {
                    "_prov_en": _pen,
                    "_prov_cn": _pcn,
                }
                for _yr in [2024, 2025]:
                    _pl = _fund_data.get(_pcn, {}).get("peak_load", {}).get(_yr, {})
                    _row[f"{_t('fund_summer_peak')} {_yr}"] = _pl.get("summer")
                    _row[f"{_t('fund_winter_peak')} {_yr}"] = _pl.get("winter")
                _peak_rows.append(_row)

            if _peak_rows:
                _peak_df = pd.DataFrame(_peak_rows)
                # Sort by 2025 summer peak descending
                _sort_col = f"{_t('fund_summer_peak')} 2025"
                if _sort_col in _peak_df.columns and _peak_df[_sort_col].notna().any():
                    _peak_df = _peak_df.sort_values(_sort_col, ascending=False, na_position="last")
                _peak_df = _peak_df.reset_index(drop=True)
                _peak_df.insert(0, "#", range(1, len(_peak_df) + 1))
                _prov_col = "省份" if _is_zh else "Province"
                _peak_df.insert(1, _prov_col, _peak_df["_prov_cn"] if _is_zh else _peak_df["_prov_en"])
                _peak_df = _peak_df.drop(columns=["_prov_en", "_prov_cn"])
                # Format numeric columns
                _num_cols = [c for c in _peak_df.columns if c not in ("#", _prov_col)]
                for _nc in _num_cols:
                    _peak_df[_nc] = _peak_df[_nc].apply(
                        lambda v: f"{int(v):,}" if pd.notna(v) and v else "—"
                    )
                st.dataframe(_peak_df, use_container_width=True, hide_index=True)

            # ── Standard EOH (expected operating hours/year) ──────────────────
            # Used for both LF and system tightness calculations.
            # Thermal blended: coal-dominated Chinese fleet (~5 500 h).
            # Storage excluded — net-zero generation asset.
            _STD_EOH = {
                "Wind":    2000,
                "Solar":   1100,
                "Thermal": 5500,
                "Hydro":   3500,
                "Nuclear": 7500,
            }
            _EOH_NOTE = (
                f"Wind {_STD_EOH['Wind']} h · Solar {_STD_EOH['Solar']} h · "
                f"Thermal {_STD_EOH['Thermal']} h · Hydro {_STD_EOH['Hydro']} h · "
                f"Nuclear {_STD_EOH['Nuclear']} h"
            )

            # ── Load Factor ranking ───────────────────────────────────────────
            st.divider()
            st.subheader(f"{_t('fund_lf_title')} — {_fund_year}")
            st.caption(_t("fund_lf_caption"))

            _lf_rows = []
            for _pcn, _pdata in _fund_data.items():
                _pen = _prov_cn_to_en.get(_pcn, _pcn)
                _cap_yr = _pdata.get("capacity", {}).get(_fund_year, {})
                _gen_yr = _pdata.get("generation", {}).get(_fund_year, {})
                _row = {"_prov_en": _pen, "_prov_cn": _pcn}
                for _fcn, _fen in _FUEL_EN.items():
                    if _fen not in _STD_EOH:
                        continue
                    _cap_v = (_cap_yr.get(_fcn) or {}).get("value")
                    _gen_v = (_gen_yr.get(_fcn) or {}).get("value")
                    if _cap_v and _cap_v > 0 and _gen_v is not None and _gen_v >= 0:
                        # LF = gen(亿kWh)×10^8 / (cap(万kW)×10^4 × 8760)
                        #     = gen × 10^4 / (cap × 8760)
                        _row[_fen] = round(_gen_v * 1e4 / (_cap_v * 8760) * 100, 1)
                    else:
                        _row[_fen] = None
                _lf_rows.append(_row)

            if _lf_rows:
                _lf_df = pd.DataFrame(_lf_rows)
                _fuel_cols_lf = [f for f in _STD_EOH if f in _lf_df.columns]
                _lf_sort_col = st.selectbox(
                    _t("fund_lf_sort"), _fuel_cols_lf,
                    index=0, key="lf_sort_fuel",
                )
                if _lf_sort_col and _lf_df[_lf_sort_col].notna().any():
                    _lf_df = _lf_df.sort_values(_lf_sort_col, ascending=False, na_position="last")
                _lf_df = _lf_df.reset_index(drop=True)
                _lf_df.insert(0, "#", range(1, len(_lf_df) + 1))
                _prov_col_lf = "省份" if _is_zh else "Province"
                _lf_df.insert(1, _prov_col_lf,
                              _lf_df["_prov_cn"] if _is_zh else _lf_df["_prov_en"])
                _lf_df = _lf_df.drop(columns=["_prov_en", "_prov_cn"])
                # Interleave LF% and equivalent hours (LF×8760) for each fuel
                _lf_out = _lf_df[["#", _prov_col_lf]].copy()
                for _fc in _fuel_cols_lf:
                    _lf_out[f"{_fc} (%)"] = _lf_df[_fc].apply(
                        lambda v: f"{v:.1f}%" if pd.notna(v) and v is not None else "—"
                    )
                    _lf_out[f"{_fc} (h)"] = _lf_df[_fc].apply(
                        lambda v: f"{int(round(v * 8760 / 100)):,}" if pd.notna(v) and v is not None else "—"
                    )
                st.dataframe(_lf_out, use_container_width=True, hide_index=True)

            # ── System Tightness ranking ──────────────────────────────────────
            st.divider()
            st.subheader(f"{_t('fund_tightness_title')} — {_fund_year}")
            st.caption(f"{_t('fund_tightness_caption')}  |  {_EOH_NOTE}")

            _tight_rows = []
            for _pcn, _pdata in _fund_data.items():
                _pen = _prov_cn_to_en.get(_pcn, _pcn)
                _cap_yr = _pdata.get("capacity", {}).get(_fund_year, {})
                _gen_yr = _pdata.get("generation", {}).get(_fund_year, {})
                _pl_yr  = _pdata.get("peak_load", {}).get(_fund_year, {})

                # Effective generation capacity (MW)
                # = Σ_type [cap(万kW) × 10 MW/万kW × EOH / 8760]
                # Use `or 0` after .get("value") to guard against {"value": None}.
                _eff_mw = sum(
                    ((_cap_yr.get(_fcn) or {}).get("value") or 0) * 10 * _STD_EOH[_fen] / 8760
                    for _fcn, _fen in _FUEL_EN.items()
                    if _fen in _STD_EOH
                    and ((_cap_yr.get(_fcn) or {}).get("value") or 0) > 0
                )

                # Average (baseload) demand (MW)
                # = total annual generation(亿kWh) × 10^8 kWh/亿kWh / 8760 h / 1000 kW/MW
                _total_gen = sum(
                    (_gen_yr.get(_fcn) or {}).get("value") or 0
                    for _fcn in _FUEL_EN
                )
                _avg_mw = _total_gen * 1e8 / 8760 / 1000 if _total_gen > 0 else None

                _sum_pk = _pl_yr.get("summer")
                _win_pk = _pl_yr.get("winter")

                _tight_rows.append({
                    "_prov_en":            _pen,
                    "_prov_cn":            _pcn,
                    _t("fund_eff_cap"):    round(_eff_mw)   if _eff_mw  else None,
                    _t("fund_avg_demand"): round(_avg_mw)   if _avg_mw  else None,
                    "Summer Peak (MW)":    round(_sum_pk)   if _sum_pk  else None,
                    "Winter Peak (MW)":    round(_win_pk)   if _win_pk  else None,
                    _t("fund_tight_avg"):  round(_eff_mw - _avg_mw) if (_eff_mw and _avg_mw)  else None,
                    _t("fund_tight_summer"): round(_eff_mw - _sum_pk) if (_eff_mw and _sum_pk) else None,
                    _t("fund_tight_winter"): round(_eff_mw - _win_pk) if (_eff_mw and _win_pk) else None,
                })

            if _tight_rows:
                _tdf = pd.DataFrame(_tight_rows)
                # Rename raw peak columns to translated labels
                _tdf = _tdf.rename(columns={
                    "Summer Peak (MW)": f"{_t('fund_summer_peak')} (MW)",
                    "Winter Peak (MW)": f"{_t('fund_winter_peak')} (MW)",
                })
                # Sort by tightness vs summer peak ascending (tightest = most stressed first)
                _tight_sort = _t("fund_tight_summer")
                if _tight_sort in _tdf.columns and _tdf[_tight_sort].notna().any():
                    _tdf = _tdf.sort_values(_tight_sort, ascending=True, na_position="last")
                _tdf = _tdf.reset_index(drop=True)
                _tdf.insert(0, "#", range(1, len(_tdf) + 1))
                _prov_col_t = "省份" if _is_zh else "Province"
                _tdf.insert(1, _prov_col_t,
                            _tdf["_prov_cn"] if _is_zh else _tdf["_prov_en"])
                _tdf = _tdf.drop(columns=["_prov_en", "_prov_cn"])

                # Format all numeric columns; prefix surplus cols with + / − for clarity
                _surplus_col_keys = {
                    _t("fund_tight_avg"),
                    _t("fund_tight_summer"),
                    _t("fund_tight_winter"),
                }
                _num_cols_t = [c for c in _tdf.columns if c not in ("#", _prov_col_t)]
                for _nc in _num_cols_t:
                    if _nc in _surplus_col_keys:
                        _tdf[_nc] = _tdf[_nc].apply(
                            lambda v: (f"+{int(v):,}" if v > 0 else f"{int(v):,}")
                            if pd.notna(v) and v is not None else "—"
                        )
                    else:
                        _tdf[_nc] = _tdf[_nc].apply(
                            lambda v: f"{int(v):,}" if pd.notna(v) and v is not None else "—"
                        )

                st.dataframe(_tdf, use_container_width=True, hide_index=True)


# ── Tab 9: Agent ──────────────────────────────────────────────────────────────
with tab_agent:
    import os as _os
    import json as _json
    import uuid as _uuid
    import anthropic as _anthropic

    # ── Memory infrastructure ──────────────────────────────────────────────────
    _SPOT_MEM_KEY = "spot_v1"
    _SPOT_APP_NAME = "spot_market"

    @st.cache_resource
    def _ensure_spot_memory_table():
        conn = _conn()
        with conn.cursor() as _cur:
            _cur.execute("""
                CREATE TABLE IF NOT EXISTS marketdata.agent_memory (
                    id SERIAL PRIMARY KEY,
                    app TEXT NOT NULL DEFAULT 'spot_market',
                    category TEXT NOT NULL,
                    subject TEXT NOT NULL,
                    content TEXT NOT NULL,
                    source TEXT DEFAULT 'manual',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    active BOOLEAN DEFAULT TRUE
                )
            """)
            _cur.execute("""
                ALTER TABLE marketdata.agent_memory
                ADD COLUMN IF NOT EXISTS app TEXT NOT NULL DEFAULT 'spot_market'
            """)
        conn.commit()
        return True

    @st.cache_data(ttl=60)
    def _load_spot_memories(_key) -> pd.DataFrame:
        try:
            return pd.read_sql(
                "SELECT id, category, subject, content, source "
                "FROM marketdata.agent_memory WHERE active AND app=%s ORDER BY id",
                _conn(),
                params=(_SPOT_APP_NAME,),
            )
        except Exception:
            return pd.DataFrame(columns=["id", "category", "subject", "content", "source"])

    def _save_spot_memory(category: str, subject: str, content: str, source: str = "manual"):
        conn = _conn()
        with conn.cursor() as _cur:
            _cur.execute(
                "INSERT INTO marketdata.agent_memory (app,category,subject,content,source) "
                "VALUES (%s,%s,%s,%s,%s)",
                (_SPOT_APP_NAME, category, subject, content, source),
            )
        conn.commit()
        _load_spot_memories.clear()

    def _delete_spot_memory(memory_id: int):
        conn = _conn()
        with conn.cursor() as _cur:
            _cur.execute(
                "UPDATE marketdata.agent_memory SET active=FALSE WHERE id=%s AND app=%s",
                (memory_id, _SPOT_APP_NAME),
            )
        conn.commit()
        _load_spot_memories.clear()

    try:
        _ensure_spot_memory_table()
    except Exception:
        pass  # non-fatal: agent works without this legacy table

    # ── Session persistence ────────────────────────────────────────────────────

    def _ensure_spot_sessions_table():
        if st.session_state.get("_spot_sessions_table_ok"):
            return
        conn = _conn()
        with conn.cursor() as _cur:
            _cur.execute("""
                CREATE TABLE IF NOT EXISTS staging.spot_analyst_sessions (
                    session_id TEXT PRIMARY KEY,
                    messages JSONB NOT NULL DEFAULT '[]',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
        conn.commit()
        st.session_state["_spot_sessions_table_ok"] = True

    def _save_spot_session(session_id: str, display_messages: list):
        try:
            _ensure_spot_sessions_table()
            conn = _conn()
            with conn.cursor() as _cur:
                _payload = _json.dumps(display_messages)
                _cur.execute(
                    "INSERT INTO staging.spot_analyst_sessions "
                    "(session_id, messages, updated_at) VALUES (%s, %s, NOW()) "
                    "ON CONFLICT (session_id) DO UPDATE "
                    "SET messages=%s, updated_at=NOW()",
                    (session_id, _payload, _payload),
                )
            conn.commit()
        except Exception:
            pass

    def _load_spot_session(session_id: str) -> list:
        try:
            _ensure_spot_sessions_table()
            conn = _conn()
            with conn.cursor() as _cur:
                _cur.execute(
                    "SELECT messages FROM staging.spot_analyst_sessions "
                    "WHERE session_id = %s",
                    (session_id,),
                )
                row = _cur.fetchone()
                return _json.loads(row[0]) if row else []
        except Exception:
            return []

    def _list_recent_spot_sessions(limit: int = 3) -> pd.DataFrame:
        try:
            _ensure_spot_sessions_table()
            return pd.read_sql(
                "SELECT session_id, jsonb_array_length(messages) AS msg_count, "
                "updated_at AT TIME ZONE 'Asia/Singapore' AS updated_at "
                "FROM staging.spot_analyst_sessions "
                "WHERE jsonb_array_length(messages) > 0 "
                "ORDER BY updated_at DESC LIMIT %s",
                _conn(), params=(limit,),
            )
        except Exception:
            return pd.DataFrame()

    def _display_from_session(session_messages: list) -> list:
        """Reconstruct agent_display list from saved display messages."""
        return session_messages if session_messages else [
            {"role": "assistant", "content": _t("agent_welcome"), "tool": None}
        ]

    def _api_messages_from_display(display: list) -> list:
        """Reconstruct lean API message list (text only) from display messages for context."""
        return [
            {"role": m["role"], "content": m["content"]}
            for m in display
            if m.get("role") in ("user", "assistant") and m.get("content")
        ]

    # ── Knowledge gap interview helpers ────────────────────────────────────────

    def _generate_spot_interview_questions() -> list[dict]:
        """Audit kp_expert_insights pool, identify gaps, return up to 5 questions."""
        try:
            _summary = pd.read_sql(
                "SELECT insight_type, confidence, COUNT(*) AS n "
                "FROM staging.kp_expert_insights WHERE active = TRUE "
                "GROUP BY insight_type, confidence ORDER BY n DESC",
                _conn(),
            )
        except Exception:
            _summary = pd.DataFrame()
        try:
            _sample = pd.read_sql(
                "SELECT insight_text, insight_type "
                "FROM staging.kp_expert_insights WHERE active = TRUE "
                "ORDER BY id DESC LIMIT 15",
                _conn(),
            )
        except Exception:
            _sample = pd.DataFrame()

        _ctx = ["Current expert insight pool:"]
        if not _summary.empty:
            for _, _r in _summary.iterrows():
                _ctx.append(f"  {_r['insight_type']} ({_r['confidence']}): {int(_r['n'])} insights")
        else:
            _ctx.append("  (empty — knowledge gaps in all areas)")
        _ctx.append("\nSample of already-known insights (do NOT duplicate):")
        if not _sample.empty:
            for _, _r in _sample.iterrows():
                _ctx.append(f"  [{_r['insight_type']}] {str(_r['insight_text'])[:120]}")

        _system = """\
You are the China electricity market strategist agent auditing your own knowledge base.
Identify the 5 most valuable areas where knowledge is THIN, UNCERTAIN, or MISSING.
Generate one precise expert interview question per gap — something only a practitioner
with hands-on China electricity market experience can answer from observation.

Prioritise gaps in these areas (in order):
1. Province-specific dispatch mechanics (curtailment triggers, peak-shaving rules, settlement quirks)
2. FM/ancillary market nuances in Inner Mongolia, Shanxi, Shandong, or Guangdong
3. BESS capacity payment rules or recent policy changes with concrete operational implications
4. Counterintuitive DA/RT spread patterns the expert has personally observed
5. Revenue stacking strategies that differentiate top-performing BESS assets from median

Do NOT generate questions already answered in the sample insights above.
Do NOT generate generic textbook questions about Chinese power markets.

Respond ONLY with valid JSON:
{"questions": [{"question": "...", "topic": "market_structure|regulation|operations|dispatch_economics|investment", "why_asking": "one sentence on what knowledge gap this fills"}]}
"""
        try:
            _api_key = _os.environ.get("ANTHROPIC_API_KEY", "")
            if not _api_key:
                return []
            _haiku = _anthropic.Anthropic(api_key=_api_key)
            _resp = _haiku.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=900,
                system=_system,
                messages=[{"role": "user", "content": "\n".join(_ctx)}],
            )
            _raw = _resp.content[0].text.strip()
            if _raw.startswith("```"):
                _raw = _raw.split("```", 2)[1]
                if _raw.startswith("json"):
                    _raw = _raw[4:]
            return _json.loads(_raw).get("questions", [])[:5]
        except Exception:
            return []

    def _answer_from_kb(question: str) -> str | None:
        """Try to answer a gap question from the knowledge base. Returns text answer or None."""
        try:
            from services.knowledge_pool.knowledge_docs import search_reference_docs as _srd
            _chunks = _srd(query=question, app="strategist", limit=5)
            if not _chunks or len(_chunks) < 2:
                return None
            _combined = "\n\n".join(c.get("chunk_text", "") for c in _chunks[:5])
            if len(_combined) < 150:
                return None
            _api_key = _os.environ.get("ANTHROPIC_API_KEY", "")
            if not _api_key:
                return None
            _haiku = _anthropic.Anthropic(api_key=_api_key)
            _resp = _haiku.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=400,
                system=(
                    "Answer the question using only the provided knowledge base excerpts. "
                    "Be concise and specific (2-4 sentences). If the excerpts do not contain "
                    "enough information to answer confidently, respond with exactly: INSUFFICIENT"
                ),
                messages=[{"role": "user", "content": (
                    f"Question: {question}\n\nKB Excerpts:\n{_combined[:3000]}"
                )}],
            )
            _answer = _resp.content[0].text.strip()
            if "INSUFFICIENT" in _answer.upper() or len(_answer) < 40:
                return None
            return _answer
        except Exception:
            return None

    def _store_spot_interview_answer(question: str, answer: str, topic: str,
                                      confidence: str = "high") -> None:
        """Store a user or KB-sourced expert interview answer as an insight."""
        _text = f"[Expert interview] Q: {question[:150]} | A: {answer}"
        try:
            conn = _conn()
            with conn.cursor() as _cur:
                _cur.execute(
                    "INSERT INTO staging.kp_expert_insights "
                    "(insight_text, insight_type, confidence, source_session, validated_at) "
                    "VALUES (%s, %s, %s, %s, NOW())",
                    (_text[:1000], topic, confidence, date.today().isoformat()),
                )
            conn.commit()
        except Exception as _e:
            raise _e

    # ── Base system prompt ─────────────────────────────────────────────────────
    _SPOT_AGENT_BASE_SYSTEM = """\
You are a specialist analyst for China's spot electricity market. \
Your knowledge comes **exclusively** from the data tools below — never from general training data or external information. \
Do not state any price level, trend, or market event unless it was returned by a tool call in this conversation.

## Domain definitions
- **DA price**: Day-Ahead electricity clearing price (¥/kWh). Set one day ahead via auction.
- **RT price**: Real-Time electricity clearing price (¥/kWh). Reflects actual intraday supply/demand balance.
- **Spread**: DA − RT; positive = DA premium (normal); negative = RT spike (intraday supply stress).
- **送端**: Exporting province in inter-provincial spot market.
- **受端**: Importing province in inter-provincial spot market.
- Province names in DB: Shandong, Guangdong, Mengxi, Shanxi, Gansu, Sichuan, Yunnan, Guizhou, \
Guangxi, Hunan, Hubei, Anhui, Zhejiang, Jiangsu, Fujian, Henan, Shaanxi, Ningxia, Xinjiang, \
Liaoning, Jilin, Heilongjiang, Mengdong, Hebei, Hebei-North, Hebei-South, Qinghai, Jiangxi, \
Hainan, Chongqing, Shanghai, Beijing, Tianjin.

## Analytical framework
1. Always call a tool before stating any price, spread, volume, or trend.
2. When comparing provinces or time periods, call get_spot_prices with the full range then compute statistics from the returned rows.
3. For structural questions (fuel mix, capacity, renewables share, peak demand), call get_market_fundamentals.
4. For inter-provincial flow questions (volumes, sending/receiving provinces), call get_interprov_flow.
5. For qualitative market colour or key drivers, call get_market_summaries.
6. For questions about market rules, trading procedures, settlement mechanisms, annual exchange reports, \
regulatory policy, or any uploaded reference data (Excel spreadsheets with trading volumes, network losses, \
contract data, etc.), call search_reference_docs. \
The knowledge base indexes ALL file types: PDF, Excel (.xlsx/.xls), PowerPoint, Word, and text files.
7. Use markdown tables for multi-province or multi-period comparisons.
8. To ingest a new reference document (PDF, Excel, PPTX, DOCX — any format), call ingest_kb_document \
with s3_key (file in the S3 uploads bucket) or file_path (local/repo-relative path). \
For spot market daily price PDFs specifically, use run_pipeline instead (it also extracts structured DA/RT price data).
9. If the user says they already uploaded a file via the UI, call search_reference_docs directly — \
the file is already ingested and searchable without calling ingest_kb_document again.
10. For questions about Inner Mongolia BESS asset performance, P&L, dispatch cycles, or strategy \
comparison across suyou/hangjinqi/siziwangqi/gushanliang, call get_bess_pnl. \
It returns daily P&L and dispatch metrics across all 5 strategy scenarios.
"""

    def _build_spot_system(query: str = "") -> str:
        base = _SPOT_AGENT_BASE_SYSTEM
        _api_key = _os.environ.get("ANTHROPIC_API_KEY", "")

        if query:
            # Inject structured expert insights from kp_expert_insights (FTS-retrieved)
            try:
                from services.knowledge_pool.expert_memory import (
                    get_relevant_insights as _get_insights,
                    inject_expert_memory as _inject_memory,
                )
                _insights = _get_insights(query=query, limit=5)
                _mem_block = _inject_memory(_insights)
                if _mem_block:
                    base += f"\n\n{_mem_block}"
            except Exception:
                pass

            # Inject advanced retrieval context (HyDE + reranking) from knowledge pool
            if _api_key:
                try:
                    from services.knowledge_pool.advanced_retrieval import retrieve_for_agent as _retrieve
                    _kb_ctx = _retrieve(
                        query=query, api_key=_api_key, app="shared",
                        use_hyde=True, use_rerank=True, top_k=5,
                    )
                    if _kb_ctx:
                        base += f"\n\n{_kb_ctx}"
                except Exception:
                    pass

        # Inject flat analyst preferences from agent_memory
        mems = _load_spot_memories(_SPOT_MEM_KEY)
        if not mems.empty:
            mem_lines = "\n".join(
                f"- [{r.category}] {r.subject}: {r.content}"
                for r in mems.itertuples()
            )
            base += f"\n\n## Analyst preferences & domain knowledge\n{mem_lines}"

        lang_suffix = "\n\n请用中文（简体）回复所有问题。" if st.session_state.get("lang_radio") == "中文" else "\n\nRespond in English."
        return base + lang_suffix

    def _extract_spot_memories(user_msg: str, agent_reply: str) -> list[dict]:
        """Use Haiku to extract memorable facts/preferences from a conversation turn."""
        api_key = _os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return []
        try:
            haiku = _anthropic.Anthropic(api_key=api_key)
            resp = haiku.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=512,
                system=(
                    "Extract memorable analyst preferences or domain facts from the conversation. "
                    "Return a JSON array of objects with keys: category (string, e.g. 'preference', 'market_view', 'methodology'), "
                    "subject (short title ≤8 words), content (one sentence). "
                    "Only extract genuinely reusable insights — not one-off data points. "
                    "Return [] if nothing is worth remembering."
                ),
                messages=[{
                    "role": "user",
                    "content": f"User: {user_msg}\n\nAgent: {agent_reply}",
                }],
            )
            raw = next((b.text for b in resp.content if hasattr(b, "text")), "[]")
            start, end = raw.find("["), raw.rfind("]")
            if start == -1:
                return []
            return _json.loads(raw[start:end + 1])
        except Exception:
            return []

    # ── Tool definitions ───────────────────────────────────────────────────────
    _AGENT_TOOLS = [
        {
            "name": "get_spot_prices",
            "description": (
                "Fetch day-ahead (DA) and real-time (RT) spot electricity clearing "
                "prices from public.spot_daily. Prices in ¥/kWh."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "ISO date, e.g. '2026-01-01'"},
                    "end_date":   {"type": "string", "description": "ISO date, e.g. '2026-04-30'"},
                    "provinces":  {
                        "type": "array", "items": {"type": "string"},
                        "description": "Optional list of province_en names, e.g. ['Shandong','Guangdong']",
                    },
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "get_interprov_flow",
            "description": (
                "Fetch inter-provincial spot trading data (省间现货交易情况). "
                "Returns daily peak/floor average prices and volumes for exporting "
                "(送端) and importing (受端) provinces."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "ISO date"},
                    "end_date":   {"type": "string", "description": "ISO date"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "get_market_summaries",
            "description": (
                "Fetch AI-generated daily market narrative summaries. Each summary "
                "covers price levels, key drivers, inter-provincial flows, and "
                "notable events for that trading day."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "ISO date"},
                    "end_date":   {"type": "string", "description": "ISO date"},
                },
                "required": ["start_date", "end_date"],
            },
        },
        {
            "name": "run_pipeline",
            "description": (
                "Run the full spot-market ingestion pipeline for one PDF report. "
                "Parses DA/RT prices, inter-provincial data, generates AI summary, "
                "and populates the knowledge pool."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "pdf_path": {"type": "string", "description": "Absolute or repo-relative path to the PDF"},
                    "dry_run":  {"type": "boolean", "description": "If true, parse only — no DB writes"},
                },
                "required": ["pdf_path"],
            },
        },
        {
            "name": "ingest_kb_document",
            "description": (
                "Add any reference document — Excel (.xlsx/.xls), PDF, PowerPoint (.pptx), "
                "Word (.docx), plain text, or image — to the knowledge base so it can be "
                "searched with search_reference_docs. "
                "Use s3_key to fetch a file from the uploads S3 bucket, or file_path for a "
                "local/repo-relative path. "
                "Supports all file types including Excel spreadsheets with trading data, "
                "network loss tables, contract volumes, or any structured market data."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "s3_key": {
                        "type": "string",
                        "description": "S3 object key in the uploads bucket, e.g. 'uploads/跨区交易.xlsx'",
                    },
                    "file_path": {
                        "type": "string",
                        "description": "Repo-relative or absolute path, e.g. 'data/market-fundamentals/report.xlsx'",
                    },
                    "category": {
                        "type": "string",
                        "description": "Optional: market_rules | annual_report | policy_doc | technical_spec | research_report | other",
                    },
                    "app": {
                        "type": "string",
                        "description": "'shared' (visible to all agents, default) or 'strategist'",
                    },
                },
                "required": [],
            },
        },
        {
            "name": "get_market_fundamentals",
            "description": (
                "Fetch market fundamentals for Chinese electricity provinces: "
                "installed capacity by fuel type (万kW), generation mix (亿kWh), "
                "and seasonal peak loads (MW). Data covers 2024 and 2025. "
                "Use this to answer questions about fuel mix, renewables penetration, "
                "storage capacity, peak demand, or structural comparisons between provinces."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "provinces": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Optional list of province_en names, e.g. ['Shandong','Guangdong']. Omit for all.",
                    },
                    "year": {
                        "type": "integer",
                        "description": "2024 or 2025 (default 2025)",
                    },
                },
                "required": [],
            },
        },
        {
            "name": "search_reference_docs",
            "description": (
                "Search the uploaded reference document knowledge base. Covers market rules, "
                "annual exchange reports, policy documents, technical specs, research reports, "
                "AND Excel spreadsheets with trading data (cross-regional volumes, network losses, "
                "contract quantities, etc.). Supports all file types: PDF, Excel (.xlsx/.xls), "
                "PPTX, DOCX, TXT. Use this whenever the user asks about trading rules, "
                "settlement procedures, market mechanisms, regulatory requirements, "
                "or any data from documents they have uploaded."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search query in Chinese or English, e.g. '结算周期' or 'DA price cap'",
                    },
                    "category": {
                        "type": "string",
                        "description": (
                            "Optional category filter: market_rules | annual_report | "
                            "policy_doc | technical_spec | research_report | other. "
                            "Omit to search all categories."
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Max number of text chunks to return (default 5, max 10).",
                    },
                },
                "required": ["query"],
            },
        },
        {
            "name": "get_bess_pnl",
            "description": (
                "Fetch daily P&L and dispatch metrics for Inner Mongolia BESS assets "
                "across all strategy scenarios. Returns total_pnl, market_revenue, "
                "compensation_revenue, discharge_mwh, charge_mwh, avg_daily_cycles. "
                "The 4 assets: suyou, hangjinqi, siziwangqi, gushanliang. "
                "Scenarios: perfect_foresight_hourly (LP upper bound), "
                "forecast_ols_rt_time_v1 (LP forecast), nominated_dispatch (ops), "
                "cleared_actual (ops actual), trading_cleared (id market). "
                "Use this for BESS performance analysis, strategy comparison, or P&L attribution."
            ),
            "input_schema": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "ISO date, e.g. '2026-01-01'"},
                    "end_date":   {"type": "string", "description": "ISO date, e.g. '2026-04-30'"},
                    "asset_codes": {
                        "type": "array", "items": {"type": "string"},
                        "description": "Optional list of asset codes. Omit for all 4 IM assets.",
                    },
                },
                "required": ["start_date", "end_date"],
            },
        },
    ]

    # ── Tool dispatcher ────────────────────────────────────────────────────────
    def _dispatch_tool(name: str, inputs: dict) -> str:
        from services.spot_mcp.tools import (
            get_spot_prices as _gsp,
            get_interprov_flow as _gif,
            get_market_summaries as _gms,
            run_pipeline as _rp,
            get_market_fundamentals as _gmf,
            ingest_kb_document as _ikd,
        )
        try:
            if name == "get_spot_prices":
                result = _gsp(**inputs)
            elif name == "get_interprov_flow":
                result = _gif(**inputs)
            elif name == "get_market_summaries":
                result = _gms(**inputs)
            elif name == "run_pipeline":
                result = _rp(**inputs)
            elif name == "get_market_fundamentals":
                result = _gmf(**inputs)
            elif name == "search_reference_docs":
                from services.knowledge_pool.knowledge_docs import search_reference_docs as _srd
                _limit = min(int(inputs.get("limit", 5)), 10)
                rows = _srd(
                    query=inputs["query"],
                    category=inputs.get("category"),
                    app="strategist",
                    limit=_limit,
                )
                result = {"count": len(rows), "chunks": rows}
            elif name == "ingest_kb_document":
                result = _ikd(**inputs)
            elif name == "get_bess_pnl":
                from services.bess_mcp.tools import bess_get_portfolio_pnl as _bgms
                result = _bgms(
                    asset_codes=inputs.get("asset_codes"),
                    start_date=inputs["start_date"],
                    end_date=inputs["end_date"],
                )
            else:
                result = {"error": f"Unknown tool: {name}"}
        except Exception as _e:
            result = {"error": str(_e)}
        return _json.dumps(result, default=str)

    # ── Agent turn (handles multi-step tool-use loop) ──────────────────────────
    def _run_agent_turn(
        messages: list, system: str, text_placeholder=None
    ) -> tuple[str, list, list]:
        _api_key = _os.environ.get("ANTHROPIC_API_KEY", "")
        if not _api_key:
            return _t("agent_no_key"), messages, []

        client = _anthropic.Anthropic(api_key=_api_key)
        tool_events: list[dict] = []
        # Status line shown during tool calls (lives inside the same chat message)
        _status_ph = st.empty() if text_placeholder is not None else None

        while True:
            streamed_text = ""

            if text_placeholder is not None:
                if _status_ph:
                    _status_ph.caption("⏳ Thinking…")
                with client.messages.stream(
                    model="claude-sonnet-4-6",
                    max_tokens=4096,
                    system=system,
                    tools=_AGENT_TOOLS,
                    messages=messages,
                ) as _stream:
                    for _chunk in _stream.text_stream:
                        streamed_text += _chunk
                        if _status_ph:
                            _status_ph.empty()
                        text_placeholder.markdown(streamed_text + "▌")
                    _final = _stream.get_final_message()
            else:
                _final = client.messages.create(
                    model="claude-sonnet-4-6",
                    max_tokens=4096,
                    system=system,
                    tools=_AGENT_TOOLS,
                    messages=messages,
                )
                streamed_text = next(
                    (b.text for b in _final.content if hasattr(b, "text")), ""
                )

            messages = messages + [{"role": "assistant", "content": _final.content}]

            if _final.stop_reason == "end_turn":
                if _status_ph:
                    _status_ph.empty()
                if text_placeholder is not None:
                    text_placeholder.markdown(streamed_text)
                return streamed_text, messages, tool_events

            if _final.stop_reason != "tool_use":
                if _status_ph:
                    _status_ph.empty()
                return f"Unexpected stop_reason: {_final.stop_reason}", messages, tool_events

            tool_results = []
            for block in _final.content:
                if block.type == "tool_use":
                    if _status_ph:
                        _icon = _TOOL_ICONS.get(block.name, "⚙️")
                        _status_ph.caption(f"{_icon} Calling `{block.name}`…")
                    result_str = _dispatch_tool(block.name, block.input)
                    tool_events.append({"tool": block.name, "result": result_str})
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block.id,
                        "content": result_str,
                    })

            if _status_ph:
                _status_ph.empty()

            messages = messages + [{"role": "user", "content": tool_results}]

    # ── Session state ──────────────────────────────────────────────────────────
    if "spot_strat_session_id" not in st.session_state:
        st.session_state["spot_strat_session_id"] = str(_uuid.uuid4())
    if "agent_messages" not in st.session_state:
        st.session_state["agent_messages"] = []
    if "agent_display" not in st.session_state:
        st.session_state["agent_display"] = [
            {"role": "assistant", "content": _t("agent_welcome"), "tool": None}
        ]
    if "spot_mem_suggestions" not in st.session_state:
        st.session_state["spot_mem_suggestions"] = []  # kept for back-compat, unused

    st.subheader(_t("agent_title"))
    _n_ins = (_cached_memory_stats() or {}).get("total", 0) or 0
    if _n_ins:
        st.caption(
            f"{_t('agent_caption')} · "
            f"{_n_ins} expert insight{'s' if _n_ins != 1 else ''} accumulated"
        )
    else:
        st.caption(_t("agent_caption"))

    # ── Clear button (generates new session UUID, old session preserved in DB) ─
    if st.button(_t("agent_clear"), key="agent_clear_btn"):
        st.session_state["spot_strat_session_id"] = str(_uuid.uuid4())
        st.session_state["agent_messages"] = []
        st.session_state["agent_display"] = [
            {"role": "assistant", "content": _t("agent_welcome"), "tool": None}
        ]
        st.rerun()

    # ── Resume previous session (only when current chat is empty) ─────────────
    if not st.session_state["agent_messages"]:
        _recent_sessions = _list_recent_spot_sessions()
        if not _recent_sessions.empty:
            with st.expander("Resume a previous conversation?", expanded=False):
                for _, _srow in _recent_sessions.iterrows():
                    _sess_label = (
                        f"{_srow['updated_at'].strftime('%Y-%m-%d %H:%M')} — "
                        f"{int(_srow['msg_count'])} messages"
                    )
                    if st.button(_sess_label, key=f"resume_spot_{_srow['session_id']}"):
                        _loaded_display = _load_spot_session(_srow["session_id"])
                        st.session_state["spot_strat_session_id"] = _srow["session_id"]
                        st.session_state["agent_display"] = (
                            _display_from_session(_loaded_display)
                        )
                        st.session_state["agent_messages"] = (
                            _api_messages_from_display(_loaded_display)
                        )
                        st.rerun()

    # ── Tool result display helper ─────────────────────────────────────────────
    _TOOL_ICONS = {
        "get_spot_prices":        "📊",
        "get_interprov_flow":     "🔀",
        "get_market_summaries":   "📝",
        "run_pipeline":           "⚙️",
        "get_market_fundamentals": "🏭",
        "search_reference_docs":  "🔍",
        "ingest_kb_document":     "📥",
        "get_bess_pnl":           "⚡",
    }

    def _render_tool_result(tool_name: str, content_str: str):
        icon = _TOOL_ICONS.get(tool_name, "🔧")
        try:
            parsed = _json.loads(content_str)
        except Exception:
            st.code(content_str, language="json")
            return

        if "error" in parsed:
            st.error(parsed["error"])
            return

        if tool_name == "get_spot_prices":
            rows = parsed.get("rows", [])
            n = len(rows)
            provs = sorted({r.get("province_en", "") for r in rows})
            dates = sorted({r.get("report_date", "") for r in rows})
            date_range = f"{dates[0]} → {dates[-1]}" if dates else "—"
            c1, c2, c3 = st.columns(3)
            c1.metric("Rows", f"{n:,}")
            c2.metric("Provinces", f"{len(provs)}")
            c3.metric("Date range", date_range)
            if provs:
                st.caption("Provinces: " + ", ".join(provs[:8]) + ("…" if len(provs) > 8 else ""))
            with st.expander("Raw data", expanded=False):
                st.json(parsed, expanded=1)

        elif tool_name == "get_interprov_flow":
            rows = parsed.get("rows", [])
            n = len(rows)
            dates = sorted({r.get("report_date", "") for r in rows})
            date_range = f"{dates[0]} → {dates[-1]}" if dates else "—"
            c1, c2 = st.columns(2)
            c1.metric("Rows", f"{n:,}")
            c2.metric("Date range", date_range)
            with st.expander("Raw data", expanded=False):
                st.json(parsed, expanded=1)

        elif tool_name == "get_market_summaries":
            items = parsed.get("summaries", [])
            n = len(items)
            dates = sorted({r.get("report_date", "") for r in items})
            date_range = f"{dates[0]} → {dates[-1]}" if dates else "—"
            c1, c2 = st.columns(2)
            c1.metric("Summaries", f"{n:,}")
            c2.metric("Date range", date_range)
            if items:
                with st.expander("Latest summary", expanded=False):
                    st.markdown(items[0].get("summary_text", "")[:600])
            with st.expander("All summaries (raw)", expanded=False):
                st.json(parsed, expanded=1)

        elif tool_name == "get_market_fundamentals":
            provs = parsed.get("provinces", [])
            n = len(provs)
            year = parsed.get("year", "")
            c1, c2 = st.columns(2)
            c1.metric("Provinces", f"{n:,}")
            c2.metric("Year", str(year))
            with st.expander("Raw data", expanded=False):
                st.json(parsed, expanded=1)

        elif tool_name == "search_reference_docs":
            chunks = parsed.get("chunks", parsed.get("results", []))
            n = len(chunks)
            docs = sorted({c.get("file_name", "") for c in chunks})
            st.metric("Chunks found", f"{n}")
            if docs:
                st.caption("From: " + ", ".join(docs[:4]) + ("…" if len(docs) > 4 else ""))
            if chunks:
                with st.expander("Top result", expanded=False):
                    top = chunks[0]
                    st.caption(f"{top.get('file_name', '')} · p{top.get('page_no', '?')} · {top.get('category', '')}")
                    st.markdown(top.get("chunk_text", "")[:500])
            with st.expander("All chunks (raw)", expanded=False):
                st.json(parsed, expanded=1)

        elif tool_name == "ingest_kb_document":
            status = parsed.get("status", "")
            fname = parsed.get("filename", "")
            cat = parsed.get("category", "")
            if status == "ingested":
                st.success(f"Ingested **{fname}** (category: {cat})")
            elif status == "duplicate":
                st.info(f"**{fname}** already in KB (doc_id={parsed.get('doc_id')})")
            else:
                st.warning(parsed.get("message", str(parsed)))

        elif tool_name == "run_pipeline":
            upserted = parsed.get("upserted", 0)
            dates = parsed.get("dates", [])
            errs = parsed.get("errors", [])
            c1, c2, c3 = st.columns(3)
            c1.metric("Rows upserted", upserted)
            c2.metric("Dates", len(dates))
            c3.metric("Errors", len(errs))
            if errs:
                st.warning("\n".join(errs[:3]))
            with st.expander("Raw data", expanded=False):
                st.json(parsed, expanded=1)

        elif tool_name == "get_bess_pnl":
            rows = parsed.get("rows", [])
            n = len(rows)
            assets = sorted({r.get("asset_code", "") for r in rows})
            dates = sorted({r.get("trade_date", "") for r in rows})
            date_range = f"{dates[0]} → {dates[-1]}" if dates else "—"
            scenarios = sorted({r.get("scenario_name", "") for r in rows})
            c1, c2, c3 = st.columns(3)
            c1.metric("Rows", f"{n:,}")
            c2.metric("Assets", len(assets))
            c3.metric("Date range", date_range)
            if assets:
                st.caption("Assets: " + ", ".join(assets))
            if scenarios:
                st.caption("Scenarios: " + ", ".join(scenarios))
            with st.expander("Raw data", expanded=False):
                st.json(parsed, expanded=1)

        else:
            _n = parsed.get("count", parsed.get("rows", ""))
            _n = len(_n) if isinstance(_n, list) else _n
            st.caption(_t("agent_tool_result", n=_n) if isinstance(_n, int) else "")
            st.json(parsed, expanded=1)

    # ── Chat-area file upload ──────────────────────────────────────────────────
    with st.expander("📎 Upload file to knowledge base", expanded=False):
        _chat_uploads = st.file_uploader(
            "Drop a file here to ingest it immediately — then ask the agent about it",
            type=["pdf", "pptx", "ppt", "txt", "docx", "doc",
                  "xlsx", "xls", "png", "jpg", "jpeg", "webp"],
            accept_multiple_files=True,
            key="agent_chat_uploader",
            label_visibility="collapsed",
        )
        if _chat_uploads:
            from services.knowledge_pool.knowledge_docs import register_and_ingest as _rai_chat
            _api_key_up = _os.environ.get("ANTHROPIC_API_KEY")
            _processed = st.session_state.setdefault("_agent_processed_uploads", set())
            _new_files = [
                _f for _f in _chat_uploads
                if f"{_f.name}_{_f.size}" not in _processed
            ]
            if _new_files and st.button(
                f"Ingest {len(_new_files)} file(s)", key="agent_ingest_btn", type="primary"
            ):
                _ingest_results = []
                _prog_up = st.progress(0, text="Ingesting…")
                for _fi, _f in enumerate(_new_files):
                    _prog_up.progress((_fi + 1) / len(_new_files), text=f"{_f.name}…")
                    try:
                        _doc_id, _is_new, _cat = _rai_chat(
                            _f.read(), _f.name, api_key=_api_key_up, app="strategist"
                        )
                        _ingest_results.append((_f.name, _cat, _is_new))
                        _processed.add(f"{_f.name}_{_f.size}")
                    except Exception as _upe:
                        st.error(f"{_f.name}: {_upe}")
                _prog_up.empty()
                if _ingest_results:
                    _summary = "; ".join(
                        f"**{n}** ({c}, {'new' if isnew else 'already indexed'})"
                        for n, c, isnew in _ingest_results
                    )
                    _up_user_msg = f"📎 I uploaded: {_summary}"
                    _up_asst_msg = (
                        f"File(s) ingested into the knowledge base: {_summary}. "
                        "You can now ask me to search or analyse their contents."
                    )
                    st.session_state["agent_display"].append(
                        {"role": "user", "content": _up_user_msg, "tool": None}
                    )
                    st.session_state["agent_messages"].append(
                        {"role": "user", "content": _up_user_msg}
                    )
                    st.session_state["agent_display"].append(
                        {"role": "assistant", "content": _up_asst_msg, "tool": None}
                    )
                    st.session_state["agent_messages"].append(
                        {"role": "assistant", "content": _up_asst_msg}
                    )
                    st.rerun()

    # ── Render existing chat history ───────────────────────────────────────────
    for _msg in st.session_state["agent_display"]:
        if _msg["role"] == "tool":
            _icon = _TOOL_ICONS.get(_msg["tool"], "🔧")
            with st.expander(f"{_icon} {_t('agent_tool_call', tool=_msg['tool'])}", expanded=False):
                _render_tool_result(_msg["tool"], _msg["content"])
        else:
            with st.chat_message(_msg["role"]):
                st.markdown(_msg["content"])

    # ── Chat input ─────────────────────────────────────────────────────────────
    _user_input = st.chat_input(_t("agent_placeholder"), key="agent_input")

    if _user_input:
        st.session_state["agent_display"].append(
            {"role": "user", "content": _user_input, "tool": None}
        )
        with st.chat_message("user"):
            st.markdown(_user_input)

        st.session_state["agent_messages"].append(
            {"role": "user", "content": _user_input}
        )

        with st.chat_message("assistant"):
            _text_ph = st.empty()
            try:
                _reply, _new_msgs, _tool_events = _run_agent_turn(
                    st.session_state["agent_messages"],
                    _build_spot_system(_user_input),
                    text_placeholder=_text_ph,
                )
            except Exception as _exc:
                _reply = _t("agent_error", err=str(_exc))
                _new_msgs = st.session_state["agent_messages"]
                _tool_events = []
                _text_ph.markdown(_reply)

            for _ev in _tool_events:
                st.session_state["agent_display"].append({
                    "role": "tool",
                    "content": _ev["result"],
                    "tool": _ev["tool"],
                })

            st.session_state["agent_messages"] = _new_msgs
            st.session_state["agent_display"].append(
                {"role": "assistant", "content": _reply, "tool": None}
            )

        # ── Auto-save memories + log conversation turn ─────────────────────────
        try:
            _suggestions = _extract_spot_memories(_user_input, _reply)
            for _sug in _suggestions:
                _save_spot_memory(
                    _sug["category"], _sug["subject"], _sug["content"], source="auto"
                )
            if _suggestions:
                st.toast(_t("mem_saved_ok", n=len(_suggestions)))
        except Exception:
            pass

        # ── Extract structured expert insights into kp_expert_insights ─────────
        try:
            from services.knowledge_pool.expert_memory import extract_spot_insights as _ext_insights
            _api_key_ins = _os.environ.get("ANTHROPIC_API_KEY", "")
            if _api_key_ins:
                _n_insights = _ext_insights(_user_input, _reply, _api_key_ins)
        except Exception:
            pass

        # ── Persist session display to DB ──────────────────────────────────────
        try:
            _save_spot_session(
                st.session_state["spot_strat_session_id"],
                st.session_state["agent_display"],
            )
        except Exception:
            pass

        try:
            from services.knowledge_pool.knowledge_docs import log_conversation_turn as _log_turn
            _log_turn(_user_input, _reply)
        except Exception:
            pass

    # ── Knowledge Gap Interview ────────────────────────────────────────────────
    st.divider()
    with st.expander("🎓 Teach the Strategist — Knowledge Gap Interview", expanded=False):
        for _ik in [("interview_questions", []), ("interview_idx", 0),
                    ("interview_answers", 0), ("interview_kb_queried", False),
                    ("interview_kb_results", {}), ("interview_pending_qs", [])]:
            if _ik[0] not in st.session_state:
                st.session_state[_ik[0]] = _ik[1]

        _iq  = st.session_state["interview_questions"]
        _ii  = st.session_state["interview_idx"]
        _pqs = st.session_state["interview_pending_qs"]

        # Stage 0: Generate questions
        if not _iq:
            st.markdown(
                "The agent audits its knowledge base, identifies gaps in China electricity "
                "market expertise, then searches the knowledge pool for answers before "
                "asking you only the questions it couldn't resolve."
            )
            if st.button("Generate Knowledge Gap Questions", key="gen_spot_interview"):
                with st.spinner("Auditing knowledge base and identifying gaps…"):
                    _new_qs = _generate_spot_interview_questions()
                st.session_state["interview_questions"]   = _new_qs
                st.session_state["interview_idx"]         = 0
                st.session_state["interview_answers"]     = 0
                st.session_state["interview_kb_queried"]  = False
                st.session_state["interview_kb_results"]  = {}
                st.session_state["interview_pending_qs"]  = []
                st.rerun()

        # Stage 1: KB search first-pass
        elif not st.session_state["interview_kb_queried"]:
            st.markdown("**Generated knowledge gap questions:**")
            for _qi, _qo in enumerate(_iq):
                st.markdown(f"{_qi+1}. **[{_qo['topic']}]** {_qo['question']}")
                st.caption(f"*Why: {_qo.get('why_asking', '')}*")
            st.divider()
            _col_kb, _col_skip = st.columns([2, 1])
            with _col_kb:
                if st.button("Search KB First", key="interview_kb_search", type="primary"):
                    _kbres = {}
                    with st.spinner("Searching knowledge pool for each gap question…"):
                        for _qo in _iq:
                            _ans = _answer_from_kb(_qo["question"])
                            _kbres[_qo["question"]] = _ans
                            if _ans:
                                try:
                                    _store_spot_interview_answer(
                                        _qo["question"], _ans, _qo["topic"],
                                        confidence="medium",
                                    )
                                except Exception:
                                    pass
                    st.session_state["interview_kb_results"]  = _kbres
                    st.session_state["interview_pending_qs"]  = [
                        _qo for _qo in _iq if not _kbres.get(_qo["question"])
                    ]
                    st.session_state["interview_kb_queried"]  = True
                    st.session_state["interview_idx"]         = 0
                    st.rerun()
            with _col_skip:
                if st.button("Answer Yourself (skip KB)", key="interview_skip_kb"):
                    st.session_state["interview_pending_qs"]  = list(_iq)
                    st.session_state["interview_kb_queried"]  = True
                    st.session_state["interview_idx"]         = 0
                    st.rerun()

        # Stage 3: All questions done — summary
        elif _ii >= len(_pqs):
            _kbres = st.session_state["interview_kb_results"]
            _n_kb  = sum(1 for v in _kbres.values() if v)
            _n_usr = st.session_state["interview_answers"]
            if _n_kb:
                st.success(
                    f"KB answered **{_n_kb}** gap question(s) — stored as medium-confidence insights. "
                    f"You answered **{_n_usr}** additional question(s) as high-confidence insights."
                )
                with st.expander("View KB answers", expanded=False):
                    for _qo in _iq:
                        _ans = _kbres.get(_qo["question"])
                        if _ans:
                            st.markdown(f"**Q: {_qo['question']}**")
                            st.markdown(_ans[:500] + ("…" if len(_ans) > 500 else ""))
                            st.divider()
            else:
                st.success(
                    f"Interview complete — {_n_usr} expert answer(s) stored as high-confidence insights."
                )
            if st.button("Start New Interview", key="new_spot_interview"):
                for _k2 in ["interview_questions", "interview_pending_qs"]:
                    st.session_state[_k2] = []
                st.session_state["interview_kb_results"]  = {}
                st.session_state["interview_idx"]         = 0
                st.session_state["interview_answers"]     = 0
                st.session_state["interview_kb_queried"]  = False
                st.rerun()

        # Stage 2: User Q&A for unanswered questions
        else:
            _q = _pqs[_ii]
            _kbres = st.session_state["interview_kb_results"]
            if _ii == 0 and _kbres:
                _n_auto = sum(1 for v in _kbres.values() if v)
                if _n_auto:
                    st.info(
                        f"KB answered {_n_auto} of {len(_iq)} questions. "
                        f"Please answer the remaining {len(_pqs)}."
                    )
            st.progress(_ii / max(len(_pqs), 1), text=f"Question {_ii + 1} of {len(_pqs)}")
            st.markdown(f"**[{_q['topic']}]** {_q['question']}")
            st.caption(f"*Why this matters: {_q.get('why_asking', '')}*")
            _ans = st.text_area(
                "Your answer:", key=f"spot_interview_ans_{_ii}", height=120,
                placeholder="Share what you know from hands-on experience…",
            )
            _col_sub, _col_ski = st.columns([2, 1])
            with _col_sub:
                if st.button("Submit Answer", key=f"spot_interview_submit_{_ii}", type="primary"):
                    if _ans.strip():
                        try:
                            _store_spot_interview_answer(
                                _q["question"], _ans.strip(), _q["topic"], confidence="high"
                            )
                            st.session_state["interview_idx"]     += 1
                            st.session_state["interview_answers"] += 1
                            st.rerun()
                        except Exception as _e:
                            st.error(f"Failed to store answer: {_e}")
                    else:
                        st.warning("Please enter an answer before submitting.")
            with _col_ski:
                if st.button("Skip", key=f"spot_interview_skip_{_ii}"):
                    st.session_state["interview_idx"] += 1
                    st.rerun()

    # ── Memory management ─────────────────────────────────────────────────────
    with st.expander(f"🗄️ {_t('mem_manage')}", expanded=False):
        st.caption(_t("mem_caption"))
        _mem_df = _load_spot_memories(_SPOT_MEM_KEY)
        if _mem_df.empty:
            st.info(_t("mem_empty"))
        else:
            for _row in _mem_df.itertuples():
                _c1, _c2, _c3 = st.columns([1, 5, 1])
                _c1.markdown(f"**{_row.category}**")
                _c2.markdown(f"**{_row.subject}** — {_row.content}")
                if _c3.button(_t("mem_delete"), key=f"del_spot_mem_{_row.id}"):
                    _delete_spot_memory(_row.id)
                    st.rerun()

    # ── Knowledge Base ─────────────────────────────────────────────────────────
    with st.expander(f"📚 {_t('kb_title')}", expanded=False):
        from services.knowledge_pool.knowledge_docs import (
            init_knowledge_tables as _kb_init,
            register_and_ingest as _kb_ingest,
            list_knowledge_docs as _kb_list,
            delete_knowledge_doc as _kb_delete,
            CATEGORY_LABELS as _KB_CATS,
        )
        if not st.session_state.get("_kp_tables_init_done"):
            try:
                _kb_init()
                st.session_state["_kp_tables_init_done"] = True
            except Exception as _kbi_exc:
                st.warning(f"KB table init: {_kbi_exc}")

        st.caption(_t("kb_caption"))

        # --- KB metrics ---
        try:
            _skbs = _spot_kb_stats()
            _skm1, _skm2, _skm3 = st.columns(3)
            _skm1.metric("📄 Files", f"{_skbs['total']:,}")
            _skm2.metric("🧩 Chunks", f"{_skbs['n_chunks']:,}")
            _skm3.metric("💡 Insights", f"{_skbs['n_insights']:,}")
            st.caption(
                f"**Parse success rate** — {_skbs['parsed']}/{_skbs['total']} files "
                f"parsed without error"
            )
            st.progress(_skbs["parse_pct"], text=f"{_skbs['parse_pct']:.0%}")
        except Exception:
            pass

        _kb_up_tab, _kb_url_tab = st.tabs(["📂 Upload Files", "🌐 Fetch from URL"])

        # ── Upload Files tab ───────────────────────────────────────────────────
        with _kb_up_tab:
            _kb_files = st.file_uploader(
                _t("kb_upload_label"),
                type=["pdf", "pptx", "ppt", "txt", "docx", "doc",
                      "xlsx", "xls", "png", "jpg", "jpeg", "webp"],
                accept_multiple_files=True,
                key="kb_uploader",
            )
            _cat_options = [_t("kb_category_auto")] + list(_KB_CATS.keys())
            _cat_labels  = [_t("kb_category_auto")] + list(_KB_CATS.values())
            _cat_sel_idx = st.selectbox(
                _t("kb_category_label"),
                options=range(len(_cat_options)),
                format_func=lambda i: _cat_labels[i],
                key="kb_cat_sel",
            )
            _cat_override = None if _cat_sel_idx == 0 else _cat_options[_cat_sel_idx]

            if st.button(_t("kb_upload_btn"), key="kb_upload_btn", disabled=not _kb_files):
                _api_key = _os.environ.get("ANTHROPIC_API_KEY")
                _added, _dupes, _errors = 0, [], []
                _new_doc_ids = []
                _prog = st.progress(0, text="Ingesting…")
                for _fi, _f in enumerate(_kb_files):
                    _prog.progress((_fi + 1) / len(_kb_files), text=f"Processing {_f.name}…")
                    _bytes = _f.read()
                    try:
                        _doc_id, _is_new, _cat = _kb_ingest(
                            _bytes, _f.name,
                            category_override=_cat_override,
                            api_key=_api_key,
                        )
                        if _is_new:
                            _added += 1
                            _new_doc_ids.append(_doc_id)
                        else:
                            _dupes.append(_f.name)
                    except Exception as _exc:
                        _errors.append((_f.name, str(_exc)))
                _prog.empty()

                if _added:
                    st.success(_t("kb_success", n=_added))
                    if _api_key and _new_doc_ids:
                        try:
                            from services.knowledge_pool.expert_memory import digest_spot_kb_docs as _digest
                            _n_ins = _digest(_api_key, doc_ids=_new_doc_ids)
                            if _n_ins:
                                st.toast(f"{_n_ins} insight{'s' if _n_ins != 1 else ''} extracted from uploaded document(s)")
                        except Exception:
                            pass
                for _d in _dupes:
                    st.info(_t("kb_duplicate", fname=_d))
                for _fn, _err in _errors:
                    st.error(_t("kb_failed", fname=_fn, err=_err))
                _cached_kb_docs.clear()
                st.rerun()

        # ── Fetch from URL tab ─────────────────────────────────────────────────
        with _kb_url_tab:
            st.caption("Paste a public URL (policy doc, research article, regulator notice). The page text will be extracted and added to the knowledge base.")
            _kb_url_input = st.text_input("URL", placeholder="https://...", key="kb_url_input", label_visibility="collapsed")
            if st.button("Fetch & Add to KB", key="kb_url_fetch_btn", disabled=not _kb_url_input):
                _api_key = _os.environ.get("ANTHROPIC_API_KEY")
                with st.spinner("Fetching and indexing…"):
                    try:
                        from services.knowledge_pool.knowledge_docs import register_url as _kb_register_url
                        _url_doc_id, _url_is_new, _url_cat = _kb_register_url(_kb_url_input.strip(), api_key=_api_key)
                        if _url_is_new:
                            st.success(f"Added to KB (category: {_url_cat})")
                            if _api_key:
                                try:
                                    from services.knowledge_pool.expert_memory import digest_spot_kb_docs as _digest_url
                                    _n_url_ins = _digest_url(_api_key, doc_ids=[_url_doc_id])
                                    if _n_url_ins:
                                        st.toast(f"{_n_url_ins} insight{'s' if _n_url_ins != 1 else ''} extracted")
                                except Exception:
                                    pass
                            _cached_kb_docs.clear()
                            st.rerun()
                        else:
                            st.info("This URL is already in the knowledge base.")
                    except Exception as _url_exc:
                        st.error(f"Fetch failed: {_url_exc}")

        # ── Document list ──────────────────────────────────────────────────────
        _kb_docs = _cached_kb_docs()
        _kb_total = len(_kb_docs)
        _kb_display = _kb_docs[:50]  # cap at 50 rows — rendering all 2000+ docs stalls the page
        st.markdown(
            f"**{_t('kb_doc_list_title')}** "
            + (f"({_kb_total} total, showing latest 50)" if _kb_total > 50 else f"({_kb_total})")
        )
        if not _kb_docs:
            st.info(_t("kb_doc_list_empty"))
        else:
            for _doc in _kb_display:
                _dc1, _dc2, _dc3, _dc4 = st.columns([3, 1, 1, 1])
                _dc1.markdown(
                    f"**{_doc['file_name']}**"
                    + (f"  \n_{_doc['title']}_" if _doc.get('title') and _doc['title'] != _doc['file_name'] else "")
                )
                _dc2.markdown(f"`{_KB_CATS.get(_doc['category'], _doc['category'])}`")
                _dc3.markdown(
                    _t("kb_pages", n=_doc.get('page_count', 0))
                    if _doc.get('ingest_status') == 'parsed'
                    else f"_{_doc.get('ingest_status', '—')}_"
                )
                if _dc4.button(_t("kb_delete"), key=f"kb_del_{_doc['id']}"):
                    _kb_delete(_doc['id'])
                    _cached_kb_docs.clear()
                    st.rerun()

        # ── Batch KB digest ────────────────────────────────────────────────────
        st.divider()
        _col_dig1, _col_dig2 = st.columns([3, 1])
        with _col_dig1:
            st.caption(
                "Extract expert insights from synthesized knowledge base documents "
                "into the insight pool (processes up to 100 undigested docs per run)."
            )
        with _col_dig2:
            if st.button("Digest KB → Insights", key="kb_digest_btn"):
                with st.spinner("Extracting insights from synthesized documents…"):
                    try:
                        from services.knowledge_pool.expert_memory import digest_spot_kb_docs as _digest_batch
                        _n_batch = _digest_batch(
                            _os.environ.get("ANTHROPIC_API_KEY", ""), limit=100
                        )
                        st.success(f"{_n_batch} new insight{'s' if _n_batch != 1 else ''} extracted.")
                    except Exception as _de:
                        st.error(f"Digest failed: {_de}")


# ── Tab 9 helpers — module-level so @st.cache_data can hash them stably ───────
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

_S3_BUCKET = _os.environ.get("UPLOADS_BUCKET", "bess-uploader-data-chen-singp-2026")
_S3_PREFIX = "spot-reports"


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


@st.cache_data(ttl=600, show_spinner=False)
def _scan_pdf_inventory(year: int = 2026):
    data_dir = _REPO / "data" / "spot reports" / str(year)
    pdfs = []

    if data_dir.exists():
        for p in sorted(data_dir.glob("*.pdf")):
            stem = p.stem
            m = _re.search(r"[（(]([^)）]+)[）)]", stem)
            if not m:
                continue
            date_range_result = _parse_pdf_date_range(m.group(1).strip(), year)
            if date_range_result:
                pdfs.append((p.name, date_range_result[0], date_range_result[1], p))
        return pdfs

    # AWS: no local data dir — scan S3
    import boto3 as _boto3
    from botocore.config import Config as _BotoCfg
    s3 = _boto3.client("s3", config=_BotoCfg(connect_timeout=5, read_timeout=15, retries={"max_attempts": 1}))
    prefix = f"{_S3_PREFIX}/{year}/"
    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=_S3_BUCKET, Prefix=prefix):
            for obj in sorted(page.get("Contents", []), key=lambda o: o["Key"]):
                key = obj["Key"]
                fname = key.split("/")[-1]
                if not fname.lower().endswith(".pdf"):
                    continue
                stem = Path(fname).stem
                m = _re.search(r"[（(]([^)）]+)[）)]", stem)
                if not m:
                    continue
                date_range_result = _parse_pdf_date_range(m.group(1).strip(), year)
                if date_range_result:
                    pdfs.append((fname, date_range_result[0], date_range_result[1], key))
    except Exception:
        pass
    return pdfs


@st.cache_data(ttl=300, show_spinner=False)
def _db_coverage(year: int = 2026):
    cur = _conn().cursor()
    cur.execute(
        "SELECT DISTINCT report_date FROM spot_daily "
        "WHERE report_date BETWEEN %s AND %s",
        (date(year, 1, 1), date(year, 12, 31)),
    )
    return {r[0] for r in cur.fetchall()}


@st.cache_data(ttl=300, show_spinner=False)
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


# ── News Sources ──────────────────────────────────────────────────────────────
with tab_news:
    import psycopg2 as _ns_pg2

    # Inline CRUD helpers — avoids importing services.hermes (not in this image)
    def _ns_init_db(pg_url: str) -> None:
        """Create hermes schema + news_sources table if they don't exist."""
        conn = _ns_pg2.connect(pg_url, options="-c statement_timeout=15000")
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS hermes")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS hermes.news_sources (
                        id                  SERIAL PRIMARY KEY,
                        name                TEXT NOT NULL,
                        url                 TEXT NOT NULL,
                        source_type         TEXT NOT NULL DEFAULT 'wechat',
                        biz_id              TEXT,
                        region_bucket       TEXT,
                        category_hint       TEXT,
                        scrape_config       JSONB,
                        active              BOOLEAN NOT NULL DEFAULT TRUE,
                        last_scraped_at     TIMESTAMPTZ,
                        consecutive_failures INT NOT NULL DEFAULT 0,
                        created_at          TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE(name, url)
                    )
                """)
            conn.commit()
        finally:
            conn.close()
    def _ns_get_sources(pg_url: str, active_only: bool = True) -> list:
        conn = _ns_pg2.connect(pg_url, options="-c statement_timeout=10000")
        try:
            with conn.cursor() as cur:
                sql = (
                    "SELECT id, name, url, source_type, biz_id, region_bucket, "
                    "category_hint, active, last_scraped_at, consecutive_failures "
                    "FROM hermes.news_sources"
                    + (" WHERE active = TRUE" if active_only else "")
                    + " ORDER BY name"
                )
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
        finally:
            conn.close()

    def _ns_add_source(pg_url: str, name: str, url: str, source_type: str = "wechat",
                       biz_id=None, region_bucket: str = "全国", category_hint: str = "other") -> dict:
        # Auto-extract biz_id from WeChat article URL if not provided
        if source_type == "wechat" and not biz_id and "mp.weixin.qq.com/s/" in url:
            import re as _re, requests as _rq
            try:
                _r = _rq.get(url, headers={"User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15"}, timeout=20)
                _m = _re.search(r'var\s+biz\s*=\s*"([^"]+)"', _r.text) or _re.search(r'__biz=([A-Za-z0-9=+/]+)', _r.text)
                if _m:
                    biz_id = _m.group(1)
                    url = f"https://mp.weixin.qq.com/mp/profile_ext?action=home&__biz={biz_id}&scene=124"
            except Exception:
                pass
        conn = _ns_pg2.connect(pg_url, options="-c statement_timeout=10000")
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO hermes.news_sources (name, url, source_type, biz_id, region_bucket, category_hint)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (name, url) DO UPDATE SET
                         source_type=EXCLUDED.source_type, biz_id=COALESCE(EXCLUDED.biz_id, hermes.news_sources.biz_id),
                         region_bucket=EXCLUDED.region_bucket, category_hint=EXCLUDED.category_hint, active=TRUE
                       RETURNING id, name, url, source_type, biz_id, region_bucket, category_hint, active""",
                    (name, url, source_type, biz_id, region_bucket, category_hint),
                )
                row = cur.fetchone(); cols = [d[0] for d in cur.description]
            conn.commit()
            return dict(zip(cols, row))
        finally:
            conn.close()

    def _ns_set_active(pg_url: str, source_id: int, active: bool) -> None:
        conn = _ns_pg2.connect(pg_url, options="-c statement_timeout=10000")
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE hermes.news_sources SET active=%s WHERE id=%s", (active, source_id))
            conn.commit()
        finally:
            conn.close()

    def _ns_delete(pg_url: str, source_id: int) -> None:
        conn = _ns_pg2.connect(pg_url, options="-c statement_timeout=10000")
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM hermes.news_sources WHERE id=%s", (source_id,))
            conn.commit()
        finally:
            conn.close()

    _ns_pg_url = _os.environ.get("PGURL", "")
    _ns_hermes_url = _os.environ.get("HERMES_URL", "http://localhost:8000")

    st.subheader(_t("tab_news"))

    # ── Header row: Run Now + Backfill All + Refresh ──────────────────────────
    _ns_col1, _ns_col2, _ns_col3, _ns_col4 = st.columns([1, 1.5, 1, 2])
    with _ns_col1:
        _ns_run = st.button("▶ Run Now", key="ns_run_now", type="primary")
    with _ns_col2:
        _ns_backfill_all = st.button("⏮ Backfill All (2025-01-01)", key="ns_backfill_all")
    with _ns_col3:
        _ns_refresh = st.button("↻ Refresh", key="ns_refresh")

    if _ns_run:
        try:
            import requests as _ns_req
            import urllib3 as _ns_urllib3
            _ns_urllib3.disable_warnings(_ns_urllib3.exceptions.InsecureRequestWarning)
            _ns_resp = _ns_req.post(
                f"{_ns_hermes_url}/hermes/news-screener/run",
                timeout=15,
                verify=False,  # internal ALB call; cert not verifiable from container
            )
            if _ns_resp.ok:
                st.success("✅ Screener started — Feishu digest will arrive at completion.")
            else:
                st.error(f"Hermes returned {_ns_resp.status_code}: {_ns_resp.text[:200]}")
        except Exception as _ns_exc:
            st.error(f"Could not reach Hermes: {_ns_exc}")

    if _ns_backfill_all:
        try:
            import requests as _ns_req_bf2
            import urllib3 as _ns_urllib3_bf2
            _ns_urllib3_bf2.disable_warnings(_ns_urllib3_bf2.exceptions.InsecureRequestWarning)
            _ns_bf2_resp = _ns_req_bf2.post(
                f"{_ns_hermes_url}/hermes/news-screener/backfill",
                json={"start_date": "2025-01-01"},
                timeout=15,
                verify=False,
            )
            if _ns_bf2_resp.ok:
                _ns_bf2_data = _ns_bf2_resp.json()
                st.success(
                    f"⏳ Backfill started for {_ns_bf2_data.get('sources', '?')} sources from 2025-01-01 — "
                    f"Feishu notifications will arrive per-source as they complete."
                )
            else:
                st.error(f"Hermes returned {_ns_bf2_resp.status_code}: {_ns_bf2_resp.text[:200]}")
        except Exception as _ns_bf2_exc:
            st.error(f"Could not reach Hermes: {_ns_bf2_exc}")

    # Last-run timestamp
    if _ns_pg_url:
        try:
            import psycopg2 as _ns_pg
            _ns_conn = _ns_pg.connect(_ns_pg_url, options="-c statement_timeout=5000")
            with _ns_conn.cursor() as _ns_cur:
                _ns_cur.execute(
                    "SELECT MAX(last_scraped_at) FROM hermes.news_sources"
                )
                _ns_last = _ns_cur.fetchone()
            _ns_conn.close()
            if _ns_last and _ns_last[0]:
                _ns_col3.caption(f"Last run: {_ns_last[0].strftime('%Y-%m-%d %H:%M UTC')}")
        except Exception:
            pass

    # ── Sources table ─────────────────────────────────────────────────────────
    if _ns_pg_url:
        try:
            _ns_init_db(_ns_pg_url)
            _ns_sources = _ns_get_sources(_ns_pg_url, active_only=False)
        except Exception as _ns_exc:
            st.error(f"Could not load sources: {_ns_exc}")
            _ns_sources = []

        if _ns_sources:
            st.markdown("**Configured sources**")
            _NS_REGIONS = ["全国", "华北", "华东", "华南", "西北", "西南", "东北"]
            _NS_CATS = ["other", "policy", "market_rules", "market_analytics", "technology", "industry_news"]
            if "ns_editing" not in st.session_state:
                st.session_state["ns_editing"] = None
            for _ns_src in _ns_sources:
                _ns_id = _ns_src["id"]
                _ns_sc1, _ns_sc2, _ns_sc3, _ns_sc4, _ns_sc5, _ns_sc6, _ns_sc7 = st.columns(
                    [3, 1.5, 1.5, 2, 1, 0.6, 0.6]
                )
                _ns_sc1.write(_ns_src["name"])
                _ns_sc2.write(_ns_src.get("source_type", "wechat"))
                _ns_sc3.write(_ns_src.get("region_bucket") or "—")
                _ns_sc4.write(_ns_src.get("category_hint") or "other")
                _ns_active_val = bool(_ns_src.get("active", True))
                _ns_new_active = _ns_sc5.checkbox(
                    "Active", value=_ns_active_val,
                    key=f"ns_active_{_ns_id}", label_visibility="collapsed",
                )
                if _ns_new_active != _ns_active_val:
                    _ns_set_active(_ns_pg_url, _ns_id, _ns_new_active)
                    st.rerun()
                if _ns_sc6.button("✏️", key=f"ns_edit_btn_{_ns_id}", help="Edit"):
                    st.session_state["ns_editing"] = (
                        None if st.session_state["ns_editing"] == _ns_id else _ns_id
                    )
                    st.rerun()
                if _ns_sc7.button("🗑", key=f"ns_del_{_ns_id}"):
                    _ns_delete(_ns_pg_url, _ns_id)
                    if st.session_state.get("ns_editing") == _ns_id:
                        st.session_state["ns_editing"] = None
                    st.rerun()

                # Inline edit form — shown only for the row being edited
                if st.session_state.get("ns_editing") == _ns_id:
                    with st.container():
                        _ec1, _ec2, _ec3, _ec4 = st.columns([3, 2, 2, 1])
                        _ns_e_name = _ec1.text_input(
                            "Name", value=_ns_src["name"], key=f"ns_ename_{_ns_id}"
                        )
                        _ns_e_region = _ec2.selectbox(
                            "Region", _NS_REGIONS,
                            index=_NS_REGIONS.index(_ns_src.get("region_bucket") or "全国"),
                            key=f"ns_eregion_{_ns_id}",
                        )
                        _ns_e_cat = _ec3.selectbox(
                            "Category", _NS_CATS,
                            index=_NS_CATS.index(_ns_src.get("category_hint") or "other"),
                            key=f"ns_ecat_{_ns_id}",
                        )
                        _ec4.write("")  # vertical spacer
                        _ec4.write("")
                        if _ec4.button("Save", key=f"ns_esave_{_ns_id}", type="primary"):
                            _ns_econn = _ns_pg2.connect(_ns_pg_url, options="-c statement_timeout=10000")
                            try:
                                with _ns_econn.cursor() as _ns_ecur:
                                    _ns_ecur.execute(
                                        "UPDATE hermes.news_sources SET name=%s, region_bucket=%s, category_hint=%s WHERE id=%s",
                                        (_ns_e_name, _ns_e_region, _ns_e_cat, _ns_id),
                                    )
                                _ns_econn.commit()
                            finally:
                                _ns_econn.close()
                            st.session_state["ns_editing"] = None
                            st.rerun()
        else:
            st.info("No news sources configured yet. Add one below.")
    else:
        st.warning("PGURL not set — cannot manage sources.")
        _ns_sources = []

    st.divider()

    # ── Add Source ────────────────────────────────────────────────────────────
    with st.expander("➕ Add Source", expanded=len(_ns_sources) == 0):
        _ns_name = st.text_input("Display name", key="ns_name")
        _ns_type = st.selectbox(
            "Type",
            ["wechat", "web", "rss"],
            key="ns_type",
        )
        _ns_url_label = (
            "WeChat article URL (any mp.weixin.qq.com/s/… to auto-extract __biz)"
            if _ns_type == "wechat"
            else "Listing page URL"
        )
        _ns_url = st.text_input(_ns_url_label, key="ns_url")
        _ns_region = st.selectbox(
            "Region hint",
            ["全国", "华北", "华东", "华南", "西北", "西南", "东北"],
            key="ns_region",
        )
        _ns_cat = st.selectbox(
            "Category hint",
            ["other", "policy", "market_rules", "market_analytics", "technology", "industry_news"],
            key="ns_cat",
        )
        if st.button("Add Source", key="ns_add", type="primary"):
            if not _ns_name or not _ns_url:
                st.error("Name and URL are required.")
            elif not _ns_pg_url:
                st.error("PGURL not configured.")
            else:
                try:
                    _ns_new = _ns_add_source(
                        _ns_pg_url,
                        name=_ns_name,
                        url=_ns_url,
                        source_type=_ns_type,
                        region_bucket=_ns_region,
                        category_hint=_ns_cat,
                    )
                    if _ns_type == "wechat" and _ns_new.get("biz_id"):
                        st.success(
                            f"✅ Added **{_ns_name}** (biz_id: `{_ns_new['biz_id']}` auto-extracted)"
                        )
                    else:
                        st.success(f"✅ Added **{_ns_name}**")
                    # Auto-trigger backfill for new source from 2025-01-01
                    try:
                        import requests as _ns_req_bf
                        import urllib3 as _ns_urllib3_bf
                        _ns_urllib3_bf.disable_warnings(_ns_urllib3_bf.exceptions.InsecureRequestWarning)
                        _ns_bf_resp = _ns_req_bf.post(
                            f"{_ns_hermes_url}/hermes/news-screener/backfill",
                            json={"source_id": _ns_new["id"], "start_date": "2025-01-01"},
                            timeout=10,
                            verify=False,
                        )
                        if _ns_bf_resp.ok:
                            st.info("⏳ Backfilling articles since 2025-01-01 in background — you'll get a Feishu notification when done.")
                        else:
                            st.warning(f"Backfill request failed: {_ns_bf_resp.status_code}")
                    except Exception as _ns_bf_exc:
                        st.warning(f"Could not trigger backfill: {_ns_bf_exc}")
                    st.rerun()
                except Exception as _ns_exc:
                    st.error(f"Failed to add source: {_ns_exc}")

# ── Library ───────────────────────────────────────────────────────────────────
with tab_library:
    from services.common.report_library_ui import render_library_tab
    render_library_tab("spot", "China Spot Market", "spot")

# ── Tab 9: Data Management ────────────────────────────────────────────────────
with tab_mgmt:
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

    # ── S3 uploader (AWS only — when no local data folder) ────────────────────
    _local_data = _REPO / "data" / "spot reports" / str(sel_year)
    if not _local_data.exists():
        with st.expander(_t("upload_pdf"), expanded=True):
            st.caption(_t("upload_help"))
            _uploaded = st.file_uploader(
                _t("upload_pdf"),
                type=["pdf"],
                accept_multiple_files=True,
                key=f"mgmt_upload_{sel_year}",
                label_visibility="collapsed",
            )
            if _uploaded:
                if st.button(
                    _t("upload_btn", n=len(_uploaded)),
                    key=f"mgmt_do_upload_{sel_year}",
                    type="primary",
                ):
                    import boto3 as _boto3
                    _s3 = _boto3.client("s3")
                    for _uf in _uploaded:
                        _s3.put_object(
                            Bucket=_S3_BUCKET,
                            Key=f"{_S3_PREFIX}/{sel_year}/{_uf.name}",
                            Body=_uf.read(),
                        )
                    st.success(_t("upload_success", n=len(_uploaded)))
                    _scan_pdf_inventory.clear()
                    st.session_state.pop(f"mgmt_upload_{sel_year}", None)
                    st.rerun()

    st.divider()

    # ── DA / RT Price Data Export ──────────────────────────────────────────────
    with st.expander(f"📥 {_t('export_title')}", expanded=True):
        st.caption(_t("export_caption"))
        _ex_col1, _ex_col2 = st.columns([1, 1])
        _ex_start = _ex_col1.date_input("开始日期 Start", date(sel_year, 1, 1), key="export_start")
        _ex_end   = _ex_col2.date_input("结束日期 End",   min(date(sel_year, 12, 31), date.today()), key="export_end")

        @st.cache_data(ttl=300, show_spinner=False)
        def _build_export_xlsx(_start: str, _end: str) -> bytes:
            import io
            import openpyxl
            from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
            from openpyxl.utils import get_column_letter

            df_exp = pd.read_sql(
                """SELECT report_date, province_cn, da_avg, rt_avg
                   FROM spot_daily
                   WHERE report_date BETWEEN %s AND %s AND da_avg IS NOT NULL
                   ORDER BY report_date, province_cn""",
                _conn(),
                params=(_start, _end),
            )

            if df_exp.empty:
                # return empty workbook
                wb = openpyxl.Workbook()
                wb.active.title = "无数据"
                buf = io.BytesIO()
                wb.save(buf)
                return buf.getvalue()

            # Pivot: rows = date, columns = province_cn
            pivot_da = df_exp.pivot_table(index="report_date", columns="province_cn", values="da_avg")
            pivot_rt = df_exp.pivot_table(index="report_date", columns="province_cn", values="rt_avg")

            # Province column order: sort by name
            provinces = sorted(set(df_exp["province_cn"].dropna()))
            pivot_da = pivot_da.reindex(columns=provinces)
            pivot_rt = pivot_rt.reindex(columns=provinces)

            # Helper: write one sheet
            header_fill = PatternFill("solid", fgColor="1F4E79")
            header_font = Font(bold=True, color="FFFFFF")
            date_fill   = PatternFill("solid", fgColor="D6E4F0")
            thin        = Side(style="thin", color="CCCCCC")
            border      = Border(left=thin, right=thin, top=thin, bottom=thin)

            def _write_sheet(ws, pivot, sheet_title: str, unit_label: str):
                ws.title = sheet_title
                cols = list(pivot.columns)
                # Row 1: title
                ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=len(cols) + 1)
                title_cell = ws.cell(1, 1, f"{sheet_title}（{unit_label}）")
                title_cell.font = Font(bold=True, size=13)
                title_cell.alignment = Alignment(horizontal="center")
                # Row 2: headers
                ws.cell(2, 1, "日期").font = header_font
                ws.cell(2, 1).fill = header_fill
                ws.cell(2, 1).alignment = Alignment(horizontal="center")
                for ci, prov in enumerate(cols, start=2):
                    c = ws.cell(2, ci, prov)
                    c.font = header_font
                    c.fill = header_fill
                    c.alignment = Alignment(horizontal="center")
                # Data rows
                for ri, (dt_idx, row) in enumerate(pivot.iterrows(), start=3):
                    dt_str = dt_idx.strftime("%Y-%m-%d") if hasattr(dt_idx, "strftime") else str(dt_idx)
                    dc = ws.cell(ri, 1, dt_str)
                    dc.fill = date_fill
                    dc.font = Font(bold=True)
                    dc.alignment = Alignment(horizontal="center")
                    for ci, prov in enumerate(cols, start=2):
                        val = row.get(prov)
                        c = ws.cell(ri, ci)
                        if val is not None and not (isinstance(val, float) and val != val):
                            c.value = round(float(val), 4)
                            c.number_format = "0.0000"
                        c.border = border
                        c.alignment = Alignment(horizontal="right")
                # Column widths
                ws.column_dimensions["A"].width = 13
                for ci in range(2, len(cols) + 2):
                    ws.column_dimensions[get_column_letter(ci)].width = 9
                # Freeze header row
                ws.freeze_panes = "B3"

            wb = openpyxl.Workbook()
            _write_sheet(wb.active,    pivot_da, "日前均价",  "元/千瓦时")
            _write_sheet(wb.create_sheet(), pivot_rt, "实时均价", "元/千瓦时")

            buf = io.BytesIO()
            wb.save(buf)
            return buf.getvalue()

        if st.button("生成 Excel / Generate", key="export_generate"):
            with st.spinner("查询数据库…"):
                try:
                    _xlsx_bytes = _build_export_xlsx(str(_ex_start), str(_ex_end))
                    st.download_button(
                        label=f"📥 {_t('export_btn')}",
                        data=_xlsx_bytes,
                        file_name=_t("export_filename"),
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key="export_dl",
                    )
                    st.success("点击上方按钮下载 / Click button above to download")
                except Exception as _ex_exc:
                    st.error(f"Export failed: {_ex_exc}")

    st.divider()

    # ── PDF inventory + gap analysis ──────────────────────────────────────────
    inventory = _scan_pdf_inventory(sel_year)
    try:
        coverage = _db_coverage_detail(sel_year)
    except Exception as _cov_exc:
        st.warning(f"Could not load coverage data: {_cov_exc}")
        coverage = {}
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
                # S3 path is a str key; local path is a _Path object
                if isinstance(path, str):
                    import boto3 as _boto3
                    _tmp_path = Path(f"/tmp/{fname}")
                    _boto3.client("s3").download_file(_S3_BUCKET, path, str(_tmp_path))
                    actual_path = _tmp_path
                    _key_parts = path.split("/")
                    pdf_year = int(_key_parts[1]) if len(_key_parts) > 1 and _key_parts[1].isdigit() else sel_year
                else:
                    actual_path = path
                    pdf_year = int(path.parent.name) if path.parent.name.isdigit() else sel_year
                interprov_count = 0
                ai_count = 0
                try:
                    parsed = _parse_pdf(actual_path, pdf_year, provinces_cn)
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
                        interprov_rows = _parse_interprov(actual_path, pdf_year)
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

    # ── Knowledge Base Sync ────────────────────────────────────────────────────
    st.divider()
    with st.expander(f"📚 {_t('kb_sync_title')}", expanded=False):
        _KB_SYNC_DIR = _REPO / "data" / "market-fundamentals"
        _KB_SYNC_EXCLUDE = "各省现货价格及边界数据"
        _KB_SUPPORTED_EXTS = {
            ".pdf", ".pptx", ".ppt", ".docx", ".doc",
            ".xlsx", ".xls", ".txt",
            ".png", ".jpg", ".jpeg", ".webp",
        }

        st.caption(_t("kb_sync_caption"))

        if not _KB_SYNC_DIR.exists():
            st.info(_t("kb_sync_unavailable"))
        else:
            # Resolve app scope: trader for trading/settlement/frequency-result folders, else shared
            _KBS_TRADER_MARKERS = ("5-交易数据", "交易数据", "电力市场结算情况", "调频结果数据")
            def _kbs_resolve_app(p: Path) -> str:
                p_str = str(p)
                return "trader" if any(m in p_str for m in _KBS_TRADER_MARKERS) else "shared"

            _api_key = _os.environ.get("ANTHROPIC_API_KEY")

            # Scan is deferred to avoid freezing the tab on page load (3000+ files on network drive)
            _kbs_scan_key = "kbs_scanned_files"
            col_check, col_sync = st.columns([1, 1])

            if col_check.button("🔍 Scan for new files", key="kb_sync_scan"):
                _results = []
                for _p in sorted(_KB_SYNC_DIR.rglob("*")):
                    if not _p.is_file(): continue
                    if _p.name.endswith("_Error.txt"): continue
                    if _p.suffix.lower() not in _KB_SUPPORTED_EXTS: continue
                    if _KB_SYNC_EXCLUDE in str(_p): continue
                    _results.append(str(_p))
                st.session_state[_kbs_scan_key] = _results

            _kbs_paths = [Path(p) for p in st.session_state.get(_kbs_scan_key, [])]

            if _kbs_paths:
                st.markdown(f"**{len(_kbs_paths)}** file(s) found under `data/market-fundamentals/`")
                _run_sync = col_sync.button(
                    _t("kb_sync_btn", n=len(_kbs_paths)),
                    type="primary",
                    key="kb_sync_run",
                )
                if _run_sync:
                    from services.knowledge_pool.knowledge_docs import (
                        init_knowledge_tables as _kbs_init,
                        register_and_ingest as _kbs_ingest,
                    )
                    _kbs_init()
                    _kbs_added, _kbs_skipped, _kbs_errors = 0, 0, 0
                    _kbs_total = len(_kbs_paths)
                    _kbs_prog = st.progress(0, text=_t("prog_starting"))
                    for _ki, _kp in enumerate(_kbs_paths):
                        _kbs_prog.progress(
                            _ki / _kbs_total,
                            text=_t("kb_sync_progress", i=_ki + 1, n=_kbs_total, fname=_kp.name),
                        )
                        try:
                            _kbs_app = _kbs_resolve_app(_kp)
                            _, _is_new, _ = _kbs_ingest(
                                file_bytes=_kp.read_bytes(),
                                filename=_kp.name,
                                app=_kbs_app,
                                api_key=_api_key,
                            )
                            if _is_new:
                                _kbs_added += 1
                            else:
                                _kbs_skipped += 1
                        except Exception as _kbs_exc:
                            _kbs_errors += 1
                            st.warning(f"{_kp.name}: {_kbs_exc}")
                    _kbs_prog.progress(1.0, text=_t("prog_done"))
                    st.success(_t("kb_sync_done",
                                   added=_kbs_added, skipped=_kbs_skipped, errors=_kbs_errors))
                    st.session_state.pop(_kbs_scan_key, None)
            else:
                st.caption("Click 'Scan' to check for new files in data/market-fundamentals/")

    # ── WeChat Article Batch Import ───────────────────────────────────────────
    st.divider()
    with st.expander(f"💬 {_t('wechat_title')}", expanded=False):
        st.caption(_t("wechat_caption"))

        _wc_urls_raw = st.text_area(
            _t("wechat_url_label"),
            height=140,
            placeholder=_t("wechat_url_placeholder"),
            key="wechat_urls_input",
        )
        _wc_urls = [u.strip() for u in _wc_urls_raw.splitlines() if u.strip().startswith("http")]

        _wc_col1, _wc_col2 = st.columns([2, 1])
        _wc_category = _wc_col2.selectbox(
            "Category",
            options=["research_report", "market_rules", "policy_doc", "technical_spec", "annual_report", "other"],
            index=0,
            key="wechat_category",
        )
        _wc_app = _wc_col2.selectbox(
            "App scope",
            options=["shared", "strategist", "trader"],
            index=0,
            key="wechat_app_scope",
        )

        _wc_run = _wc_col1.button(
            _t("wechat_run_btn", n=len(_wc_urls)) if _wc_urls else _t("wechat_no_urls"),
            disabled=not _wc_urls,
            type="primary",
            key="wechat_run",
        )

        if _wc_run and _wc_urls:
            import requests as _requests

            def _fetch_wechat(url: str) -> tuple[bytes, str]:
                """Fetch a WeChat article and return (text_bytes, title)."""
                headers = {
                    "User-Agent": (
                        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
                        "AppleWebKit/605.1.15 (KHTML, like Gecko) "
                        "Version/17.0 Mobile/15E148 Safari/604.1"
                    ),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "Referer": "https://mp.weixin.qq.com/",
                }
                resp = _requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(resp.content, "html.parser")
                # Extract title
                title_tag = (
                    soup.find("h1", id="activity-name")
                    or soup.find("h2", class_="rich_media_title")
                    or soup.find("title")
                )
                title = (title_tag.get_text(strip=True) if title_tag else "") or url.split("/s/")[-1][:40]
                # Remove non-content elements
                for _tag in soup(["script", "style", "nav", "footer", "header",
                                   "iframe", "img", "svg"]):
                    _tag.decompose()
                # Try article body first, fall back to full page text
                content_div = (
                    soup.find("div", id="js_content")
                    or soup.find("div", class_="rich_media_content")
                )
                if content_div:
                    text = content_div.get_text(separator="\n")
                else:
                    text = soup.get_text(separator="\n")
                lines = [l.strip() for l in text.splitlines() if l.strip()]
                return "\n".join(lines).encode("utf-8"), title

            from services.knowledge_pool.knowledge_docs import (
                init_knowledge_tables as _wc_init,
                register_and_ingest as _wc_ingest,
            )
            _wc_init()

            _wc_added, _wc_skipped, _wc_errors = 0, 0, 0
            _wc_total = len(_wc_urls)
            _wc_prog = st.progress(0, text=_t("prog_starting"))
            _wc_api_key = _os.environ.get("ANTHROPIC_API_KEY")
            _wc_status_lines = []

            for _wi, _wu in enumerate(_wc_urls):
                _wc_prog.progress(
                    _wi / _wc_total,
                    text=_t("wechat_fetching", i=_wi + 1, n=_wc_total, url=_wu[:60]),
                )
                try:
                    _wc_bytes, _wc_title = _fetch_wechat(_wu)
                    _safe_title = _wc_title[:120].replace("/", "_").replace("\\", "_")
                    _wc_fname = f"{_safe_title}.txt"
                    _, _wc_is_new, _wc_cat = _wc_ingest(
                        file_bytes=_wc_bytes,
                        filename=_wc_fname,
                        category_override=_wc_category,
                        app=_wc_app,
                        api_key=_wc_api_key,
                        synthesize=False,
                    )
                    if _wc_is_new:
                        _wc_added += 1
                        _wc_status_lines.append(f"✅ {_wc_title[:80]}")
                    else:
                        _wc_skipped += 1
                        _wc_status_lines.append(f"⏭️ already indexed: {_wc_title[:80]}")
                except Exception as _wc_exc:
                    _wc_errors += 1
                    _wc_status_lines.append(f"❌ {_wu[:60]} — {_wc_exc}")

            _wc_prog.progress(1.0, text=_t("prog_done"))
            st.success(_t("wechat_done", added=_wc_added, skipped=_wc_skipped, errors=_wc_errors))
            for _sl in _wc_status_lines:
                st.markdown(_sl)

            # Offer immediate KB digest
            if _wc_added > 0:
                if st.button(_t("wechat_digest_btn"), key="wechat_digest_now"):
                    from services.knowledge_pool.expert_memory import digest_spot_kb_docs as _wc_digest
                    _wc_api_key2 = _os.environ.get("ANTHROPIC_API_KEY")
                    if _wc_api_key2:
                        with st.spinner("Digesting new articles into insights…"):
                            _wc_n = _wc_digest(api_key=_wc_api_key2)
                        st.toast(f"Extracted {_wc_n} insight(s) from new articles.")
                    else:
                        st.warning("ANTHROPIC_API_KEY not set — cannot digest.")

    # ── Daily Market Report ───────────────────────────────────────────────────
    st.divider()
    with st.expander(f"📧 {_t('report_section_title')}", expanded=False):
        st.caption(_t("report_section_caption"))

        # Scheduler status
        try:
            _rpt_sched = _start_spot_scheduler()
            if _rpt_sched is not None:
                _rpt_jobs = _rpt_sched.get_jobs()
                _rpt_job  = next((j for j in _rpt_jobs if j.id == "spot_daily_report"), None)
                if _rpt_job and _rpt_job.next_run_time:
                    st.info(
                        f"⏰ {_t('report_schedule_status')}: running · "
                        f"{_t('report_next_run')}: "
                        f"{_rpt_job.next_run_time.strftime('%Y-%m-%d %H:%M %Z')}"
                    )
                else:
                    st.warning("Scheduler running but no next run time found.")
            else:
                st.warning("APScheduler not available — install `apscheduler`.")
        except Exception as _sched_exc:
            st.warning(f"Scheduler status unavailable: {_sched_exc}")

        st.markdown(f"**{_t('report_webhook_title')}**")
        st.caption(_t("report_webhook_caption"))

        # Module is loaded once per process via @st.cache_resource
        try:
            _rpt_mod = _get_spot_report_mod()
        except Exception as _wt_exc:
            st.warning(f"Could not load spot_report module: {_wt_exc}")
            _rpt_mod = None

        # ── Existing webhooks (cached — no DB hit on every rerun) ─────────────
        _wh_rows = _cached_webhooks()
        if _wh_rows:
            for _wh in _wh_rows:
                _wh_col1, _wh_col2, _wh_col3, _wh_col4 = st.columns([3, 2, 1, 1])
                _wh_col1.text(_wh.get("webhook_url", "")[:60])
                _wh_col2.text(_wh.get("label", ""))
                _enabled_now = _wh_col3.checkbox(
                    _t("report_webhook_enabled"),
                    value=bool(_wh.get("enabled", True)),
                    key=f"wh_enabled_{_wh['id']}",
                    label_visibility="collapsed",
                )
                if _enabled_now != bool(_wh.get("enabled", True)):
                    if _rpt_mod:
                        _rpt_mod.upsert_webhook(
                            _wh["webhook_url"], _wh.get("label", ""), _enabled_now
                        )
                    _cached_webhooks.clear()
                    st.rerun()
                if _wh_col4.button(_t("report_webhook_delete"),
                                   key=f"wh_del_{_wh['id']}"):
                    if _rpt_mod:
                        _rpt_mod.delete_webhook(_wh["id"])
                    _cached_webhooks.clear()
                    st.rerun()
        else:
            st.caption(_t("report_webhook_empty"))

        # ── Add new webhook ───────────────────────────────────────────────────
        with st.form("add_webhook_form", clear_on_submit=True):
            _new_wh_url = st.text_input(
                _t("report_webhook_add_label"),
                placeholder="https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...",
                type="password",
            )
            _new_wh_label = st.text_input(
                _t("report_webhook_add_label_label"),
                placeholder="e.g. Trading Team",
            )
            if st.form_submit_button(_t("report_webhook_add_btn"), type="primary"):
                if _new_wh_url and _new_wh_url.startswith("http") and _rpt_mod:
                    _rpt_mod.upsert_webhook(_new_wh_url, _new_wh_label, True)
                    _cached_webhooks.clear()
                    st.success(_t("report_webhook_added"))
                    st.rerun()
                else:
                    st.error("Please enter a valid webhook URL.")

        st.divider()

        # ── Send Now ──────────────────────────────────────────────────────────
        @st.cache_data(ttl=600, show_spinner=False)
        def _last_rt_date():
            try:
                import pandas as _pd2
                _df = _pd2.read_sql(
                    "SELECT MAX(report_date) AS d FROM spot_daily WHERE rt_avg IS NOT NULL",
                    _conn(),
                )
                _v = _df.iloc[0]["d"]
                if _v is not None and not _pd2.isna(_v):
                    return _pd2.Timestamp(_v).date()
            except Exception:
                pass
            import datetime as _dt2
            return _dt2.date.today() - _dt2.timedelta(days=1)

        import datetime as _rdt
        _rpt_default_date = _last_rt_date()
        _rpt_date = st.date_input(
            "Report date",
            value=_rpt_default_date,
            max_value=_rdt.date.today(),
            key="report_date_pick",
        )
        _rpt_email_default = _os.environ.get("REPORT_TO_EMAIL", _DEFAULT_RECIPIENT)
        _rpt_email = st.text_input(
            _t("report_email_label"),
            value=_rpt_email_default,
            help=_t("report_email_help"),
            key="report_email_override",
        )
        if st.button(_t("report_send_now"), type="primary", key="report_send_now_btn"):
            if _rpt_mod is not None:
                with st.spinner("Generating and sending report…"):
                    try:
                        _rpt_result = _rpt_mod.run_daily_report(
                            to_email=_rpt_email or None,
                            report_date=_rpt_date,
                        )
                        if _rpt_result["status"] == "success":
                            st.success(
                                _t("report_send_success",
                                   size=_rpt_result.get("size_bytes", 0),
                                   rdate=_rpt_result.get("date", "?"))
                            )
                            st.info(_t("report_send_wecom", result=_rpt_result.get("wecom", "skipped")))
                        else:
                            st.error(_t("report_send_error", err=_rpt_result.get("error", "?")))
                    except Exception as _rpt_exc:
                        st.error(_t("report_send_error", err=str(_rpt_exc)))
            else:
                st.error("spot_report module not available.")
