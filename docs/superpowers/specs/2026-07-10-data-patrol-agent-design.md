# Data Patrol Agent — Design Spec

**Date:** 2026-07-10  
**Status:** Approved  
**Scope:** `services/hermes/data_patrol.py` · `services/hermes/app.py` · `services/hermes/capcomp_manual_etl.py` · `apps/bess-map/app.py`

---

## 1. Purpose

A scheduled and on-demand data patrol agent that queries all active data sources across the BESS platform, identifies staleness and gaps, and delivers a tiered Feishu report. For manual-upload gaps it sends a separate upload reminder. For capcomp / FR market / installed capacity / sysopfee / 代理购电 gaps it offers inline fill — either by typing values directly or by sending a file (PDF, Excel, JPG, PNG, DOCX, PPT, TXT) and having AI extract the values.

---

## 2. Architecture

### 2.1 New file

**`services/hermes/data_patrol.py`** (~400 lines)

```
run_patrol(pg_url, feishu, owner_open_id, api_key) → PatrolReport
  ├─ check_auto_pipelines(pg_url) → list[SourceStatus]
  ├─ check_manual_uploads(pg_url) → list[SourceStatus]
  ├─ check_monthly_data(pg_url)   → list[SourceStatus]
  ├─ check_kb_activity(pg_url)    → list[KBSummary]
  ├─ build_summary_card(report)   → dict  (Feishu interactive card)
  ├─ build_detail_card(report)    → dict  (expanded inline card)
  └─ send_upload_reminders(report, feishu, owner_open_id)
```

`SourceStatus` dataclass:
```python
@dataclass
class SourceStatus:
    name: str            # display name
    table: str           # DB table or path
    last_date: date | None
    days_behind: int
    status: Literal["fresh", "stale", "missing"]
    group: Literal["auto", "manual", "monthly"]
    reminder_text: str = ""   # non-empty for manual uploads
    fill_table: str = ""      # non-empty for inline fill targets
    fill_province: str = ""
    fill_month: str = ""
```

### 2.2 Modified files

**`services/hermes/app.py`**
- Scheduler: add job at 00:35 UTC (08:35 Beijing) daily calling `run_patrol`
- Command router: handle `/datacheck`, `/巡视` → call `run_patrol` immediately
- New endpoint `POST /hermes/patrol` — programmatic trigger (used by bess-map)
- New endpoint `POST /hermes/patrol/fill` — receives inline fill submissions
- New session dict `_pending_gap_fill: dict[str, dict]` — tracks sender → `{table, province, month}` while user is in file-upload fill flow

**`services/hermes/capcomp_manual_etl.py`**
- Extend `_load_file_text()` to handle DOCX (python-docx), PPT/PPTX (python-pptx text dump), TXT (direct decode) — PDF and Excel already handled
- Add `extract_from_file_for_gap(file_bytes, filename, table, province, month, api_key)` — routes to the correct extractor based on `table` (capcomp / fr_market / installed / sysopfee / daili)

**`apps/bess-map/app.py`**
- Cap Comp + FR Market tab: add `st.expander("📋 Data Gaps")` below existing tables
- Inside expander: heatmap grid (provinces × months, last 12 months) showing ✅ / 🔴 per table
- Clicking a 🔴 cell shows a small form → submits to `POST /hermes/patrol/fill`

---

## 3. Data Sources Monitored

### Group A — Auto pipelines (daily; flag if > 2 days behind)

| Source | Table | Date column |
|--------|-------|-------------|
| LingFeng fundamentals (29 provinces) | `marketdata.spot_fundamentals_hourly` | `datetime` |
| LingFeng prices (29 provinces) | `marketdata.spot_prices_hourly` | `datetime` |
| Fengxing nodal prices | `marketdata.md_shanxi_nodal_price_96` + siblings | `data_date` |
| Canon intraday cleared | `marketdata.md_id_cleared_energy` | `data_date` |
| Canon day-ahead cleared | `marketdata.md_da_cleared_energy` | `data_date` |
| Canon RT nodal price | `marketdata.md_rt_nodal_price` | `data_date` |
| Mengxi hist_* (RT clear, wind, solar, load, bidding space) | `public.hist_mengxi_*` | `time` |
| BESS capture daily | `marketdata.bess_capture_daily` | date col |
| GB Elexon settlement | `intl_market.gb_elexon_sp` | `settlement_date` |
| GB wind forecast | `intl_market.gb_wind_forecast` | `start_time` |

For LingFeng and Mengxi hist_* tables, check returns per-province breakdown of any provinces > 2 days behind (not just overall max date).

### Group B — Manual uploads (daily; flag if > 2 days behind)

| Source | Table | Reminder message |
|--------|-------|-----------------|
| Spot daily ops PDF (现货价格运行日报) | `spot_daily` | "请上传 电力现货市场价格与运行日报-YYYYMMDD.pdf" |

Sends as a separate Feishu text message (not just a card indicator) when stale.

### Group C — Monthly data (flag if current month missing by 10th of following month)

| Source | Table | Fill supported |
|--------|-------|---------------|
| Exchange monthly reports (29 provinces) | `staging.exchange_monthly_reports` | No (file upload only via Hermes) |
| Capacity compensation | `marketdata.province_cap_comp` | Yes — inline fill + file AI extract |
| FR market | `marketdata.province_fr_market` | Yes — inline fill + file AI extract |
| Installed capacity | `province_installed_monthly` | Yes — inline fill + file AI extract |
| System operation fee | `province_sysopfee_monthly` | Yes — inline fill + file AI extract |
| 代理购电 price | `province_sysopfee_monthly` (same table, separate rows) | Yes — inline fill + file AI extract |

### Group D — KB ad-hoc (count only; shown on Monday and 1st of month)

| Source | Table | Shown |
|--------|-------|-------|
| Spot KB documents | `staging.spot_knowledge_docs` | Weekly count (Mon) + monthly count (1st) |
| GB KB documents | `intl_market.gb_knowledge_docs` | Same |
| AU KB documents | `intl_market.au_knowledge_docs` | Same |
| PH KB documents | `intl_market.ph_knowledge_docs` | Same |
| PO KB documents | `intl_market.po_knowledge_docs` | Same |
| News items ingested | `staging.spot_knowledge_docs` (source_type=news) + screener config in `hermes.news_sources` | Last screener run timestamp + weekly count |

---

## 4. Feishu Card Design

### 4.1 Summary card (initial, always shown)

```
📡 数据巡视报告 — 2026-07-10 周四 08:35

✅ 自动管道      10/10 正常
⚠️ 手动上传      1 项需关注
🔴 月度数据      2 项缺失
📊 知识库        本周新增 12 篇

[展开详情 ▼]   [关闭]
```

Header template: `orange` if any stale/missing, `green` if all fresh.

### 4.2 Detail card (replaces summary in-place on tap)

One section per group. Each source row:
```
[🔴 spot_daily]  最后日期: 2026-07-07 · 落后 3 天   [上传提醒]
[✅ gb_elexon]   最后日期: 2026-07-09 · 正常
[🔴 province_cap_comp / 山东 / 2026-06]  [填入数据]
```

`[上传提醒]` button → triggers separate Feishu text message with filename format.  
`[填入数据]` button → sends an input card (see §4.3).

### 4.3 Gap fill flow

**Step 1** — Feishu sends fill card:
```
填写缺失数据 — 容量补偿 / 山东 / 2026-06

省份: 山东 (locked)
生效日期: 2026-06-01
容量补偿标准 (¥/kW): [___]
峰值时段 (h): [___]
备注: [___]

[提交]    [发文件给我，AI自动提取]
```

**Step 2a — Manual submit**: POST to `/hermes/patrol/fill` with values → upsert → confirmation card.

**Step 2b — File upload**: Sets `_pending_gap_fill[sender_id] = {table, province, month}`. User sends file in chat. Hermes receives via existing file handler, detects pending gap fill context, calls `extract_from_file_for_gap()`. Shows extracted values in a confirmation card with **[确认提交]** / **[修改后提交]** / **[取消]** buttons. On confirm → upsert.

Supported file types for AI extraction: PDF, XLSX/XLS, JPG, PNG, DOCX, PPTX, TXT.

---

## 5. bess-map Cap Comp + FR Market Tab — Gap Display

Add `st.expander("📋 Data Gaps — 容量补偿 / 调频市场 / 装机容量")` at the bottom of `tab_aux`.

Inside:
- Three sub-tabs: **容量补偿**, **调频市场**, **装机容量**
- Each: a province × month heatmap (last 12 months) using Plotly `imshow` with discrete colour scale (green = has data, red = missing, grey = N/A)
- Below heatmap: a simple form — select province + month + enter value → POST to `{HERMES_URL}/hermes/patrol/fill`
- On success: `st.success()` + `st.cache_data.clear()` + `st.rerun()`

---

## 6. Scheduling

| Job | Cron (UTC) | Beijing time |
|-----|-----------|--------------|
| Daily patrol | `35 0 * * *` | 08:35 daily |
| (on-demand) | `/datacheck` or `/巡视` command | immediate |

Patrol runs after morning briefing (00:03) and health check (00:10) to avoid DB contention.

---

## 7. New HTTP Endpoints in Hermes

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/hermes/patrol` | Trigger patrol, returns `PatrolReport` JSON |
| POST | `/hermes/patrol/fill` | Upsert a single gap fill row |
| GET | `/hermes/patrol/status` | Last patrol result (cached in memory) |

---

## 8. Error Handling

- Each `check_*()` function catches DB exceptions independently — a failure in one group does not block others
- If DB is unavailable entirely, send a short text message: "⚠️ 数据巡视失败 — 数据库暂时不可达"
- File AI extraction failures fall back to: show extracted partial values with a warning, or prompt user to type manually

---

## 9. Out of Scope

- IM BESS ops Excel — migrating to Risk Management app, not included
- AU/PH/PO live price data staleness — KB doc count only (Group D)
- Alerting to WeChat/Telegram — Feishu only for patrol report (existing channels unaffected)
