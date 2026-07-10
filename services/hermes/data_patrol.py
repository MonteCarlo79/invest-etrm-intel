# services/hermes/data_patrol.py
"""
Data Patrol Agent
=================
Checks all platform data sources for staleness/gaps and delivers
a tiered Feishu report. Follows the pattern of news_screener.py.

Entry point:
    run_patrol(pg_url, feishu, owner_open_id, api_key) -> PatrolReport
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Literal, Optional

logger = logging.getLogger(__name__)

_BJ = timezone(timedelta(hours=8))
_DAILY_STALE_DAYS = 2       # flag auto/manual daily data if > 2 days behind
_MONTHLY_FLAG_DAY = 10      # flag missing monthly data after 10th of following month
_MISSING_SENTINEL = 9999   # sentinel value for "no data at all"


@dataclass
class SourceStatus:
    name: str
    table: str
    last_date: Optional[date]
    days_behind: int
    status: Literal["fresh", "stale", "missing"]
    group: Literal["auto", "manual", "monthly"]
    reminder_text: str = ""   # non-empty → send separate upload reminder
    fill_table: str = ""      # non-empty → show 填入数据 button in detail card
    fill_province: str = ""
    fill_month: str = ""      # YYYY-MM


@dataclass
class KBSummary:
    name: str
    table: str
    last_ingested: Optional[date]
    count_7d: int
    count_30d: int


@dataclass
class PatrolReport:
    sources: list[SourceStatus]
    kb_summaries: list[KBSummary]
    generated_at: datetime = field(default_factory=lambda: datetime.now(_BJ))

    def count_by_status(self, status: str) -> int:
        return sum(1 for s in self.sources if s.status == status)

    def by_group(self, group: str) -> list[SourceStatus]:
        return [s for s in self.sources if s.group == group]

    @property
    def has_alerts(self) -> bool:
        return any(s.status in ("stale", "missing") for s in self.sources)


def _days_behind(last_date: Optional[date], today: Optional[date] = None) -> int:
    if last_date is None:
        return _MISSING_SENTINEL
    if today is None:
        today = datetime.now(_BJ).date()
    return max(0, (today - last_date).days)


def _classify_daily(days: int) -> Literal["fresh", "stale", "missing"]:
    if days == _MISSING_SENTINEL:
        return "missing"
    if days > _DAILY_STALE_DAYS:
        return "stale"
    return "fresh"


# ── Group A: Auto pipeline freshness checks ───────────────────────────────────

_AUTO_SOURCES = [
    # (display_name, table, date_col, extra_where)
    ("LingFeng 基本面 (29省)", "marketdata.spot_fundamentals_hourly", "datetime::date", ""),
    ("LingFeng 现货价格 (29省)", "marketdata.spot_prices_hourly", "datetime::date", ""),
    ("Canon 日内出清", "marketdata.md_id_cleared_energy", "data_date", ""),
    ("Canon 日前出清", "marketdata.md_da_cleared_energy", "data_date", ""),
    ("Canon RT节点电价", "marketdata.md_rt_nodal_price", "data_date", ""),
    ("BESS捕获率日数据", "marketdata.bess_capture_daily", "trade_date", ""),
    ("GB Elexon结算", "intl_market.gb_elexon_sp", "settlement_date", ""),
    ("GB风电预测", "intl_market.gb_wind_forecast", "start_time::date", ""),
]

_MENGXI_HIST_TABLES = [
    "hist_mengxi_provincerealtimeclearprice_15min",
    "hist_mengxi_newenergyreal_15min",
    "hist_mengxi_windpowerreal_15min",
    "hist_mengxi_solarpowerreal_15min",
    "hist_mengxi_loadregulationreal_15min",
    "hist_mengxi_biddingspacereal_15min",
]

_FENGXING_TABLES = [
    ("marketdata.md_shanxi_nodal_price_96", "data_date"),
]


def check_auto_pipelines(pg_url: str) -> list[SourceStatus]:
    """Query max date for each auto-scheduled data source."""
    import psycopg2
    results: list[SourceStatus] = []

    def _query_max(cur, table: str, date_col: str, extra: str = "") -> Optional[date]:
        try:
            where = f"WHERE {extra}" if extra else ""
            cur.execute(f"SELECT MAX({date_col}) FROM {table} {where}")
            row = cur.fetchall()
            val = row[0][0] if row else None
            if val is None:
                return None
            return val.date() if hasattr(val, "date") else val
        except Exception as exc:
            logger.warning("patrol query failed for %s: %s", table, exc)
            return None

    try:
        conn = psycopg2.connect(pg_url)
        with conn:
            with conn.cursor() as cur:
                # Standard auto sources
                for name, table, date_col, extra in _AUTO_SOURCES:
                    last = _query_max(cur, table, date_col, extra)
                    days = _days_behind(last)
                    results.append(SourceStatus(
                        name=name, table=table, last_date=last,
                        days_behind=days, status=_classify_daily(days), group="auto",
                    ))

                # Mengxi hist tables (report as group — show worst)
                mengxi_dates = []
                for tbl in _MENGXI_HIST_TABLES:
                    d = _query_max(cur, f"public.{tbl}", "time::date")
                    mengxi_dates.append(d)
                mengxi_last = min((d for d in mengxi_dates if d), default=None)  # worst case
                days = _days_behind(mengxi_last)
                results.append(SourceStatus(
                    name="蒙西 hist_* 实时数据", table="public.hist_mengxi_*",
                    last_date=mengxi_last, days_behind=days,
                    status=_classify_daily(days), group="auto",
                ))

                # Fengxing nodal tables
                for table, date_col in _FENGXING_TABLES:
                    last = _query_max(cur, table, date_col)
                    days = _days_behind(last)
                    results.append(SourceStatus(
                        name=f"丰行节点电价 ({table.split('.')[-1]})",
                        table=table, last_date=last, days_behind=days,
                        status=_classify_daily(days), group="auto",
                    ))
        conn.close()
    except Exception as exc:
        logger.error("check_auto_pipelines: DB unavailable: %s", exc)
        # Return all missing on total DB failure
        for name, table, _, _ in _AUTO_SOURCES:
            results.append(SourceStatus(
                name=name, table=table, last_date=None,
                days_behind=_MISSING_SENTINEL, status="missing", group="auto",
            ))
    return results


# ── Group B: Manual upload checks ────────────────────────────────────────────

def check_manual_uploads(pg_url: str) -> list[SourceStatus]:
    """Check manually-uploaded daily data sources."""
    import psycopg2
    results: list[SourceStatus] = []
    try:
        conn = psycopg2.connect(pg_url)
        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT MAX(report_date) FROM spot_daily")
                row = cur.fetchall()
                last = row[0][0] if row and row[0][0] else None
                if last and hasattr(last, "date"):
                    last = last.date()
                days = _days_behind(last)
                today = datetime.now(_BJ).date()
                reminder = (
                    f"请上传 电力现货市场价格与运行日报-{today.strftime('%Y%m%d')}.pdf"
                    if days > _DAILY_STALE_DAYS else ""
                )
                results.append(SourceStatus(
                    name="现货日报 PDF",
                    table="spot_daily",
                    last_date=last,
                    days_behind=days,
                    status=_classify_daily(days),
                    group="manual",
                    reminder_text=reminder,
                ))
        conn.close()
    except Exception as exc:
        logger.error("check_manual_uploads: %s", exc)
        results.append(SourceStatus(
            name="现货日报 PDF", table="spot_daily",
            last_date=None, days_behind=_MISSING_SENTINEL, status="missing", group="manual",
            reminder_text="请上传 电力现货市场价格与运行日报.pdf",
        ))
    return results


# ── Group C: Monthly data checks ─────────────────────────────────────────────

_MONTHLY_FILL_SOURCES = [
    # (display_name, table, year_month_expr, fill_table)
    ("容量补偿", "marketdata.province_cap_comp",
     "EXTRACT(year FROM effective_date)::int * 100 + EXTRACT(month FROM effective_date)::int",
     "province_cap_comp"),
    ("调频市场", "marketdata.province_fr_market",
     "EXTRACT(year FROM effective_date)::int * 100 + EXTRACT(month FROM effective_date)::int",
     "province_fr_market"),
    ("储能装机容量", "province_installed_monthly", "year_month", "province_installed_monthly"),
    ("系统运行费", "province_sysopfee_monthly", "year_month", "province_sysopfee_monthly"),
]

_EXCHANGE_REPORT_PROVINCES = 29


def check_monthly_data(pg_url: str) -> list[SourceStatus]:
    """Check monthly data tables for prior-month coverage."""
    import psycopg2
    results: list[SourceStatus] = []
    now = datetime.now(_BJ)
    today = now.date()

    if today.month == 1:
        target_year, target_month = today.year - 1, 12
    else:
        target_year, target_month = today.year, today.month - 1

    flag_date = date(today.year, today.month, _MONTHLY_FLAG_DAY)
    should_flag = today >= flag_date

    try:
        conn = psycopg2.connect(pg_url)
        with conn:
            with conn.cursor() as cur:
                for name, table, ym_expr, fill_table in _MONTHLY_FILL_SOURCES:
                    try:
                        target_ym = target_year * 100 + target_month
                        cur.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE {ym_expr} = %s",
                            (target_ym,)
                        )
                        row = cur.fetchall()
                        count = row[0][0] if row else 0
                        status = "missing" if (count == 0 and should_flag) else "fresh"
                        results.append(SourceStatus(
                            name=f"{name} ({target_year}-{target_month:02d})",
                            table=table,
                            last_date=None if count == 0 else date(target_year, target_month, 1),
                            days_behind=0 if status == "fresh" else 30,
                            status=status,
                            group="monthly",
                            fill_table=fill_table if status == "missing" else "",
                            fill_month=f"{target_year}-{target_month:02d}" if status == "missing" else "",
                        ))
                    except Exception as exc:
                        logger.warning("monthly check failed for %s: %s", table, exc)

                # Exchange monthly reports
                try:
                    target_ym_str = f"{target_year}-{target_month:02d}"
                    cur.execute(
                        "SELECT COUNT(DISTINCT province) FROM staging.exchange_monthly_reports "
                        "WHERE TO_CHAR(report_month, 'YYYY-MM') = %s",
                        (target_ym_str,)
                    )
                    row = cur.fetchall()
                    found = row[0][0] if row else 0
                    missing = _EXCHANGE_REPORT_PROVINCES - found
                    status = "missing" if (missing > 0 and should_flag) else "fresh"
                    results.append(SourceStatus(
                        name=f"交易所月报 ({target_ym_str}, {found}/{_EXCHANGE_REPORT_PROVINCES}省)",
                        table="staging.exchange_monthly_reports",
                        last_date=None if found == 0 else date(target_year, target_month, 1),
                        days_behind=0 if status == "fresh" else 30,
                        status=status,
                        group="monthly",
                    ))
                except Exception as exc:
                    logger.warning("exchange reports check failed: %s", exc)
        conn.close()
    except Exception as exc:
        logger.error("check_monthly_data: DB unavailable: %s", exc)
    return results


# ── Group D: KB activity ──────────────────────────────────────────────────────

_KB_TABLES = [
    ("Spot KB", "staging.spot_knowledge_docs", "created_at"),
    ("GB KB", "intl_market.gb_knowledge_docs", "created_at"),
    ("AU KB", "intl_market.au_knowledge_docs", "created_at"),
    ("PH KB", "intl_market.ph_knowledge_docs", "created_at"),
    ("PO KB", "intl_market.po_knowledge_docs", "created_at"),
]


def check_kb_activity(pg_url: str) -> list[KBSummary]:
    """Count KB docs ingested in last 7 and 30 days."""
    import psycopg2
    results: list[KBSummary] = []
    try:
        conn = psycopg2.connect(pg_url)
        with conn:
            with conn.cursor() as cur:
                for name, table, ts_col in _KB_TABLES:
                    try:
                        cur.execute(f"""
                            SELECT
                                MAX({ts_col})::date,
                                COUNT(*) FILTER (WHERE {ts_col} >= NOW() - INTERVAL '7 days'),
                                COUNT(*) FILTER (WHERE {ts_col} >= NOW() - INTERVAL '30 days')
                            FROM {table}
                        """)
                        row = cur.fetchall()
                        if row and row[0][0] is not None:
                            last_raw = row[0][0]
                            last = last_raw.date() if hasattr(last_raw, "date") else last_raw
                            results.append(KBSummary(
                                name=name, table=table, last_ingested=last,
                                count_7d=int(row[0][1] or 0),
                                count_30d=int(row[0][2] or 0),
                            ))
                        else:
                            results.append(KBSummary(
                                name=name, table=table, last_ingested=None,
                                count_7d=0, count_30d=0,
                            ))
                    except Exception as exc:
                        logger.warning("KB activity check failed for %s: %s", table, exc)
        conn.close()
    except Exception as exc:
        logger.error("check_kb_activity: DB unavailable: %s", exc)
    return results


# ── Feishu card builders ──────────────────────────────────────────────────────

_STATUS_ICON = {"fresh": "✅", "stale": "⚠️", "missing": "🔴"}
_WEEKDAYS_CN = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]


def build_summary_card(report: PatrolReport) -> dict:
    now = report.generated_at
    date_str = f"{now.year}年{now.month}月{now.day}日 {_WEEKDAYS_CN[now.weekday()]}"

    auto_sources = report.by_group("auto")
    manual_sources = report.by_group("manual")
    monthly_sources = report.by_group("monthly")

    auto_fresh = sum(1 for s in auto_sources if s.status == "fresh")
    manual_issues = sum(1 for s in manual_sources if s.status != "fresh")
    monthly_missing = sum(1 for s in monthly_sources if s.status == "missing")

    kb_line = ""
    if report.kb_summaries:
        total_7d = sum(k.count_7d for k in report.kb_summaries)
        now_bj = datetime.now(_BJ)
        if now_bj.weekday() == 0:
            kb_line = f"📊 知识库        本周新增 {total_7d} 篇"
        elif now_bj.day == 1:
            total_30d = sum(k.count_30d for k in report.kb_summaries)
            kb_line = f"📊 知识库        本月新增 {total_30d} 篇"

    lines = [
        f"✅ 自动管道      {auto_fresh}/{len(auto_sources)} 正常",
        f"{'⚠️' if manual_issues else '✅'} 手动上传      {'%d 项需关注' % manual_issues if manual_issues else '正常'}",
        f"{'🔴' if monthly_missing else '✅'} 月度数据      {'%d 项缺失' % monthly_missing if monthly_missing else '正常'}",
    ]
    if kb_line:
        lines.append(kb_line)

    body = "\n".join(lines)
    template = "orange" if report.has_alerts else "green"

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": template,
            "title": {"content": f"📡 数据巡视报告 — {date_str}", "tag": "plain_text"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md", "content": body}},
            {"tag": "hr"},
            {"tag": "action", "actions": [
                {"tag": "button", "text": {"tag": "plain_text", "content": "展开详情 ▼"},
                 "type": "primary", "value": {"act": "patrol_expand"}},
                {"tag": "button", "text": {"tag": "plain_text", "content": "关闭"},
                 "type": "default", "value": {"act": "patrol_close"}},
            ]},
        ],
    }


def _source_row(s: SourceStatus) -> dict:
    """Build one div row for a SourceStatus, with optional 填入数据 button."""
    icon = _STATUS_ICON.get(s.status, "❓")
    last_str = str(s.last_date) if s.last_date else "—"
    behind_str = f" · 落后 {s.days_behind} 天" if s.days_behind not in (0, _MISSING_SENTINEL) else ""
    label = f"{icon} **{s.name}**  最后: {last_str}{behind_str}"
    row: dict = {"tag": "div", "text": {"tag": "lark_md", "content": label}}
    if s.fill_table:
        row["extra"] = {
            "tag": "button",
            "text": {"tag": "plain_text", "content": "填入数据"},
            "type": "danger",
            "value": {
                "act": "patrol_fill_open",
                "fill_table": s.fill_table,
                "fill_province": s.fill_province,
                "fill_month": s.fill_month,
            },
        }
    elif s.reminder_text:
        row["extra"] = {
            "tag": "button",
            "text": {"tag": "plain_text", "content": "上传提醒"},
            "type": "warning",
            "value": {"act": "patrol_remind", "reminder": s.reminder_text},
        }
    return row


def build_detail_card(report: PatrolReport) -> dict:
    now = report.generated_at
    date_str = f"{now.year}年{now.month}月{now.day}日 {_WEEKDAYS_CN[now.weekday()]}"
    elements: list[dict] = []

    def _section(title: str, sources: list[SourceStatus]) -> None:
        if not sources:
            return
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": f"**{title}**"}})
        for s in sources:
            elements.append(_source_row(s))
        elements.append({"tag": "hr"})

    _section("⚡ 自动管道", report.by_group("auto"))
    _section("📤 手动上传", report.by_group("manual"))
    _section("🗓 月度数据", report.by_group("monthly"))

    if report.kb_summaries:
        elements.append({"tag": "div", "text": {"tag": "lark_md", "content": "**📚 知识库活跃度**"}})
        for k in report.kb_summaries:
            last_str = str(k.last_ingested) if k.last_ingested else "—"
            elements.append({"tag": "div", "text": {"tag": "lark_md",
                "content": f"• {k.name}  最后入库: {last_str} · 7天 {k.count_7d}篇 · 30天 {k.count_30d}篇"}})
        elements.append({"tag": "hr"})

    elements.append({"tag": "action", "actions": [
        {"tag": "button", "text": {"tag": "plain_text", "content": "收起 ▲"},
         "type": "default", "value": {"act": "patrol_collapse"}},
    ]})

    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "orange" if report.has_alerts else "green",
            "title": {"content": f"📡 数据巡视详情 — {date_str}", "tag": "plain_text"},
        },
        "elements": elements,
    }


def build_fill_card(fill_table: str, fill_province: str, fill_month: str) -> dict:
    """Interactive card for manually entering a missing monthly value."""
    _TABLE_LABELS = {
        "province_cap_comp":          ("容量补偿", "cap_comp_yuan_kw", "容量补偿标准 (¥/kW·年)", "peak_duration_hours", "年最高净负荷峰值时段 (h)"),
        "province_fr_market":         ("调频市场", "fr_price_yuan_kw_h", "调频容量价格 (¥/kW·h)", "fr_pool_yi_yuan", "全省调频资金池 (亿元/年)"),
        "province_installed_monthly": ("储能装机", "installed_mw", "储能装机 (MW)", None, None),
        "province_sysopfee_monthly":  ("系统运行费", "fee_yuan_kwh", "系统运行费 (¥/kWh)", None, None),
    }
    label, field1, field1_label, field2, field2_label = _TABLE_LABELS.get(
        fill_table, (fill_table, "value", "数值", None, None)
    )
    actions = [
        {"tag": "input", "name": field1,
         "placeholder": {"tag": "plain_text", "content": field1_label}, "width": "fill"},
    ]
    if field2:
        actions.append(
            {"tag": "input", "name": field2,
             "placeholder": {"tag": "plain_text", "content": field2_label}, "width": "fill"}
        )
    actions.append({
        "tag": "button", "text": {"tag": "plain_text", "content": "提交"},
        "type": "primary",
        "value": {"act": "patrol_fill_submit",
                  "fill_table": fill_table, "fill_province": fill_province,
                  "fill_month": fill_month, "field1": field1, "field2": field2 or ""},
    })
    actions.append({
        "tag": "button", "text": {"tag": "plain_text", "content": "发文件给我，AI自动提取"},
        "type": "default",
        "value": {"act": "patrol_fill_file",
                  "fill_table": fill_table, "fill_province": fill_province,
                  "fill_month": fill_month},
    })
    return {
        "config": {"wide_screen_mode": True},
        "header": {
            "template": "blue",
            "title": {"content": f"填写缺失数据 — {label} / {fill_province or '(选省份)'} / {fill_month}", "tag": "plain_text"},
        },
        "elements": [
            {"tag": "div", "text": {"tag": "lark_md",
                "content": f"省份: **{fill_province or '请输入'}**\n月份: **{fill_month}**"}},
            {"tag": "hr"},
            {"tag": "action", "actions": actions},
        ],
    }


# ── Entry point ───────────────────────────────────────────────────────────────

# Cache last patrol result in memory so /hermes/patrol/status can return it
_last_report: Optional[PatrolReport] = None


def run_patrol(
    pg_url: str,
    feishu,
    owner_open_id: str,
    api_key: str = "",
) -> PatrolReport:
    """
    Run all data checks, build and send the summary Feishu card,
    and send separate upload reminder messages for stale manual items.
    """
    global _last_report

    sources: list[SourceStatus] = []
    sources.extend(check_auto_pipelines(pg_url))
    sources.extend(check_manual_uploads(pg_url))
    sources.extend(check_monthly_data(pg_url))
    kb = check_kb_activity(pg_url)

    report = PatrolReport(sources=sources, kb_summaries=kb)
    _last_report = report

    if feishu and owner_open_id:
        try:
            card = build_summary_card(report)
            feishu.send_card(open_id=owner_open_id, card=card)
        except Exception as exc:
            logger.error("patrol: failed to send summary card: %s", exc)
            try:
                feishu.send_text(open_id=owner_open_id,
                                 text=f"📡 数据巡视完成，{'存在异常' if report.has_alerts else '一切正常'}。")
            except Exception:
                pass

        for s in sources:
            if s.reminder_text and s.status != "fresh":
                try:
                    feishu.send_text(open_id=owner_open_id, text=f"📤 数据缺失提醒：\n{s.reminder_text}")
                except Exception as exc:
                    logger.warning("patrol: reminder send failed: %s", exc)

    return report


def get_last_report() -> Optional[PatrolReport]:
    return _last_report
