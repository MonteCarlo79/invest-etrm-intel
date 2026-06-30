"""
Daily BESS Market Ranking report for Mengxi (Inner Mongolia) market.

Plant list is read from OneDrive data/电站.xlsx — no station_master dependency.
New-BESS detection has been moved to mengxi_bess_screener.py.

Schedule: daily at 23:00 UTC (07:00 Beijing UTC+8).
"""
from __future__ import annotations

import io
import logging
from datetime import date, timedelta
from typing import Optional

# ── Capacity compensation rates ───────────────────────────────────────────────
# Assets with zero compensation (no 350元/MWh eligibility)
_ZERO_COMP_PLANTS: frozenset[str] = frozenset([
    "大航都林储能电站",
    "大航额日和图储能电站",
])

# Assets with explicit 280元/MWh compensation
_280_COMP_PLANTS: frozenset[str] = frozenset([
    "荣鑫地房子储能电站",
])

# Assets joining the market after this date (and before 2027-01-01) get 280元/MWh
_NEW_ASSET_COMP_CUTOFF = date(2026, 6, 27)
_NEW_ASSET_COMP_END    = date(2027, 1, 1)
_DEFAULT_COMP_RATE     = 350.0
_NEW_COMP_RATE         = 280.0

import pandas as pd
import psycopg2

logger = logging.getLogger(__name__)

# ── Ranking SQL ───────────────────────────────────────────────────────────────
# Scoped to the plant_names list supplied by caller (from 电站.xlsx).
# No station_master dependency — owner/mw are merged in Python afterwards.
# HAVING filter ensures only assets with both charge AND discharge are included.

_RANKING_SQL = """
WITH raw_data AS (
    SELECT
        e.plant_name,
        date_trunc('month', e.datetime)::date                                           AS month,
        SUM(CASE WHEN e.cleared_energy_mwh > 0
                 THEN e.cleared_energy_mwh * e.cleared_price ELSE 0 END) / 4           AS discharge_rev,
        SUM(CASE WHEN e.cleared_energy_mwh < 0
                 THEN -e.cleared_energy_mwh * e.cleared_price ELSE 0 END) / 4          AS charge_cost,
        SUM(CASE WHEN e.cleared_energy_mwh > 0
                 THEN e.cleared_energy_mwh ELSE 0 END) / 4                             AS discharge_mwh,
        COUNT(DISTINCT e.datetime::date)                                                AS days,
        MAX(e.cleared_energy_mwh)                                                       AS max_energy,
        MIN(e.cleared_energy_mwh)                                                       AS min_energy
    FROM marketdata.md_id_cleared_energy e
    WHERE e.data_date >= %(start)s
      AND e.data_date <  %(end_excl)s
      AND e.plant_name = ANY(%(plant_names)s)
    GROUP BY e.plant_name, date_trunc('month', e.datetime)::date
),
raw AS (
    SELECT
        plant_name,
        SUM(discharge_rev)  AS discharge_rev,
        SUM(charge_cost)    AS charge_cost,
        SUM(discharge_mwh)  AS discharge_mwh,
        SUM(days)           AS days,
        MAX(max_energy)     AS max_energy
    FROM raw_data
    GROUP BY plant_name
    HAVING MAX(max_energy) > 0
       AND MIN(min_energy) < 0
)
SELECT
    plant_name,
    discharge_rev,
    charge_cost,
    discharge_mwh,
    days::int  AS days,
    max_energy
FROM raw
"""


_BESS_EXCEL_PATH = "etrm/bess-platform/data/电站.xlsx"


def _read_bess_list_from_onedrive(onedrive_client) -> list[dict]:
    """Download 电站.xlsx from OneDrive and return list of {plant_name, owner, mw}.

    Expected columns (0-indexed):
      0: ID, 1: 储能电站名称 (plant_name), 3: 省份, 4: 位置,
      5: 业主 (owner), 6: 运营方, 7: 装机MW (mw), 8: 装机容量MWh
    """
    raw = onedrive_client.read_file_by_path(_BESS_EXCEL_PATH)

    wb = __import__("openpyxl").load_workbook(
        io.BytesIO(raw), read_only=True, data_only=True
    )
    ws = wb.active
    plants = []
    first_row = True
    for row in ws.iter_rows(values_only=True):
        if first_row:
            first_row = False
            continue  # skip header
        plant_name = row[1] if len(row) > 1 else None
        if not plant_name:
            continue
        owner = row[5] if len(row) > 5 and row[5] else "未知"
        mw_raw = row[7] if len(row) > 7 else None
        try:
            mw = float(mw_raw) if mw_raw is not None else 0.0
        except (TypeError, ValueError):
            mw = 0.0
        plants.append({"plant_name": str(plant_name).strip(), "owner": str(owner).strip(), "mw": mw})
    wb.close()
    logger.info("Loaded %d plants from 电站.xlsx", len(plants))
    return plants


def _read_station_master(pg_url: str) -> list[dict]:
    """Load plant metadata from marketdata.station_master (owner + MW pre-screened by Claude)."""
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=10000")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT plant_name, mw, owner FROM marketdata.station_master ORDER BY plant_name")
            return [{"plant_name": r[0], "mw": float(r[1]) if r[1] else 0.0, "owner": r[2] or "未知"}
                    for r in cur.fetchall()]
    finally:
        conn.close()


def _query(pg_url: str, start: date, end_excl: date, plant_names: list[str]) -> pd.DataFrame:
    # 10-minute timeout — YTD queries on a 7 GB table need headroom
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=600000")
    try:
        df = pd.read_sql_query(
            _RANKING_SQL,
            conn,
            params={"start": start, "end_excl": end_excl, "plant_names": plant_names},
        )
    finally:
        conn.close()
    return df


def _query_first_seen(pg_url: str, plant_names: list[str]) -> dict[str, date]:
    """Return {plant_name: first_data_date} for each plant."""
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=60000")
    try:
        df = pd.read_sql_query(
            "SELECT plant_name, MIN(data_date)::date AS first_seen "
            "FROM marketdata.md_id_cleared_energy "
            "WHERE plant_name = ANY(%(names)s) "
            "GROUP BY plant_name",
            conn,
            params={"names": plant_names},
        )
    finally:
        conn.close()
    return {row["plant_name"]: row["first_seen"] for _, row in df.iterrows()}


def _resolve_comp_rate(plant_name: str, first_seen: Optional[date], report_date: date) -> float:
    """Return the capacity compensation rate (元/MWh) for a plant."""
    if plant_name in _ZERO_COMP_PLANTS:
        return 0.0
    if plant_name in _280_COMP_PLANTS:
        return _NEW_COMP_RATE
    if (
        first_seen is not None
        and first_seen > _NEW_ASSET_COMP_CUTOFF
        and report_date < _NEW_ASSET_COMP_END
    ):
        return _NEW_COMP_RATE
    return _DEFAULT_COMP_RATE


def _apply_comp(df: pd.DataFrame, first_seen_map: dict[str, date], report_date: date) -> pd.DataFrame:
    """Add comp_yuan column to a raw query result DataFrame."""
    if df.empty:
        df["comp_yuan"] = 0.0
        return df
    rates = df["plant_name"].map(
        lambda name: _resolve_comp_rate(name, first_seen_map.get(name), report_date)
    )
    df = df.copy()
    df["comp_yuan"] = rates * df["discharge_mwh"]
    return df


def _enrich_and_rank(raw_df: pd.DataFrame, plant_list: list[dict]) -> pd.DataFrame:
    """Merge DB result with plant metadata and compute rank."""
    if raw_df.empty:
        return raw_df

    plant_df = pd.DataFrame(plant_list)[["plant_name", "owner", "mw"]]
    df = raw_df.merge(plant_df, on="plant_name", how="left")
    df["owner"] = df["owner"].fillna("未知")
    df["mw"] = df["mw"].fillna(0.0)

    # For plants missing MW in 电站.xlsx, infer from max observed dispatch energy.
    # max_energy = MAX(cleared_energy_mwh per 15-min interval); MW ≈ max_energy * 4.
    mask_no_mw = df["mw"] == 0
    if mask_no_mw.any() and "max_energy" in df.columns:
        df.loc[mask_no_mw, "mw"] = (df.loc[mask_no_mw, "max_energy"] * 4).round().clip(lower=1)

    df = df[df["mw"] > 0].copy()
    if df.empty:
        return df

    df["profit_wan"] = (df["discharge_rev"] - df["charge_cost"] + df["comp_yuan"]) / 10000.0
    df["installed_mwh"] = df["mw"] * 4
    df["score"] = df.apply(
        lambda r: (r["profit_wan"] * 10000) / (r["installed_mwh"] * r["days"])
        if r["installed_mwh"] > 0 and r["days"] > 0 else None,
        axis=1,
    )
    df = df.sort_values("score", ascending=False, na_position="last").reset_index(drop=True)
    df.insert(0, "rank", range(1, len(df) + 1))
    df["mw"] = df["mw"].astype(int)
    df["profit_wan"] = df["profit_wan"].round(1)
    df["score"] = df["score"].round(4)
    return df[["rank", "plant_name", "owner", "mw", "profit_wan", "score", "days"]]


# ── PDF generation ────────────────────────────────────────────────────────────

def _cap_str(df: pd.DataFrame) -> str:
    """Return ' / X.XX GW / X.XX GWh' capacity string from a ranked df, or ''."""
    if df.empty or "mw" not in df.columns:
        return ""
    mw = float(df["mw"].sum())
    if mw <= 0:
        return ""
    return f" / {mw/1000:.2f} GW / {mw*4/1000:.2f} GWh"


def _generate_pdf(
    yesterday_df: pd.DataFrame,
    month_df: pd.DataFrame,
    ytd_df: pd.DataFrame,
    report_date: date,
    total_mw: float = 0.0,
) -> bytes:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, PageBreak,
    )
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.cidfonts import UnicodeCIDFont

    # CID font — bundled in reportlab, no external files needed
    pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
    F = "STSong-Light"

    ENVISION_BG   = colors.HexColor("#28a745")
    ENVISION_FG   = colors.white
    HEADER_BG     = colors.HexColor("#1f3b63")
    ALT_ROW       = colors.HexColor("#f0f4fa")

    buf = io.BytesIO()
    doc = SimpleDocTemplate(
        buf, pagesize=A4,
        leftMargin=12*mm, rightMargin=12*mm,
        topMargin=12*mm, bottomMargin=12*mm,
    )

    def _ps(name, size, **kw):
        return ParagraphStyle(name, fontName=F, fontSize=size, **kw)

    title_s  = _ps("t",    16, alignment=1, spaceAfter=4)
    sub_s    = _ps("sub",   8, alignment=1, textColor=colors.grey, spaceAfter=2)
    legend_s = _ps("leg",   8, textColor=ENVISION_BG, spaceAfter=6)
    h2_s     = _ps("h2",   11, spaceBefore=8, spaceAfter=3,
                   textColor=colors.HexColor("#1f3b63"))
    note_s   = _ps("n",     7, textColor=colors.grey, spaceAfter=4)

    COL_HDR = ["排名", "项目名称", "业主", "MW", "总收益(万元)", "收益/MWh/天", "天数"]
    COL_W   = [11*mm, 58*mm, 34*mm, 13*mm, 27*mm, 27*mm, 13*mm]

    def _build_table(df: pd.DataFrame, top_n: int = 30):
        sub = df.head(top_n).reset_index(drop=True)
        rows = [COL_HDR]
        for _, r in sub.iterrows():
            rows.append([
                str(r["rank"]),
                str(r["plant_name"]),
                str(r["owner"]),
                str(r["mw"]),
                f"{r['profit_wan']:.0f}",
                f"{r['score']:.0f}",
                str(r["days"]),
            ])

        cmds = [
            ("FONTNAME",   (0, 0), (-1, -1), F),
            ("FONTSIZE",   (0, 0), (-1, -1), 7.5),
            ("BACKGROUND", (0, 0), (-1,  0), HEADER_BG),
            ("TEXTCOLOR",  (0, 0), (-1,  0), colors.white),
            ("FONTSIZE",   (0, 0), (-1,  0), 8),
            ("ALIGN",      (0, 0), (-1, -1), "CENTER"),
            ("ALIGN",      (1, 1), (2, -1), "LEFT"),
            ("GRID",       (0, 0), (-1, -1), 0.25, colors.lightgrey),
            ("TOPPADDING", (0, 0), (-1, -1), 2),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
        ]
        for i in range(1, len(rows)):
            if i % 2 == 0:
                cmds.append(("BACKGROUND", (0, i), (-1, i), ALT_ROW))
            if "远景" in rows[i][2]:  # owner column
                cmds += [
                    ("BACKGROUND", (0, i), (-1, i), ENVISION_BG),
                    ("TEXTCOLOR",  (0, i), (-1, i), ENVISION_FG),
                    ("FONTNAME",   (0, i), (-1, i), F),
                ]

        t = Table(rows, colWidths=COL_W, repeatRows=1)
        t.setStyle(TableStyle(cmds))
        return t

    def _section(title: str, df: pd.DataFrame) -> list:
        elems = [Paragraph(title, h2_s)]
        if df.empty:
            elems.append(Paragraph("暂无数据（该时段无BESS充放电记录）", note_s))
        else:
            elems.append(_build_table(df, top_n=len(df)))  # show all plants
        return elems

    yesterday_str = report_date.strftime("%Y-%m-%d")
    month_start   = report_date.replace(day=1).strftime("%Y-%m-%d")
    ytd_start     = report_date.replace(month=1, day=1).strftime("%Y-%m-%d")
    latest_label  = f"最新（{yesterday_str}）"

    n_y = len(yesterday_df)
    n_m = len(month_df)
    n_y2 = len(ytd_df)

    capacity_str = ""
    if total_mw > 0:
        total_gw  = total_mw / 1000
        total_gwh = total_mw * 4 / 1000
        capacity_str = f"　　合计装机 {total_gw:.2f} GW / {total_gwh:.2f} GWh"

    story = [
        Paragraph("蒙西BESS市场排名日报", title_s),
        Paragraph(f"报告日期：{yesterday_str}　　共收录 {max(n_y, n_m, n_y2)} 个BESS项目{capacity_str}", sub_s),
        Paragraph("▲ 绿色行 = 远景能源（Envision Energy）旗下资产", legend_s),
        HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#aaaaaa")),
    ]

    story += _section(
        f"📅 {latest_label}排名　共 {n_y} 个 BESS{_cap_str(yesterday_df)}",
        yesterday_df,
    )

    story += [PageBreak()]
    story += _section(
        f"📆 本月排名（{month_start} ～ {yesterday_str}）　共 {n_m} 个 BESS{_cap_str(month_df)}",
        month_df,
    )

    story += [PageBreak()]
    story += _section(
        f"📊 年度排名（{ytd_start} ～ {yesterday_str}）　共 {n_y2} 个 BESS{_cap_str(ytd_df)}",
        ytd_df,
    )

    story += [
        Spacer(1, 4*mm),
        HRFlowable(width="100%", thickness=0.3, color=colors.lightgrey),
        Paragraph(
            "排名指标：收益/MWh/天 = (放电收入 − 充电成本 + 容量补偿) ÷ (装机容量MWh × 天数)，"
            "假设储能时长4小时。单位：万元/MWh/天。",
            note_s,
        ),
        Paragraph(
            "容量补偿标准：350元/MWh（一般项目）；280元/MWh（荣鑫地房子储能电站及2026年6月27日后入市项目）；"
            "0元/MWh（大航都林储能电站、大航额日和图储能电站）。",
            note_s,
        ),
        Paragraph(
            "价格说明：放电收入采用15分钟节点内日出清电价；充电成本采用同节点小时均价。",
            note_s,
        ),
    ]

    doc.build(story)
    return buf.getvalue()


# ── Orchestration ─────────────────────────────────────────────────────────────

def _latest_data_date(pg_url: str) -> Optional[date]:
    """Return the latest date with cleared_energy data, or None if table is empty.

    Queries both md_id_cleared_energy (intraday) and md_da_cleared_energy (day-ahead)
    and returns the most recent date across either table. This guards against the case
    where the intraday market is suspended but day-ahead data continues to arrive.
    """
    import time as _time
    last_exc: Exception = RuntimeError("no attempts made")
    for attempt in range(3):
        if attempt:
            _time.sleep(10)
        try:
            conn = psycopg2.connect(pg_url, options="-c statement_timeout=120000")
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT GREATEST(
                            (SELECT MAX(data_date) FROM marketdata.md_id_cleared_energy),
                            (SELECT MAX(data_date) FROM marketdata.md_da_cleared_energy)
                        )
                        """
                    )
                    row = cur.fetchone()
                    return row[0] if row and row[0] else None
            finally:
                conn.close()
        except Exception as exc:
            last_exc = exc
            logger.warning("_latest_data_date attempt %d failed: %s", attempt + 1, exc)
    raise last_exc


def send_daily_ranking(
    feishu,
    owner_open_id: str,
    pg_url: str,
    onedrive_client=None,
    wecom_webhook_url: Optional[str] = None,
    wecom_client=None,
    wecom_direct_uid: Optional[str] = None,
) -> None:
    """Query rankings for latest-data-date / current month / YTD, generate PDF, send to Feishu and/or WeCom."""
    from datetime import datetime, timezone, timedelta

    # ── Load plant metadata: station_master (DB) primary, 电站.xlsx fallback ──
    # station_master has Claude-screened owner + MW for all known plants.
    # 电站.xlsx provides any additional plant names not yet in station_master.
    try:
        sm_list = _read_station_master(pg_url)
        sm_by_name = {p["plant_name"]: p for p in sm_list}
        logger.info("Loaded %d plants from station_master", len(sm_list))
    except Exception as exc:
        logger.warning("Could not load station_master: %s — falling back to Excel only", exc)
        sm_by_name = {}

    excel_list: list[dict] = []
    if onedrive_client:
        try:
            excel_list = _read_bess_list_from_onedrive(onedrive_client)
        except Exception as exc:
            logger.warning("Could not read 电站.xlsx: %s", exc)

    # Merge: station_master owner/mw takes priority; Excel adds any extra plant names
    plant_map: dict[str, dict] = {}
    for p in excel_list:
        plant_map[p["plant_name"]] = p  # Excel baseline
    for name, p in sm_by_name.items():
        plant_map[name] = p  # station_master overwrites with correct owner/mw

    plant_list = list(plant_map.values())
    if not plant_list:
        logger.error("Mengxi ranking report: no plants found in station_master or 电站.xlsx")
        if feishu and owner_open_id:
            feishu.send_text(owner_open_id, "⚠️ 蒙西BESS日报失败：无法加载电站列表。")
        return

    plant_names = [p["plant_name"] for p in plant_list]

    # ── Determine report date ──────────────────────────────────────────────────
    try:
        latest = _latest_data_date(pg_url)
    except Exception as exc:
        logger.error("Mengxi ranking report: _latest_data_date failed: %s", exc)
        if feishu and owner_open_id:
            feishu.send_text(owner_open_id, f"⚠️ 蒙西BESS日报失败（获取最新日期失败）：{exc}")
        return

    if latest is None:
        logger.error("Mengxi ranking report: no data found in md_id_cleared_energy")
        if feishu and owner_open_id:
            feishu.send_text(owner_open_id, "⚠️ 蒙西BESS日报失败：数据库中暂无出清数据。")
        return

    yesterday   = latest
    month_start = yesterday.replace(day=1)
    ytd_start   = yesterday.replace(month=1, day=1)
    end_excl    = yesterday + timedelta(days=1)

    logger.info("Mengxi ranking report: computing for %s (%d plants)", yesterday, len(plant_names))

    # ── Query ──────────────────────────────────────────────────────────────────
    try:
        yesterday_raw = _query(pg_url, yesterday,    end_excl, plant_names)
        month_raw     = _query(pg_url, month_start,  end_excl, plant_names)
        ytd_raw       = _query(pg_url, ytd_start,    end_excl, plant_names)
    except Exception as exc:
        logger.error("Mengxi ranking report DB error: %s", exc, exc_info=True)
        if feishu and owner_open_id:
            feishu.send_text(owner_open_id, f"⚠️ 蒙西BESS日报失败（DB查询错误）：{exc}")
        return

    # ── Resolve per-plant compensation rates ──────────────────────────────────
    try:
        first_seen_map = _query_first_seen(pg_url, plant_names)
    except Exception as exc:
        logger.warning("Could not query first_seen dates: %s — defaulting all to 350", exc)
        first_seen_map = {}

    yesterday_raw = _apply_comp(yesterday_raw, first_seen_map, yesterday)
    month_raw     = _apply_comp(month_raw,     first_seen_map, yesterday)
    ytd_raw       = _apply_comp(ytd_raw,       first_seen_map, yesterday)

    # ── Enrich with owner/mw and compute rank ─────────────────────────────────
    yesterday_df = _enrich_and_rank(yesterday_raw, plant_list)
    month_df     = _enrich_and_rank(month_raw,     plant_list)
    ytd_df       = _enrich_and_rank(ytd_raw,       plant_list)

    # ── Generate PDF ───────────────────────────────────────────────────────────
    total_mw = float(ytd_df["mw"].sum()) if not ytd_df.empty else 0.0
    try:
        pdf_bytes = _generate_pdf(yesterday_df, month_df, ytd_df, yesterday, total_mw=total_mw)
    except Exception as exc:
        logger.error("Mengxi ranking report PDF error: %s", exc, exc_info=True)
        if feishu and owner_open_id:
            feishu.send_text(owner_open_id, f"⚠️ 蒙西BESS日报失败（PDF生成错误）：{exc}")
        return

    filename = f"蒙西BESS排名日报_{yesterday.strftime('%Y%m%d')}.pdf"

    # ── Send via Feishu ────────────────────────────────────────────────────────
    if feishu and owner_open_id:
        try:
            file_key = feishu.upload_file(pdf_bytes, filename, file_type="pdf")
            feishu.send_file(owner_open_id, file_key)
            logger.info("Mengxi ranking report sent via Feishu: %s (%d bytes)", filename, len(pdf_bytes))
        except Exception as exc:
            logger.error("Mengxi ranking report Feishu send failed: %s", exc, exc_info=True)
            try:
                feishu.send_text(owner_open_id, f"⚠️ 蒙西BESS日报PDF发送失败：{exc}")
            except Exception:
                pass
    else:
        logger.warning("Mengxi ranking report: no Feishu target configured, skipping Feishu send")

    # ── Send via WeCom webhook (group bot) ────────────────────────────────────
    if wecom_webhook_url:
        from services.hermes.wecom_client import send_pdf_via_wecom_webhook
        try:
            send_pdf_via_wecom_webhook(wecom_webhook_url, pdf_bytes, filename)
            logger.info("Mengxi ranking report sent via WeCom webhook: %s", filename)
        except Exception as exc:
            logger.error("Mengxi ranking report WeCom webhook send failed: %s", exc, exc_info=True)

    # ── Send directly to triggering WeCom user (corp app API) ─────────────────
    if wecom_client and wecom_direct_uid:
        try:
            wecom_client.send_file(wecom_direct_uid, pdf_bytes, filename)
            logger.info("Mengxi ranking report sent directly to WeCom user: %s", wecom_direct_uid)
        except Exception as exc:
            logger.error("Mengxi ranking report WeCom direct send failed: %s", exc, exc_info=True)
