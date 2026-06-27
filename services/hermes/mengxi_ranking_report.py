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
with_comp AS (
    SELECT
        plant_name,
        discharge_rev,
        charge_cost,
        discharge_mwh,
        days,
        max_energy,
        min_energy,
        350.0 * discharge_mwh AS comp_yuan
    FROM raw_data
),
raw AS (
    SELECT
        plant_name,
        SUM(discharge_rev)  AS discharge_rev,
        SUM(charge_cost)    AS charge_cost,
        SUM(discharge_mwh)  AS discharge_mwh,
        SUM(days)           AS days,
        SUM(comp_yuan)      AS comp_yuan
    FROM with_comp
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
    comp_yuan
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


def _enrich_and_rank(raw_df: pd.DataFrame, plant_list: list[dict]) -> pd.DataFrame:
    """Merge DB result with plant metadata and compute rank."""
    if raw_df.empty:
        return raw_df

    plant_df = pd.DataFrame(plant_list)[["plant_name", "owner", "mw"]]
    df = raw_df.merge(plant_df, on="plant_name", how="left")
    df["owner"] = df["owner"].fillna("未知")
    df["mw"] = df["mw"].fillna(0.0)

    # Filter: must have known MW > 0 to compute meaningful score
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

def _generate_pdf(
    yesterday_df: pd.DataFrame,
    month_df: pd.DataFrame,
    ytd_df: pd.DataFrame,
    report_date: date,
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
            elems.append(_build_table(df))
        return elems

    yesterday_str = report_date.strftime("%Y-%m-%d")
    month_start   = report_date.replace(day=1).strftime("%Y-%m-%d")
    ytd_start     = report_date.replace(month=1, day=1).strftime("%Y-%m-%d")
    latest_label  = f"最新（{yesterday_str}）"

    n_y = len(yesterday_df)
    n_m = len(month_df)
    n_y2 = len(ytd_df)

    story = [
        Paragraph("蒙西BESS市场排名日报", title_s),
        Paragraph(f"报告日期：{yesterday_str}　　共收录 {max(n_y, n_m, n_y2)} 个BESS项目", sub_s),
        Paragraph("▲ 绿色行 = 远景能源（Envision Energy）旗下资产", legend_s),
        HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#aaaaaa")),
    ]

    story += _section(
        f"📅 {latest_label}排名　共 {n_y} 个 BESS",
        yesterday_df,
    )

    story += [PageBreak()]
    story += _section(
        f"📆 本月排名（{month_start} ～ {yesterday_str}）　共 {n_m} 个 BESS",
        month_df,
    )

    story += [PageBreak()]
    story += _section(
        f"📊 年度排名（{ytd_start} ～ {yesterday_str}）　共 {n_y2} 个 BESS",
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
            "容量补偿标准：350元/MWh，适用于全部BESS项目。",
            note_s,
        ),
        Paragraph(
            "价格说明：放电收入采用15分钟节点内日出清电价；充电成本采用同节点小时均价。",
            note_s,
        ),
        Paragraph(
            "资产列表来源：OneDrive data/电站.xlsx。",
            note_s,
        ),
    ]

    doc.build(story)
    return buf.getvalue()


# ── Orchestration ─────────────────────────────────────────────────────────────

def _latest_data_date(pg_url: str) -> Optional[date]:
    """Return the latest date with cleared_energy data, or None if table is empty."""
    # MAX(data_date) — leading index key → index-only backward scan, milliseconds.
    conn = psycopg2.connect(pg_url, options="-c statement_timeout=30000")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(data_date) FROM marketdata.md_id_cleared_energy"
            )
            row = cur.fetchone()
            return row[0] if row and row[0] else None
    finally:
        conn.close()


def send_daily_ranking(
    feishu,
    owner_open_id: str,
    pg_url: str,
    onedrive_client=None,
) -> None:
    """Query rankings for latest-data-date / current month / YTD, generate PDF, send to Feishu."""
    from datetime import datetime, timezone, timedelta

    # ── Load plant list from OneDrive ──────────────────────────────────────────
    if onedrive_client is None:
        logger.error("Mengxi ranking report: no OneDrive client — cannot load plant list")
        if feishu and owner_open_id:
            feishu.send_text(owner_open_id, "⚠️ 蒙西BESS日报失败：OneDrive未配置，无法加载电站列表。")
        return

    try:
        plant_list = _read_bess_list_from_onedrive(onedrive_client)
    except Exception as exc:
        logger.error("Mengxi ranking report: failed to load 电站.xlsx: %s", exc)
        if feishu and owner_open_id:
            feishu.send_text(owner_open_id, f"⚠️ 蒙西BESS日报失败（无法读取电站.xlsx）：{exc}")
        return

    if not plant_list:
        logger.error("Mengxi ranking report: 电站.xlsx is empty")
        if feishu and owner_open_id:
            feishu.send_text(owner_open_id, "⚠️ 蒙西BESS日报失败：电站.xlsx 为空。")
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

    # ── Enrich with owner/mw and compute rank ─────────────────────────────────
    yesterday_df = _enrich_and_rank(yesterday_raw, plant_list)
    month_df     = _enrich_and_rank(month_raw,     plant_list)
    ytd_df       = _enrich_and_rank(ytd_raw,       plant_list)

    # ── Generate PDF ───────────────────────────────────────────────────────────
    try:
        pdf_bytes = _generate_pdf(yesterday_df, month_df, ytd_df, yesterday)
    except Exception as exc:
        logger.error("Mengxi ranking report PDF error: %s", exc, exc_info=True)
        if feishu and owner_open_id:
            feishu.send_text(owner_open_id, f"⚠️ 蒙西BESS日报失败（PDF生成错误）：{exc}")
        return

    if not feishu or not owner_open_id:
        logger.warning("Mengxi ranking report: no Feishu target, skipping send")
        return

    # ── Send via Feishu ────────────────────────────────────────────────────────
    filename = f"蒙西BESS排名日报_{yesterday.strftime('%Y%m%d')}.pdf"
    try:
        file_key = feishu.upload_file(pdf_bytes, filename, file_type="pdf")
        feishu.send_file(owner_open_id, file_key)
        logger.info("Mengxi ranking report sent: %s (%d bytes)", filename, len(pdf_bytes))
    except Exception as exc:
        logger.error("Mengxi ranking report Feishu send failed: %s", exc, exc_info=True)
        feishu.send_text(owner_open_id, f"⚠️ 蒙西BESS日报PDF发送失败：{exc}")
