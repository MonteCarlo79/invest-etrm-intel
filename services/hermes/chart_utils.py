"""Chart generation for Hermes — matplotlib-based, CJK-aware.

Parses markdown tables from market agent text output and renders PNG charts.
"""
from __future__ import annotations

import io
import logging
import re
from typing import Optional

logger = logging.getLogger(__name__)

# ── Direct DB fetch for charts (bypasses LLM truncation) ─────────────────────

_SPOT_METRICS = {"da_avg", "da_max", "da_min", "rt_avg", "rt_max", "rt_min"}
_PROVINCE_ALIASES = {
    "陕西": "陕西", "shaanxi": "陕西", "shanxi": "山西", "山西": "山西",
    "山东": "山东", "shandong": "山东", "广东": "广东", "guangdong": "广东",
    "蒙西": "蒙西", "内蒙古": "蒙西", "mengxi": "蒙西",
    "蒙东": "蒙东", "mengdong": "蒙东",
    "甘肃": "甘肃", "gansu": "甘肃", "宁夏": "宁夏", "ningxia": "宁夏",
    "新疆": "新疆", "xinjiang": "新疆", "四川": "四川", "sichuan": "四川",
    "云南": "云南", "yunnan": "云南", "贵州": "贵州", "guizhou": "贵州",
    "广西": "广西", "guangxi": "广西", "湖南": "湖南", "hunan": "湖南",
    "湖北": "湖北", "hubei": "湖北", "江苏": "江苏", "jiangsu": "江苏",
    "浙江": "浙江", "zhejiang": "浙江", "福建": "福建", "fujian": "福建",
    "河南": "河南", "henan": "河南", "河北": "河北", "hebei": "河北",
    "安徽": "安徽", "anhui": "安徽", "江西": "江西", "jiangxi": "江西",
    "辽宁": "辽宁", "liaoning": "辽宁", "吉林": "吉林", "jilin": "吉林",
    "黑龙江": "黑龙江", "heilongjiang": "黑龙江",
}


def _parse_spot_query_params(question: str, api_key: str) -> dict:
    """Use Claude Haiku to extract province, date range, and metrics from a question."""
    from shared.anthropic_client import make_client as _make_anthropic_client
    import json
    from datetime import date

    today = date.today().isoformat()
    year = date.today().year

    client = _make_anthropic_client(api_key)
    resp = client.messages.create(
        model="claude-sonnet-4-6",  # haiku-4-5 requires use-case form on this Bedrock account
        max_tokens=200,
        messages=[{
            "role": "user",
            "content": (
                f"Today is {today}. Extract chart params from this question.\n"
                f"Question: {question}\n\n"
                "Return ONLY a JSON object (no markdown) with:\n"
                '{"province_cn": "省名 or null", '
                f'"start_date": "YYYY-MM-DD (default {year}-01-01 for YTD)", '
                f'"end_date": "YYYY-MM-DD (default {today})", '
                '"metrics": ["rt_avg"] // subset of: da_avg,da_max,da_min,rt_avg,rt_max,rt_min}'
            ),
        }],
    )
    text = resp.content[0].text.strip()
    # Strip markdown fences if present
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.DOTALL).strip()
    try:
        params = json.loads(text)
    except Exception:
        # Fallback defaults
        params = {"province_cn": None, "start_date": f"{date.today().year}-01-01",
                  "end_date": today, "metrics": ["rt_avg"]}
    return params


def fetch_spot_dataframe(question: str, api_key: str, pg_url: str):
    """Query spot_daily directly for chart data — bypasses LLM truncation.

    Returns a pandas DataFrame with columns: report_date + requested metrics.
    """
    import psycopg2
    import pandas as pd
    from datetime import date

    params = _parse_spot_query_params(question, api_key)

    province_cn = params.get("province_cn")
    # Normalize province alias
    if province_cn:
        province_cn = _PROVINCE_ALIASES.get(province_cn, province_cn)

    start_date = params.get("start_date", f"{date.today().year}-01-01")
    end_date = params.get("end_date", date.today().isoformat())
    metrics = [m for m in params.get("metrics", ["rt_avg"]) if m in _SPOT_METRICS]
    if not metrics:
        metrics = ["rt_avg"]

    metric_sql = ", ".join(metrics)

    conn = psycopg2.connect(pg_url, options="-c statement_timeout=30000")
    try:
        if province_cn:
            df = pd.read_sql_query(
                f"SELECT report_date, {metric_sql} FROM public.spot_daily "
                "WHERE report_date BETWEEN %s AND %s AND province_cn = %s "
                "ORDER BY report_date",
                conn,
                params=(start_date, end_date, province_cn),
            )
        else:
            df = pd.read_sql_query(
                f"SELECT report_date, province_cn, {metric_sql} FROM public.spot_daily "
                "WHERE report_date BETWEEN %s AND %s ORDER BY report_date",
                conn,
                params=(start_date, end_date),
            )
    finally:
        conn.close()

    return df, province_cn, start_date, end_date, metrics


def generate_spot_line_chart(
    question: str,
    api_key: str,
    pg_url: str,
    title: str = "",
    y_label: str = "价格 (¥/kWh)",
) -> bytes:
    """Directly query spot_daily and render a line chart — no LLM table truncation."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import pandas as pd

    _setup_font()

    df, province_cn, start_date, end_date, metrics = fetch_spot_dataframe(
        question, api_key, pg_url
    )

    if df is None or df.empty:
        raise ValueError(f"spot_daily 中 {province_cn} {start_date}~{end_date} 无数据")

    # Convert report_date to datetime
    df["report_date"] = pd.to_datetime(df["report_date"])

    # Convert prices from ¥/kWh to ¥/MWh for readability
    for m in metrics:
        if m in df.columns:
            df[m] = pd.to_numeric(df[m], errors="coerce") * 1000
    y_label = y_label.replace("¥/kWh", "¥/MWh")
    if "kWh" not in y_label and "MWh" not in y_label:
        y_label = "价格 (¥/MWh)"

    palette = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]
    metric_labels = {
        "da_avg": "日前均价", "da_max": "日前最高", "da_min": "日前最低",
        "rt_avg": "实时均价", "rt_max": "实时最高", "rt_min": "实时最低",
    }

    fig, ax = plt.subplots(figsize=(14, 5))
    fig.patch.set_facecolor("#f5f7fa")
    ax.set_facecolor("#f5f7fa")

    if province_cn and "province_cn" not in df.columns:
        for i, m in enumerate(metrics):
            if m in df.columns:
                ax.plot(df["report_date"], df[m],
                        label=metric_labels.get(m, m),
                        color=palette[i % len(palette)], linewidth=1.5, alpha=0.9)
    else:
        # Multi-province: one line per province
        for i, (prov, grp) in enumerate(df.groupby("province_cn")):
            m = metrics[0]
            if m in grp.columns:
                ax.plot(grp["report_date"], grp[m], label=prov,
                        color=palette[i % len(palette)], linewidth=1.4, alpha=0.85)

    # x-axis
    span_days = (df["report_date"].max() - df["report_date"].min()).days
    if span_days > 180:
        ax.xaxis.set_major_locator(mdates.MonthLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    else:
        ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
    plt.xticks(rotation=40, ha="right", fontsize=8)

    chart_title = title or f"{province_cn or '全国'} {start_date[:4]}年 现货价格走势"
    ax.set_title(chart_title, fontsize=13, pad=10, fontweight="bold")
    ax.set_ylabel(y_label, fontsize=9)
    ax.set_xlabel("日期", fontsize=9)
    if len(metrics) > 1 or not province_cn:
        ax.legend(loc="best", fontsize=8, framealpha=0.7)
    ax.grid(True, alpha=0.25, color="white", linewidth=1)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    n_points = len(df)
    ax.annotate(
        f"数据点：{n_points} 天  ({start_date} ~ {end_date})",
        xy=(0.01, 0.01), xycoords="axes fraction",
        fontsize=7, color="gray",
    )

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def _setup_font() -> None:
    """Configure matplotlib to use a CJK font when available."""
    import matplotlib as mpl
    mpl.rcParams["axes.unicode_minus"] = False
    from matplotlib import font_manager
    cjk_candidates = [
        "Noto Sans CJK SC", "Noto Sans SC", "Noto CJK SC",
        "SimHei", "WenQuanYi Micro Hei", "AR PL UMing CN",
    ]
    for name in cjk_candidates:
        try:
            fp = font_manager.findfont(name, fallback_to_default=False)
            if fp and "DejaVu" not in fp:
                mpl.rcParams["font.sans-serif"] = [name] + mpl.rcParams["font.sans-serif"]
                logger.debug("matplotlib CJK font: %s", name)
                return
        except Exception:
            continue


def _parse_markdown_table(text: str):
    """Extract the first markdown table from text and return a pandas DataFrame.

    Returns None if no usable table is found.
    """
    import pandas as pd

    lines = text.split("\n")
    table_lines: list[str] = []
    collecting = False

    for line in lines:
        stripped = line.strip()
        if "|" in stripped:
            # Skip pure separator rows like |---|---|
            if re.match(r"^\|[-| :]+\|$", stripped):
                continue
            collecting = True
            table_lines.append(stripped)
        elif collecting and stripped == "":
            if len(table_lines) >= 2:
                break  # end of table
        elif collecting:
            break

    if len(table_lines) < 2:
        return None

    def _split_row(row: str) -> list[str]:
        return [c.strip() for c in row.strip("|").split("|")]

    headers = _split_row(table_lines[0])
    rows = [_split_row(r) for r in table_lines[1:]]
    # Normalize row lengths
    n = len(headers)
    rows = [r[:n] + [""] * max(0, n - len(r)) for r in rows]

    df = pd.DataFrame(rows, columns=headers)
    return df


def _to_numeric(series):
    """Convert a pandas Series to float, replacing missing markers."""
    import pandas as pd
    return pd.to_numeric(
        series.replace(["", "-", "—", "N/A", "无", "null", "None"], None),
        errors="coerce",
    )


def _find_date_col(df) -> Optional[str]:
    """Return the name of the first column that looks like dates."""
    import pandas as pd
    for col in df.columns:
        try:
            sample = pd.to_datetime(df[col].dropna().head(3), errors="raise")
            if len(sample) > 0:
                return col
        except Exception:
            continue
    return None


def _numeric_cols(df, exclude: Optional[str] = None) -> list[str]:
    """Return columns that are mostly numeric."""
    cols = []
    for col in df.columns:
        if col == exclude:
            continue
        s = _to_numeric(df[col])
        if s.notna().sum() >= max(1, len(df) // 3):
            cols.append(col)
    return cols


# ── Public renderers ──────────────────────────────────────────────────────────

def generate_line_chart(
    text_data: str,
    title: str = "",
    y_label: str = "",
    x_label: str = "日期",
) -> bytes:
    """Parse a markdown table from text_data and render as a line chart PNG."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import pandas as pd

    _setup_font()

    df = _parse_markdown_table(text_data)
    if df is None or df.empty:
        raise ValueError("无法从数据中提取表格（未找到 markdown 表格）")

    date_col = _find_date_col(df)
    num_cols = _numeric_cols(df, exclude=date_col)[:6]  # max 6 lines

    if not num_cols:
        raise ValueError(f"表格中无数值列（列名：{list(df.columns)}）")

    fig, ax = plt.subplots(figsize=(13, 5))
    fig.patch.set_facecolor("#f5f7fa")
    ax.set_facecolor("#f5f7fa")

    palette = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd", "#8c564b"]

    if date_col:
        x_vals = pd.to_datetime(df[date_col], errors="coerce")
        for i, col in enumerate(num_cols):
            y_vals = _to_numeric(df[col])
            ax.plot(x_vals, y_vals, label=col, color=palette[i % len(palette)],
                    linewidth=1.6, alpha=0.9)
        # x-axis formatting
        span_days = (x_vals.max() - x_vals.min()).days if x_vals.notna().sum() > 1 else 0
        if span_days > 180:
            ax.xaxis.set_major_locator(mdates.MonthLocator())
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        elif span_days > 30:
            ax.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        else:
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d"))
        plt.xticks(rotation=40, ha="right", fontsize=8)
    else:
        # No date column — use first col as category labels
        x_labels = df.iloc[:, 0].tolist()
        x_vals = range(len(x_labels))
        for i, col in enumerate(num_cols):
            y_vals = _to_numeric(df[col])
            ax.plot(list(x_vals), y_vals, label=col, color=palette[i % len(palette)],
                    linewidth=1.6, alpha=0.9, marker="o", markersize=4)
        ax.set_xticks(list(x_vals))
        ax.set_xticklabels(x_labels, rotation=40, ha="right", fontsize=8)

    ax.set_title(title or "数据图表", fontsize=13, pad=10, fontweight="bold")
    if x_label:
        ax.set_xlabel(x_label, fontsize=9)
    if y_label:
        ax.set_ylabel(y_label, fontsize=9)
    if len(num_cols) > 1:
        ax.legend(loc="best", fontsize=8, framealpha=0.7)
    ax.grid(True, alpha=0.25, color="white", linewidth=1)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()


def generate_bar_chart(
    text_data: str,
    title: str = "",
    y_label: str = "",
    x_label: str = "",
) -> bytes:
    """Parse a markdown table from text_data and render as a bar chart PNG."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    _setup_font()

    df = _parse_markdown_table(text_data)
    if df is None or df.empty:
        raise ValueError("无法从数据中提取表格")

    label_col = df.columns[0]
    num_cols = _numeric_cols(df, exclude=label_col)[:3]
    if not num_cols:
        raise ValueError(f"表格中无数值列（列名：{list(df.columns)}）")

    labels = df[label_col].tolist()
    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(max(10, len(labels) * 0.6), 5))
    fig.patch.set_facecolor("#f5f7fa")
    ax.set_facecolor("#f5f7fa")

    palette = ["#1f77b4", "#ff7f0e", "#2ca02c"]
    if len(num_cols) == 1:
        vals = _to_numeric(df[num_cols[0]])
        bars = ax.bar(x, vals, color=palette[0], alpha=0.85, width=0.65)
    else:
        width = 0.75 / len(num_cols)
        for i, col in enumerate(num_cols):
            vals = _to_numeric(df[col])
            ax.bar(x + i * width - 0.375 + width / 2, vals, width=width,
                   label=col, color=palette[i], alpha=0.85)
        ax.legend(fontsize=8, framealpha=0.7)

    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=40, ha="right", fontsize=8)
    ax.set_title(title or "数据图表", fontsize=13, pad=10, fontweight="bold")
    if y_label:
        ax.set_ylabel(y_label, fontsize=9)
    if x_label:
        ax.set_xlabel(x_label, fontsize=9)
    ax.grid(True, axis="y", alpha=0.25, color="white", linewidth=1)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)

    plt.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=140, bbox_inches="tight")
    plt.close(fig)
    return buf.getvalue()
