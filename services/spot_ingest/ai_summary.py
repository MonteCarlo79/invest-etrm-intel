"""
AI-generated daily market summaries for the China spot electricity market.

Uses Claude (claude-sonnet-4-6) to produce a concise English paragraph from:
  - Province-level DA/RT average prices (from spot_daily)
  - Inter-provincial trading highlights (from spot_interprov_flow rows)

Called from apps/spot-watcher/pipeline.py after the DB upsert step.
"""
from __future__ import annotations

import datetime as dt
import logging
import os
from typing import Optional

_log = logging.getLogger(__name__)
_MODEL = "claude-sonnet-4-6"


def _build_prompt(
    report_date: dt.date,
    price_rows: list[dict],
    interprov_rows: list[dict],
) -> str:
    lines: list[str] = [
        f"You are a China electricity market analyst. Write a concise 2–3 paragraph "
        f"English market summary for {report_date.strftime('%d %B %Y')}.",
        "",
        "## Province Spot Prices (yuan/kWh)",
    ]

    da = [(r["province_en"], r["da_avg"]) for r in price_rows if r.get("da_avg") is not None]
    rt = [(r["province_en"], r["rt_avg"]) for r in price_rows if r.get("rt_avg") is not None]

    if da:
        da_s = sorted(da, key=lambda x: x[1])
        avg = sum(v for _, v in da) / len(da)
        lines.append(
            f"DA ({len(da)} markets): avg {avg:.3f}, "
            f"low {da_s[0][1]:.3f} ({da_s[0][0]}), "
            f"high {da_s[-1][1]:.3f} ({da_s[-1][0]})"
        )
    if rt:
        rt_s = sorted(rt, key=lambda x: x[1])
        avg = sum(v for _, v in rt) / len(rt)
        lines.append(
            f"RT ({len(rt)} markets): avg {avg:.3f}, "
            f"low {rt_s[0][1]:.3f} ({rt_s[0][0]}), "
            f"high {rt_s[-1][1]:.3f} ({rt_s[-1][0]})"
        )

    if interprov_rows:
        lines += ["", "## Inter-Provincial Trade (省间现货交易)"]
        for r in interprov_rows:
            prov = r.get("province_cn") or "—"
            price = r.get("price_yuan_kwh")
            p_str = f"{price:.3f}" if price is not None else "—"
            chg = r.get("price_chg_pct")
            chg_str = f" ({chg:+.2f}%)" if chg is not None else ""
            vol = r.get("total_vol_100gwh")
            vol_str = f", total {vol:.3f} 亿kWh" if vol is not None else ""
            lines.append(
                f"  {r['direction']} {r['metric_type']}: {prov} @ {p_str} ¥/kWh{chg_str}{vol_str}"
            )

    lines += [
        "",
        "Cover: (1) overall market conditions and price levels, "
        "(2) notable province spreads or extremes, "
        "(3) inter-provincial trade highlights if available. "
        "Be factual and analytical. Max 200 words.",
    ]
    return "\n".join(lines)


def generate_summary(
    report_date: dt.date,
    price_rows: list[dict],
    interprov_rows: list[dict],
    source_pdf: str = "",
) -> Optional[dict]:
    """
    Generate an AI summary for one report date.

    price_rows:     list of {province_en, da_avg, rt_avg} — from spot_daily
    interprov_rows: list of parsed interprov dicts for the same date

    Returns a dict ready for interprov_upsert.upsert_summary(), or None on failure.
    """
    try:
        import anthropic
    except ImportError:
        _log.warning("[AI] anthropic package not installed; skipping summary")
        return None

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        _log.warning("[AI] ANTHROPIC_API_KEY not set; skipping summary for %s", report_date)
        return None

    prompt = _build_prompt(report_date, price_rows, interprov_rows)

    try:
        client = anthropic.Anthropic(api_key=api_key)
        msg = client.messages.create(
            model=_MODEL,
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        return {
            "report_date":       report_date,
            "summary_text":      msg.content[0].text,
            "model":             _MODEL,
            "prompt_tokens":     msg.usage.input_tokens,
            "completion_tokens": msg.usage.output_tokens,
            "source_pdf":        source_pdf,
        }
    except Exception as exc:
        _log.error("[AI] Summary generation failed for %s: %s", report_date, exc)
        return None
