"""
Strategy Agent — v2
====================
Upgraded strategy agent that combines:

  1. Quantitative market data (IRR, spreads, capacity mix, Mengxi rankings)
  2. Knowledge intelligence (synthesized docs, Q&A pairs, policy timeline)
  3. Expert memory (accumulated validated insights from prior sessions)
  4. Advanced retrieval (HyDE + re-ranking)

The agent answers strategy questions with the full context of the
platform's knowledge pool rather than just raw DB numbers.
"""
from __future__ import annotations

import os
from typing import Optional

import anthropic
import pandas as pd

from shared.agents.db import run_query

# ── Quantitative data loaders (unchanged from v1) ────────────────────────────


def load_top_provinces(limit: int = 10) -> pd.DataFrame:
    sql = """
    select province,
           irr_total,
           payback_years_total,
           irr_arbitrage,
           cap_payment_irr,
           ancillary_irr
    from bess_province_return_snapshot
    where as_of_date = (select max(as_of_date) from bess_province_return_snapshot)
    order by irr_total desc nulls last
    limit %s
    """
    return run_query(sql, params=[limit])


def load_spread_ts(days: int = 180) -> pd.DataFrame:
    sql = """
    select date, province, spread_cny_per_mwh
    from bess_theoretical_spread_ts
    where date >= current_date - (%s || ' days')::interval
    order by date, province
    """
    return run_query(sql, params=[days])


def load_capacity_mix() -> pd.DataFrame:
    sql = """
    select province, solar_mw, wind_mw, thermal_mw
    from nodal_capacity_mix_snapshot
    """
    return run_query(sql)


def load_mengxi_rank(days: int = 30) -> pd.DataFrame:
    sql = """
    select date, site, profit_cny, rank
    from mengxi_profitability_daily
    where date >= current_date - (%s || ' days')::interval
    order by date, rank
    """
    return run_query(sql, params=[days])


def compute_rank_delta(mx_df: pd.DataFrame) -> pd.DataFrame:
    if mx_df.empty:
        return mx_df
    mx_df = mx_df.copy()
    mx_df["date"] = pd.to_datetime(mx_df["date"])
    latest_date = mx_df["date"].max()
    first_date = latest_date - pd.Timedelta(days=29)
    latest = mx_df[mx_df["date"] == latest_date][["site", "rank"]]
    past = mx_df[mx_df["date"] == first_date][["site", "rank"]]
    merged = latest.merge(past, on="site", suffixes=("_latest", "_past"))
    merged["rank_delta"] = merged["rank_past"] - merged["rank_latest"]
    return merged.sort_values("rank_latest")


def compute_spread_stats(ts_df: pd.DataFrame) -> pd.DataFrame:
    if ts_df.empty:
        return ts_df
    out = (
        ts_df.groupby("province")["spread_cny_per_mwh"]
        .agg(["mean", "std", "min", "max"])
        .reset_index()
        .rename(columns={
            "mean": "spread_mean", "std": "spread_std",
            "min": "spread_min", "max": "spread_max",
        })
    )
    return out.sort_values("spread_mean", ascending=False)


def compute_capacity_bias(cap_df: pd.DataFrame) -> pd.DataFrame:
    if cap_df.empty:
        return cap_df
    out = cap_df.copy()
    out["total_mw"] = out[["solar_mw", "wind_mw", "thermal_mw"]].fillna(0).sum(axis=1)
    out["total_mw"] = out["total_mw"].replace(0, 1)
    out["solar_ratio"] = out["solar_mw"] / out["total_mw"]
    out["wind_ratio"] = out["wind_mw"] / out["total_mw"]
    out["thermal_ratio"] = out["thermal_mw"] / out["total_mw"]
    out["structural_spread_bias"] = (
        out["solar_ratio"] * 1.0
        - out["wind_ratio"] * 0.6
        - out["thermal_ratio"] * 0.3
    )
    return out.sort_values("structural_spread_bias", ascending=False)


def build_strategy_summary() -> dict:
    provinces = load_top_provinces(limit=10)
    spreads = load_spread_ts(days=180)
    spread_stats = compute_spread_stats(spreads)
    cap = compute_capacity_bias(load_capacity_mix())
    mengxi = load_mengxi_rank(days=30)
    rank_delta = compute_rank_delta(mengxi)
    return {
        "top_provinces": provinces,
        "spread_stats": spread_stats,
        "capacity_bias": cap,
        "mengxi_rank_delta": rank_delta,
    }


# ── Quantitative context formatter ────────────────────────────────────────────

def _format_quant_context(summary: dict) -> str:
    lines = ["## Quantitative Market Data\n"]

    top = summary.get("top_provinces", pd.DataFrame())
    if not top.empty:
        lines.append("### Top Provinces by BESS IRR")
        lines.append(top.head(5).to_string(index=False))
        lines.append("")

    spreads = summary.get("spread_stats", pd.DataFrame())
    if not spreads.empty:
        lines.append("### DA-RT Spread Statistics (180d)")
        lines.append(spreads.head(5).to_string(index=False))
        lines.append("")

    cap = summary.get("capacity_bias", pd.DataFrame())
    if not cap.empty:
        lines.append("### Structural Capacity Bias (top 5)")
        cols = [c for c in ["province", "solar_ratio", "wind_ratio",
                             "thermal_ratio", "structural_spread_bias"]
                if c in cap.columns]
        lines.append(cap[cols].head(5).to_string(index=False))
        lines.append("")

    mengxi = summary.get("mengxi_rank_delta", pd.DataFrame())
    if not mengxi.empty:
        lines.append("### Mengxi Sites Rank Delta (30d)")
        lines.append(mengxi.head(5).to_string(index=False))

    return "\n".join(lines)


# ── System prompt ──────────────────────────────────────────────────────────────

_STRATEGY_SYSTEM = """\
You are the Strategy Agent for a professional BESS (Battery Energy Storage)
investment and trading platform operating in China's electricity markets.

Your expertise covers:
- China spot electricity markets (DA/RT mechanisms, ancillary services, FM, AGC)
- BESS investment economics (IRR, payback, arbitrage, capacity payments)
- Provincial market rules, regulation, and policy (NEA, NREC, provincial energy bureaus)
- Dispatch optimization and revenue stacking
- Inner Mongolia, Shanxi, Shandong, Gansu, and other key provinces

You have access to:
1. Live quantitative market data (IRR rankings, spreads, capacity mix)
2. Synthesized knowledge from ~4,000 ingested policy and market documents
3. Expert memory accumulated from validated prior analysis sessions
4. A policy timeline showing regulatory changes with effective dates

Guidelines:
- Be precise and quantitative where data is available
- Cite specific policies, effective dates, and document sources where relevant
- Flag regulatory uncertainty or recent changes that may affect analysis
- Think like a senior energy market analyst advising an investment committee
- If knowledge is insufficient, say so clearly rather than speculating
"""

# ── Main agent function ────────────────────────────────────────────────────────

def run_strategy_agent(
    user_prompt: str,
    api_key: Optional[str] = None,
    app: str = "shared",
    use_knowledge: bool = True,
    use_memory: bool = True,
    stream: bool = False,
) -> str:
    """
    Full strategy agent with quantitative data + knowledge intelligence.

    Args:
        user_prompt: The analyst's question
        api_key: Anthropic API key (falls back to ANTHROPIC_API_KEY env var)
        app: Knowledge scope ('shared' for strategy, 'trader' for trading)
        use_knowledge: Whether to retrieve from knowledge pool
        use_memory: Whether to inject expert memory
        stream: If True, streams and prints response (for CLI use)

    Returns:
        Agent response string
    """
    key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        return "[Error] ANTHROPIC_API_KEY not configured."

    # Build quantitative context
    try:
        summary = build_strategy_summary()
        quant_context = _format_quant_context(summary)
    except Exception:
        quant_context = "## Quantitative Data\n(Unavailable — DB query failed)\n"

    # Build knowledge context using advanced retrieval
    knowledge_context = ""
    if use_knowledge:
        try:
            from services.knowledge_pool.advanced_retrieval import retrieve_for_agent
            knowledge_context = retrieve_for_agent(
                query=user_prompt,
                api_key=key,
                app=app,
                use_hyde=True,
                use_rerank=True,
                top_k=6,
            )
        except Exception as exc:
            knowledge_context = f"(Knowledge retrieval unavailable: {exc})"

    # Inject expert memory
    memory_context = ""
    if use_memory:
        try:
            from services.knowledge_pool.expert_memory import (
                get_relevant_insights,
                inject_expert_memory,
            )
            insights = get_relevant_insights(query=user_prompt, limit=5)
            memory_context = inject_expert_memory(insights)
        except Exception:
            memory_context = ""

    # Compose full context
    context_parts = [quant_context]
    if knowledge_context:
        context_parts.append(knowledge_context)
    if memory_context:
        context_parts.append(memory_context)

    full_context = "\n\n".join(context_parts)

    client = anthropic.Anthropic(api_key=key)

    messages = [
        {
            "role": "user",
            "content": (
                f"## Platform Data Context\n\n{full_context}\n\n"
                f"---\n\n"
                f"## Analyst Question\n\n{user_prompt}"
            ),
        }
    ]

    if stream:
        response_text = ""
        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=_STRATEGY_SYSTEM,
            messages=messages,
        ) as stream_obj:
            for text in stream_obj.text_stream:
                print(text, end="", flush=True)
                response_text += text
        print()
        return response_text
    else:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=_STRATEGY_SYSTEM,
            messages=messages,
        )
        return resp.content[0].text


# ── Backwards-compatible wrapper (used by old app.py) ─────────────────────────

def simple_strategy_memo(user_prompt: str, summary: dict) -> str:
    """Legacy wrapper — upgrades to full knowledge-aware agent."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        return run_strategy_agent(user_prompt, api_key=api_key)
    # Fallback to old behaviour if no API key
    top = summary["top_provinces"]
    top_txt = ", ".join(top["province"].head(3).astype(str).tolist()) if not top.empty else "N/A"
    return (
        f"Strategy Agent\n\nUser request:\n{user_prompt}\n\n"
        f"Top provinces: {top_txt}\n\n"
        "Set ANTHROPIC_API_KEY to enable full knowledge-aware responses."
    )
