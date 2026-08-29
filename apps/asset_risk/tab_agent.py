"""Tab 6 — Agent: Claude-powered risk assistant (platform pattern, v21+).

Platform requirements (CLAUDE.md): domain grounding rule, agent_memory
read/write scoped to app key `asset_risk`, Haiku auto-extract with auto-save,
multi-turn conversation, tool-use loop.

`tools` and `_execute_tool` are imported by services/asset_risk/headless_agent.py
— keep both names and signatures stable.
"""
from __future__ import annotations

import json
import os

import pandas as pd
import streamlit as st
from sqlalchemy import text

_ASSET_RISK_APP_NAME = "asset_risk"
_ASSET_RISK_MEM_KEY = "asset_risk_v1"
_HISTORY_TURNS = 20
_MAX_TOOL_ROUNDS = 6

_AGENT_BASE_SYSTEM = """\
You are the Asset Risk agent for a Chinese electricity trading company's risk app.

## Grounding rule
Your knowledge comes exclusively from the data tools below — never from general
training data or external information. Do not state any P&L, VaR, MtM, position,
or deviation figure unless it was returned by a tool call in this conversation.

## Domain conventions
- Money: CNY (元). Energy volumes: MWh. Prices: ¥/MWh unless stated otherwise.
- Settlement categories: charge_energy (充电电费, negative = cost), discharge_energy
  (放电收入), generation_revenue (发电收入), capacity_compensation (容量补偿),
  penalty (偏差费用), transmission (输配电费), etc.
- Arbitrage income = discharge_energy + generation_revenue + charge_energy.
- Deviation chain: nominated (申报) → DA cleared (日前出清) → RT cleared (实时出清)
  → actual (实际执行).

## Analytical framework
- Unknown book_id → call get_asset_list first.
- P&L by category across all months → get_book_pnl.
- One specific settlement month → get_settlement_summary.
- Positions & unrealised MtM → get_position_mtm. VaR → get_var.
- Nominated-vs-cleared-vs-actual volume deviation → get_deviation_analysis.
- Always call a tool before stating any figure. Respond concisely, in the same
  language as the question (Chinese or English).
"""


tools = [
    {
        "name": "get_book_pnl",
        "description": "Get P&L breakdown by category for a book.",
        "input_schema": {
            "type": "object",
            "properties": {
                "book_id": {"type": "integer"},
            },
            "required": ["book_id"],
        },
    },
    {
        "name": "get_position_mtm",
        "description": "Get current MtM summary with unrealised P&L for a book.",
        "input_schema": {
            "type": "object",
            "properties": {"book_id": {"type": "integer"}},
            "required": ["book_id"],
        },
    },
    {
        "name": "get_var",
        "description": "Get current VaR figures for a book.",
        "input_schema": {
            "type": "object",
            "properties": {"book_id": {"type": "integer"}},
            "required": ["book_id"],
        },
    },
    {
        "name": "get_asset_list",
        "description": "Get list of registered assets and their books.",
        "input_schema": {"type": "object", "properties": {}},
    },
    {
        "name": "get_settlement_summary",
        "description": (
            "Get one settlement month's summary for a book: per-category amounts "
            "and volumes, net P&L, charge/discharge volumes, arbitrage income and "
            "spread. Use for 'how did book X do in month Y' questions."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "book_id": {"type": "integer"},
                "month": {"type": "string", "description": "Settlement month 'YYYY-MM', e.g. '2026-07'"},
            },
            "required": ["book_id", "month"],
        },
    },
    {
        "name": "get_deviation_analysis",
        "description": (
            "Volume deviation chain for a book's linked asset over a date range: "
            "nominated vs DA-cleared vs RT-cleared vs actual energy (MWh), with "
            "deviation splits and count of restricted dispatch intervals."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "book_id": {"type": "integer"},
                "start_date": {"type": "string", "description": "ISO date, e.g. '2026-07-01'"},
                "end_date": {"type": "string", "description": "ISO date, inclusive"},
            },
            "required": ["book_id", "start_date", "end_date"],
        },
    },
]


# ── Agent memory (platform pattern: agent_memory table, app-scoped) ───────────

@st.cache_resource
def _ensure_memory_table(_engine):
    with _engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS marketdata.agent_memory (
                id SERIAL PRIMARY KEY,
                app TEXT NOT NULL DEFAULT 'asset_risk',
                category TEXT NOT NULL,
                subject TEXT NOT NULL,
                content TEXT NOT NULL,
                source TEXT DEFAULT 'manual',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                active BOOLEAN DEFAULT TRUE
            )
        """))
        conn.execute(text("""
            ALTER TABLE marketdata.agent_memory
            ADD COLUMN IF NOT EXISTS app TEXT NOT NULL DEFAULT 'asset_risk'
        """))
    return True


@st.cache_data(ttl=60)
def _load_memories(_engine, _key) -> pd.DataFrame:
    try:
        with _engine.connect() as conn:
            return pd.read_sql(text(
                "SELECT id, category, subject, content, source "
                "FROM marketdata.agent_memory WHERE active AND app = :app ORDER BY id"
            ), conn, params={"app": _ASSET_RISK_APP_NAME})
    except Exception:
        return pd.DataFrame(columns=["id", "category", "subject", "content", "source"])


def _save_memory(engine, category: str, subject: str, content: str, source: str = "manual"):
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO marketdata.agent_memory (app, category, subject, content, source) "
            "VALUES (:app, :cat, :sub, :con, :src)"
        ), {"app": _ASSET_RISK_APP_NAME, "cat": category, "sub": subject,
            "con": content, "src": source})
    _load_memories.clear()


def _delete_memory(engine, memory_id: int):
    with engine.begin() as conn:
        conn.execute(text(
            "UPDATE marketdata.agent_memory SET active = FALSE WHERE id = :mid AND app = :app"
        ), {"mid": memory_id, "app": _ASSET_RISK_APP_NAME})
    _load_memories.clear()


def _build_system(engine) -> str:
    base = _AGENT_BASE_SYSTEM
    mems = _load_memories(engine, _ASSET_RISK_MEM_KEY)
    if not mems.empty:
        mem_lines = "\n".join(
            f"- [{r.category}] {r.subject}: {r.content}" for r in mems.itertuples()
        )
        base += f"\n\n## Analyst preferences & domain knowledge\n{mem_lines}"
    return base


def _extract_memories(user_msg: str, agent_reply: str, api_key: str) -> list[dict]:
    """Haiku extraction of memorable facts/preferences from a turn (v21+ pattern)."""
    from shared.anthropic_client import make_client as _make_client
    try:
        _sys = (
            "Extract memorable analyst preferences or domain facts from the conversation. "
            "Return a JSON array of objects with keys: category (string, e.g. 'preference', "
            "'market_view', 'methodology', 'asset_note', 'red_flag'), "
            "subject (short title ≤8 words), content (one sentence). "
            "Only extract genuinely reusable insights — not one-off data points. "
            "Return [] if nothing is worth remembering."
        )
        resp = _make_client(api_key).messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=512,
            system=_sys,
            messages=[{"role": "user", "content": f"User: {user_msg}\n\nAgent: {agent_reply}"}],
        )
        raw = next((b.text for b in resp.content if hasattr(b, "text")), "[]")
        start, end = raw.find("["), raw.rfind("]")
        if start == -1:
            return []
        return json.loads(raw[start:end + 1])
    except Exception:
        return []


# ── Chat rendering ─────────────────────────────────────────────────────────────

def render_agent(engine):
    """Render agent chat tab."""
    st.subheader("Risk Agent")

    from shared.anthropic_client import is_llm_available
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not is_llm_available(api_key):
        st.warning("No LLM configured (set ANTHROPIC_API_KEY or BEDROCK_REGION). Agent unavailable.")
        return

    try:
        _ensure_memory_table(engine)
    except Exception:
        pass  # non-fatal: agent works without memory

    if "agent_messages" not in st.session_state:
        st.session_state.agent_messages = []

    for msg in st.session_state.agent_messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])

    if prompt := st.chat_input("Ask about your asset risk positions..."):
        st.session_state.agent_messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                history = st.session_state.agent_messages[:-1]
                response = _call_agent(prompt, engine, api_key, history)
                st.markdown(response)
        st.session_state.agent_messages.append({"role": "assistant", "content": response})

        # Auto-extract + auto-save memories (v21+ pattern, no confirmation panel)
        saved = 0
        for item in _extract_memories(prompt, response, api_key):
            if all(k in item for k in ("category", "subject", "content")):
                try:
                    _save_memory(engine, item["category"], item["subject"],
                                 item["content"], source="auto")
                    saved += 1
                except Exception:
                    pass
        if saved:
            st.toast(f"🧠 Saved {saved} new memor{'y' if saved == 1 else 'ies'} to agent memory")

    with st.expander("🧠 Agent memory (manage)"):
        mems = _load_memories(engine, _ASSET_RISK_MEM_KEY)
        if mems.empty:
            st.caption("No memories saved yet.")
        for r in mems.itertuples():
            c1, c2 = st.columns([6, 1])
            c1.markdown(f"**[{r.category}] {r.subject}** — {r.content}")
            if c2.button("Delete", key=f"del_mem_{r.id}"):
                _delete_memory(engine, r.id)
                st.rerun()


def _call_agent(user_message: str, engine, api_key: str, history: list[dict]) -> str:
    """Multi-turn agent call with a tool-use loop."""
    from shared.anthropic_client import make_client as _make_client

    client = _make_client(api_key)
    system = _build_system(engine)

    messages = [{"role": m["role"], "content": m["content"]}
                for m in history[-_HISTORY_TURNS:]]
    messages.append({"role": "user", "content": user_message})

    for _ in range(_MAX_TOOL_ROUNDS):
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=system,
            tools=tools,
            messages=messages,
        )
        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            return _extract_text(response)

        tool_results = []
        for block in response.content:
            if block.type == "tool_use":
                result = _execute_tool(block.name, block.input, engine)
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, default=str),
                })
        if not tool_results:
            return _extract_text(response)
        messages.append({"role": "user", "content": tool_results})

    return "Stopped after reaching the tool-use round limit — please rephrase or narrow the question."


def _extract_text(response) -> str:
    for block in response.content:
        if hasattr(block, "text"):
            return block.text
    return "No response generated."


def _execute_tool(name: str, inputs: dict, engine) -> dict:
    with engine.connect() as conn:
        if name == "get_asset_list":
            df = pd.read_sql(text("""
                SELECT a.id, a.name, a.asset_type, a.province, a.capacity_mw,
                       b.id as book_id, b.name as book_name
                FROM marketdata.rm_assets a
                LEFT JOIN marketdata.rm_books b ON b.asset_id = a.id ORDER BY a.name
            """), conn)
            return {"assets": df.to_dict("records")}

        elif name == "get_book_pnl":
            df = pd.read_sql(text("""
                SELECT si.category, SUM(si.amount_cny) as total_cny, SUM(si.volume_mwh) as total_mwh
                FROM marketdata.rm_settlement_items si
                JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
                WHERE s.book_id = :bid GROUP BY si.category ORDER BY total_cny DESC
            """), conn, params={"bid": inputs["book_id"]})
            return {"pnl_by_category": df.to_dict("records"), "net_pnl": float(df["total_cny"].sum())}

        elif name == "get_position_mtm":
            df = pd.read_sql(text("""
                SELECT direction, volume_mwh, price_cny_mwh, channel, province
                FROM marketdata.rm_positions WHERE book_id = :bid AND status = 'open'
            """), conn, params={"bid": inputs["book_id"]})
            if df.empty:
                return {"positions": [], "total_unrealized": 0}
            from libs.risk.mtm import compute_mtm
            fwd = pd.read_sql(text("""
                SELECT DISTINCT ON (province) province, price_cny_kwh * 1000 as price
                FROM marketdata.rm_forward_curves ORDER BY province, curve_date DESC
            """), conn)
            fwd_prices = dict(zip(fwd["province"], fwd["price"])) if not fwd.empty else {}
            results = compute_mtm(df.to_dict("records"), fwd_prices)
            return {"positions": results, "total_unrealized": sum(r["unrealized_pnl_cny"] for r in results)}

        elif name == "get_var":
            df = pd.read_sql(text("""
                SELECT * FROM marketdata.rm_var_snapshots
                WHERE book_id = :bid ORDER BY snapshot_date DESC LIMIT 1
            """), conn, params={"bid": inputs["book_id"]})
            return df.to_dict("records")[0] if not df.empty else {"message": "No VaR data"}

        elif name == "get_settlement_summary":
            month_start = f"{inputs['month']}-01"
            df = pd.read_sql(text("""
                SELECT si.category, SUM(si.amount_cny) AS amount_cny,
                       SUM(si.volume_mwh) AS volume_mwh
                FROM marketdata.rm_settlement_items si
                JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
                WHERE s.book_id = :bid AND s.settlement_month = :m
                GROUP BY si.category ORDER BY amount_cny DESC
            """), conn, params={"bid": inputs["book_id"], "m": month_start})
            if df.empty:
                return {"message": f"No settlement data for {inputs['month']}"}
            rev_cats = ("discharge_energy", "generation_revenue")
            rev = float(df.loc[df["category"].isin(rev_cats), "amount_cny"].sum())
            cost = float(df.loc[df["category"] == "charge_energy", "amount_cny"].sum())
            dis_vol = float(df.loc[df["category"].isin(rev_cats), "volume_mwh"].sum())
            chg_vol = float(df.loc[df["category"] == "charge_energy", "volume_mwh"].sum())
            arb = rev + cost
            return {
                "month": inputs["month"],
                "by_category": df.to_dict("records"),
                "net_pnl_cny": float(df["amount_cny"].sum()),
                "discharge_mwh": dis_vol,
                "charge_mwh": chg_vol,
                "arb_income_cny": arb,
                "arb_spread_cny_mwh": arb / dis_vol if dis_vol else None,
            }

        elif name == "get_deviation_analysis":
            from datetime import date as _date, timedelta as _td
            start = str(inputs["start_date"])[:10]
            end_excl = str(_date.fromisoformat(str(inputs["end_date"])[:10]) + _td(days=1))
            asset = pd.read_sql(text(
                "SELECT asset_id FROM marketdata.rm_books WHERE id = :bid"
            ), conn, params={"bid": inputs["book_id"]})
            if asset.empty or pd.isna(asset["asset_id"].iloc[0]):
                return {"message": "Book has no linked asset."}
            df = pd.read_sql(text("""
                SELECT COUNT(*) AS intervals,
                       COALESCE(SUM(nominated_mw), 0) / 12 AS nominated_mwh,
                       COALESCE(SUM(da_cleared_mw), 0) / 12 AS da_mwh,
                       COALESCE(SUM(rt_cleared_mw), 0) / 12 AS rt_mwh,
                       COALESCE(SUM(actual_mw), 0) / 12 AS actual_mwh,
                       SUM(CASE WHEN restriction IS NOT NULL THEN 1 ELSE 0 END) AS restricted_intervals
                FROM marketdata.rm_dispatch_chain
                WHERE asset_id = :aid AND interval_start >= :start AND interval_start < :end
            """), conn, params={"aid": int(asset["asset_id"].iloc[0]),
                                "start": start, "end": end_excl})
            r = df.iloc[0]
            if not r["intervals"]:
                return {"message": "No dispatch-chain data in range."}
            nom = float(r["nominated_mwh"])
            da = float(r["da_mwh"])
            rt = float(r["rt_mwh"])
            act = float(r["actual_mwh"])
            return {
                "start_date": start,
                "end_date": str(inputs["end_date"])[:10],
                "intervals": int(r["intervals"]),
                "nominated_mwh": nom,
                "da_cleared_mwh": da,
                "rt_cleared_mwh": rt,
                "actual_mwh": act,
                "da_vs_nominated_mwh": da - nom,
                "rt_vs_da_mwh": rt - da,
                "actual_vs_rt_mwh": act - rt,
                "restricted_intervals": int(r["restricted_intervals"]),
            }

    return {"error": f"Unknown tool: {name}"}
