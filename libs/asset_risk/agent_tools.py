"""Asset Risk agent tool specs, executor and system prompt (Streamlit-free)."""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

AGENT_BASE_SYSTEM = """\
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



def execute_tool(name: str, inputs: dict, engine) -> dict:
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
