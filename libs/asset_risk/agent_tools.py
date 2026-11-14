"""Asset Risk agent tool specs, executor and system prompt (Streamlit-free)."""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

AGENT_BASE_SYSTEM = """\
You are the Asset Risk agent for a Chinese electricity trading company's risk app.

## Grounding rule
Your knowledge comes exclusively from the data tools below — never from general
training data or external information. Do not state any P&L, waterfall, diagnostic,
merchant-share or deviation figure unless it was returned by a tool call in this
conversation.

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
- Nominated-vs-cleared-vs-actual volume deviation → get_deviation_analysis.
- P&L waterfall 投资标准→申报→出清→实际 and deviation attribution for a BESS asset
  over selected months → get_waterfall.
- Execution gap, bid failure, restriction windows and non-response defect events
  for a BESS asset over a date range → get_diagnostics.
- Merchant (spot) vs contract/mechanism revenue share for a book → get_merchant_share.
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
    {
        "name": "get_waterfall",
        "description": (
            "BESS P&L waterfall for one asset over selected months: stage totals "
            "(投资标准 LP benchmark → 申报 nominated → 出清 cleared → 实际 settlement bills, "
            "with the three Δ deviation stages) in CNY, plus per-cause deviation "
            "attribution rows (MWh, CNY). Use asset name from get_asset_list, or "
            "'全部资产' for all chain assets."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset": {"type": "string", "description": "rm_assets name, e.g. '悦杭独贵'"},
                "months": {"type": "array", "items": {"type": "string"},
                           "description": "Months 'YYYY-MM', e.g. ['2026-06']"},
            },
            "required": ["asset", "months"],
        },
    },
    {
        "name": "get_diagnostics",
        "description": (
            "Dispatch diagnostics for one BESS asset over a date range, per month: "
            "execution gap (cleared vs actual), bid failure (nominated vs DA cleared), "
            "restriction windows, and the top 20 non-response defect events by lost CNY."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "asset": {"type": "string", "description": "rm_assets name"},
                "start_date": {"type": "string", "description": "ISO date, e.g. '2026-07-01'"},
                "end_date": {"type": "string", "description": "ISO date, inclusive"},
            },
            "required": ["asset", "start_date", "end_date"],
        },
    },
    {
        "name": "get_merchant_share",
        "description": (
            "Merchant (spot) vs contract/mechanism vs services revenue per month for a "
            "book, from settlement bills, with merchant_share = merchant / gross inflows; "
            "plus the latest month with a defined share."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"book_id": {"type": "integer"}},
            "required": ["book_id"],
        },
    },
]


def _clean(v):
    """JSON-safe scalar: NaN/NA → None, numpy → python, Timestamp → str."""
    if v is None:
        return None
    if isinstance(v, (pd.Timestamp,)):
        return str(v)
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        return v
    if hasattr(v, "item"):
        return v.item()
    return v


def _records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    return [{k: _clean(v) for k, v in row.items()} for row in df.to_dict("records")]


def _chain_assets(engine) -> pd.DataFrame:
    """BESS rm_assets eligible for the dispatch-chain tools (same set as the tabs)."""
    from libs.asset_risk.diagnostics import chain_asset_names
    with engine.connect() as conn:
        df = pd.read_sql(text(
            "SELECT id, name, capacity_mw, bess_duration_h, commission_date "
            "FROM marketdata.rm_assets WHERE asset_type = 'bess' ORDER BY name"
        ), conn)
    return df[df["name"].isin(chain_asset_names(engine))].reset_index(drop=True)


def _get_diagnostics(inputs: dict, engine) -> dict:
    from libs.asset_risk.chain import _attach_prices, _load_chain, _load_prices
    from libs.asset_risk.diagnostics import (
        bid_fail_monthly, exec_gap_monthly, find_defect_events, load_maps,
        restriction_monthly,
    )
    assets = _chain_assets(engine)
    name = inputs["asset"]
    sel = assets[assets["name"] == name]
    if sel.empty:
        return {"error": f"Unknown or non-chain BESS asset: {name}. "
                         f"Valid: {', '.join(assets['name'])}"}
    start = str(inputs["start_date"])[:10]
    end = str(pd.Timestamp(str(inputs["end_date"])[:10]) + pd.Timedelta(days=1))[:10]
    plant_map, nom_map, rate_map = load_maps(engine)
    chain = _load_chain(engine, sel["id"].tolist(), start, end,
                        nom_code_map=nom_map, plant_map=plant_map)
    if chain.empty:
        return {"message": "No dispatch-chain data in range."}
    prices = _load_prices(engine, [plant_map[name]], start, end)
    df = _attach_prices(chain, prices, {name: plant_map[name]})
    df["month"] = df["ts"].dt.strftime("%Y-%m")
    events = find_defect_events(df)
    if not events.empty:
        events = events.sort_values("lost_cny", ascending=False).head(20)
    return {
        "exec_gap": _records(exec_gap_monthly(df)),
        "bid_fail": _records(bid_fail_monthly(df)),
        "restriction": _records(restriction_monthly(df, rate_map=rate_map)),
        "defect_events": _records(events),
    }


def _get_waterfall(inputs: dict, engine) -> dict:
    """Same assembly as apps/asset_risk/tab_waterfall.render_waterfall, minus
    Streamlit and caching (Task 16 swaps in the legs store)."""
    from libs.asset_risk import waterfall as wf_mod
    from libs.asset_risk.chain import _attach_prices, _load_chain, _load_prices
    from libs.asset_risk.diagnostics import CAPCOMP_RATE_DEFAULT, load_maps

    months = sorted({str(m)[:7] for m in inputs["months"]})
    if not months:
        return {"error": "months is empty"}
    assets = _chain_assets(engine)
    name = inputs["asset"]
    if name not in ("全部资产", "all"):
        assets = assets[assets["name"] == name]
    if assets.empty:
        return {"error": f"Unknown or non-chain BESS asset: {name}"}

    plant_map, nom_map, rate_map = load_maps(engine)
    month_end = (pd.Timestamp(max(months) + "-01") + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    chain = _load_chain(engine, assets["id"].tolist(), min(months), month_end,
                        nom_code_map=nom_map, plant_map=plant_map)
    if chain.empty:
        return {"message": "No dispatch-chain data for the selected asset/months."}
    chain["month"] = chain["ts"].dt.strftime("%Y-%m")
    df = chain[chain["month"].isin(months)].copy()
    if df.empty:
        return {"message": "No dispatch-chain data for the selected asset/months."}
    prices = _load_prices(engine, [plant_map[a] for a in assets["name"]], min(months), month_end)
    df = _attach_prices(df, prices, {a: plant_map[a] for a in assets["name"]})

    nominated = wf_mod.stage_arbitrage(df, "nominated_mw")
    nominated["cap_cny"] = wf_mod.stage_capacity(nominated["dis_mwh"], nominated["asset"], rate_map)
    cleared = wf_mod.stage_arbitrage(df, "rt_cleared_mw")
    cleared["cap_cny"] = wf_mod.stage_capacity(cleared["dis_mwh"], cleared["asset"], rate_map)

    actual = wf_mod._load_settlement_actuals(engine, assets["id"].tolist(), min(months), None)
    actual = actual[actual["month"].isin(months)] if not actual.empty else actual

    # R7: cleared > 3× billed discharge = unit error, not a feed gap — refuse.
    if not cleared.empty and not actual.empty and "dis_mwh" in actual.columns:
        billed = actual.set_index(["asset", "month"])["dis_mwh"]
        unit_bad = []
        for _, row in cleared.iterrows():
            b = billed.get((row["asset"], row["month"]))
            if b is not None and pd.notna(b) and b > 0 and row["dis_mwh"] / b > 3:
                unit_bad.append(f"{row['asset']} {row['month']}")
        if unit_bad:
            return {"error": "cleared/billed volume ratio > 3 for: " + ", ".join(unit_bad)}

    cleared, cleared_untrusted = wf_mod.guard_cleared_leg(cleared, actual)

    bench_rows = []
    for _, a in assets.iterrows():
        commission_iso = (str(a["commission_date"])[:10]
                          if pd.notna(a.get("commission_date")) else None)
        for month in months:
            rate = rate_map.get(a["name"], CAPCOMP_RATE_DEFAULT)
            r = wf_mod.benchmark_leg(engine, int(a["id"]), month, float(a["capacity_mw"]),
                                     float(a["bess_duration_h"]), commission_iso, rate,
                                     plant_map[a["name"]], asset_name=a["name"],
                                     asset_code=nom_map.get(a["name"]))
            fr = wf_mod.forecast_optimal_leg(engine, int(a["id"]), month,
                                             float(a["capacity_mw"]),
                                             float(a["bess_duration_h"]), commission_iso,
                                             plant_map[a["name"]])
            bench_rows.append({"asset": a["name"], "month": month, **r,
                               "fc_arb_cny": fr.get("arb_cny", 0.0)})
    bench = pd.DataFrame(bench_rows) if bench_rows else pd.DataFrame(
        columns=["asset", "month", "arb_cny", "dis_mwh", "cap_cny"])

    if actual.empty:
        actual = pd.DataFrame([{"asset": n, "month": m, "arb_cny": 0.0, "cap_cny": 0.0,
                                "fee_cny": 0.0, "other_cny": 0.0}
                               for n in assets["name"] for m in months])

    wf = wf_mod.build_waterfall(bench, nominated, cleared, actual)
    stage_totals = {s: float(wf.loc[wf["stage"] == s, "cny"].sum()) for s in wf_mod._STAGES}
    attr = wf_mod.attribution_rows(df, wf, bench, nominated, cleared, actual, bench_rows)
    attr = attr.drop(columns=[c for c in attr.columns if c.endswith("_fmt")])
    return {
        "stage_totals": stage_totals,
        "attribution": _records(attr),
        "cleared_untrusted": [f"{a} {m}" for a, m in cleared_untrusted],
    }


def _get_merchant_share(inputs: dict, engine) -> dict:
    from libs.asset_risk.settlement.analytics import (
        compute_merchant_shares, prefer_canonical_settlements,
    )
    with engine.connect() as conn:
        items_df = pd.read_sql(text("""
            SELECT si.category, si.amount_cny, si.notes, s.settlement_month AS month
            FROM marketdata.rm_settlement_items si
            JOIN marketdata.rm_settlements s ON s.id = si.settlement_id
            WHERE s.book_id = :bid
        """), conn, params={"bid": inputs["book_id"]})
    if items_df.empty:
        return {"monthly": [], "latest": None}
    items_df["month"] = items_df["month"].astype(str).str[:7]
    items_df, _ = prefer_canonical_settlements(items_df)
    monthly, latest = compute_merchant_shares(items_df)
    return {
        "monthly": _records(monthly),
        "latest": None if latest is None else {k: _clean(v) for k, v in latest.items()},
    }


def execute_tool(name: str, inputs: dict, engine) -> dict:
    # Composite tools manage their own connections (the waterfall LP can run long;
    # don't hold an idle pooled connection across it).
    if name == "get_waterfall":
        return _get_waterfall(inputs, engine)
    if name == "get_diagnostics":
        return _get_diagnostics(inputs, engine)
    if name == "get_merchant_share":
        return _get_merchant_share(inputs, engine)

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
            from libs.asset_risk.chain import _load_chain
            from libs.asset_risk.diagnostics import load_maps
            plant_map, nom_map, _ = load_maps(engine)
            chain = _load_chain(engine, [int(asset["asset_id"].iloc[0])], start, end_excl,
                                nom_code_map=nom_map, plant_map=plant_map)
            if chain.empty:
                return {"message": "No dispatch-chain data in range."}

            def _mwh(col):  # MW × 0.25 h per 15-min interval
                return float(chain[col].sum() * 0.25)

            nom = _mwh("nominated_mw")
            da = _mwh("da_cleared_mw")
            rt = _mwh("rt_cleared_mw")
            act = _mwh("actual_mw")
            return {
                "start_date": start,
                "end_date": str(inputs["end_date"])[:10],
                "intervals": int(len(chain)),
                "nominated_mwh": nom,
                "da_cleared_mwh": da,
                "rt_cleared_mwh": rt,
                "actual_mwh": act,
                "da_vs_nominated_mwh": da - nom,
                "rt_vs_da_mwh": rt - da,
                "actual_vs_rt_mwh": act - rt,
                "restricted_intervals": int(chain["restriction"].notna().sum()),
            }

    return {"error": f"Unknown tool: {name}"}
