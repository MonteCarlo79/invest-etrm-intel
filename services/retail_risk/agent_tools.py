"""Retail risk agent tool definitions and executor (no streamlit; shared by Streamlit tab and Hermes headless route)."""
from __future__ import annotations

import pandas as pd
from sqlalchemy import text

tools = [
    {
        "name": "get_retail_margin",
        "description": "Get retail margin breakdown (revenue, procurement, T&D, penalties, net) for a customer or all customers.",
        "input_schema": {
            "type": "object",
            "properties": {
                "customer_id": {"type": "integer", "description": "Customer ID. Omit for all customers."},
            },
            "required": [],
        },
    },
    {
        "name": "get_procurement_coverage",
        "description": "Get procurement coverage ratio (forward-bought / contracted load) for a book.",
        "input_schema": {
            "type": "object",
            "properties": {
                "book_id": {"type": "integer", "description": "Book ID to check coverage for."},
            },
            "required": ["book_id"],
        },
    },
    {
        "name": "get_customer_pnl_ranking",
        "description": "Get customers ranked by net P&L contribution, showing top N.",
        "input_schema": {
            "type": "object",
            "properties": {
                "top_n": {"type": "integer", "description": "Number of top customers to return. Default 10."},
            },
            "required": [],
        },
    },
    {
        "name": "get_contract_expiry_pipeline",
        "description": "Get contracts expiring soon, grouped by province and contract type.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days_ahead": {"type": "integer", "description": "Look ahead days. Default 90."},
            },
            "required": [],
        },
    },
]


def execute_tool(name: str, inputs: dict, engine) -> dict:
    with engine.connect() as conn:
        if name == "get_retail_margin":
            where = "WHERE c.id = :cid" if "customer_id" in inputs else ""
            params = {"cid": inputs["customer_id"]} if "customer_id" in inputs else {}
            df = pd.read_sql(text(f"""
                SELECT
                    c.id, c.name, c.province,
                    SUM(si.amount_cny) FILTER (WHERE si.category = 'retail_revenue') AS revenue_cny,
                    SUM(si.amount_cny) FILTER (WHERE si.category = 'energy_procurement') AS procurement_cny,
                    SUM(si.amount_cny) FILTER (WHERE si.category = 'transmission_distribution') AS tnd_cny,
                    SUM(si.amount_cny) FILTER (WHERE si.category = 'imbalance_penalty') AS penalty_cny,
                    SUM(si.amount_cny) AS net_cny,
                    SUM(si.volume_mwh) AS volume_mwh
                FROM marketdata.rm_customers c
                JOIN marketdata.rm_retail_settlements s ON s.customer_id = c.id
                JOIN marketdata.rm_retail_settlement_items si ON si.settlement_id = s.id
                {where}
                GROUP BY c.id, c.name, c.province
                ORDER BY net_cny DESC
            """), conn, params=params)
            return {"margins": df.to_dict("records"), "total_net_cny": float(df["net_cny"].sum()) if not df.empty else 0}

        elif name == "get_procurement_coverage":
            book_id = inputs["book_id"]
            contracted = conn.execute(text("""
                SELECT COALESCE(SUM(cp.nominated_mwh), 0) AS contracted_mwh
                FROM marketdata.rm_customer_profiles cp
                JOIN marketdata.rm_customer_contracts cc ON cc.customer_id = cp.customer_id
                WHERE cc.bound_asset_id IN (
                    SELECT asset_id FROM marketdata.rm_books WHERE id = :bid
                ) AND cc.contract_status = 'active'
            """), {"bid": book_id}).scalar()

            forward_bought = conn.execute(text("""
                SELECT COALESCE(SUM(volume_mwh), 0) AS forward_mwh
                FROM marketdata.rm_positions
                WHERE book_id = :bid AND direction = 'buy' AND status = 'open'
                  AND (source IS NULL OR source <> 'physical')
            """), {"bid": book_id}).scalar()

            contracted_f = float(contracted or 0)
            forward_f = float(forward_bought or 0)
            ratio = (forward_f / contracted_f) if contracted_f > 0 else 0
            unhedged = max(0, contracted_f - forward_f)
            return {
                "contracted_mwh": contracted_f,
                "forward_bought_mwh": forward_f,
                "coverage_ratio": round(ratio, 4),
                "unhedged_mwh": unhedged,
            }

        elif name == "get_customer_pnl_ranking":
            top_n = int(inputs.get("top_n", 10))
            df = pd.read_sql(text("""
                SELECT c.id, c.name, c.province,
                       SUM(si.amount_cny) AS net_cny,
                       SUM(si.volume_mwh) AS volume_mwh
                FROM marketdata.rm_customers c
                JOIN marketdata.rm_retail_settlements s ON s.customer_id = c.id
                JOIN marketdata.rm_retail_settlement_items si ON si.settlement_id = s.id
                GROUP BY c.id, c.name, c.province
                ORDER BY net_cny DESC
                LIMIT :topn
            """), conn, params={"topn": top_n})
            return {"top_customers": df.to_dict("records"), "total_shown": len(df)}

        elif name == "get_contract_expiry_pipeline":
            days_ahead = int(inputs.get("days_ahead", 90))
            df = pd.read_sql(text("""
                SELECT cc.id, c.name AS customer, c.province, cc.contract_ref,
                       cc.contract_type, cc.end_date, cc.annual_forecast_mwh,
                       (cc.end_date - CURRENT_DATE) AS days_remaining
                FROM marketdata.rm_customer_contracts cc
                JOIN marketdata.rm_customers c ON c.id = cc.customer_id
                WHERE cc.contract_status = 'active'
                  AND cc.end_date BETWEEN CURRENT_DATE AND CURRENT_DATE + :days
                ORDER BY cc.end_date
            """), conn, params={"days": days_ahead})
            return {"expiring_contracts": df.to_dict("records"), "count": len(df), "days_ahead": days_ahead}

    return {"error": f"Unknown tool: {name}"}
