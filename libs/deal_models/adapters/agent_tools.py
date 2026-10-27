"""libs/deal_models/adapters/agent_tools.py — Claude API tool definitions + dispatch."""
from __future__ import annotations
import json
import math
from datetime import date, datetime
import numpy as np
from libs.deal_models.contracts import (
    OUParams, PriceSimRequest, DispatchRequest,
    ProjectFinancials, MCRequest,
)
from libs.deal_models.price_simulator import simulate_prices
from libs.deal_models.dispatch_valuation import dispatch_annual
from libs.deal_models.project_cashflow import compute_cashflow
from libs.deal_models.monte_carlo import run_monte_carlo
import libs.deal_models.deal_structures as _ds_mod  # triggers registration

# Option-value engine readers (import-safe: no DB at import; engines are
# created lazily inside the handlers, never here).
from services.option_engine import snapshot as _snapshot
from services.option_engine import readers as _readers
from services.option_engine import calibration as _calibration
from services.option_engine.scout import store as _scout_store
from apps.deal_structurer import screen_tab as _screen_tab

# Cached last MC result so Strategist can reference it across tool calls
_last_mc_result = None
_last_price_paths = None


def _json_default(o):
    if isinstance(o, (np.integer, np.floating)):
        return float(o)
    if isinstance(o, np.ndarray):
        return o.tolist()
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    raise TypeError(f"Not serializable: {type(o)}")


def _j(obj) -> str:
    """JSON serialize; convert numpy scalars."""
    return json.dumps(obj, default=_json_default)


def _json_clean(o):
    """Recursively map non-finite floats to None and dates to ISO strings so
    option-value rows never emit NaN/Inf literals or crash on date objects."""
    if isinstance(o, dict):
        return {k: _json_clean(v) for k, v in o.items()}
    if isinstance(o, (list, tuple)):
        return [_json_clean(v) for v in o]
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, float):
        return o if math.isfinite(o) else None
    return o


def _jo(obj) -> str:
    """JSON for the option-value tools: literal UTF-8 (Chinese province names
    stay readable in the transcript) over pre-cleaned values."""
    return json.dumps(_json_clean(obj), default=_json_default,
                      ensure_ascii=False)


def _lazy_engine():
    """Create the DB engine inside the handler (never at module import).

    Returns None when the engine cannot be constructed (e.g. PGURL unset) so
    stubbed readers in tests still run; a real reader then fails on use and
    the handler surfaces it as a JSON error string.
    """
    try:
        from services.common.db_utils import get_engine
        return get_engine()
    except Exception:
        return None


AGENT_TOOLS = [
    {
        "name": "run_price_simulation",
        "description": "Simulate forward price paths for a province using OU or PCA model. Returns P10/P50/P90 price levels and stores paths for downstream dispatch valuation.",
        "input_schema": {
            "type": "object",
            "properties": {
                "province": {"type": "string", "description": "Province name e.g. 蒙西"},
                "model": {"type": "string", "enum": ["ou", "pca"], "description": "Price model"},
                "kappa": {"type": "number", "description": "OU mean-reversion speed (default 2.0)"},
                "mu": {"type": "number", "description": "OU long-run mean yuan/MWh (default 300)"},
                "sigma": {"type": "number", "description": "OU annualised vol yuan/MWh (default 80)"},
                "n_simulations": {"type": "integer", "description": "Number of paths (default 1000)"},
            },
            "required": ["province"],
        },
    },
    {
        "name": "run_dispatch_valuation",
        "description": "Estimate annual BESS or wind revenue using the last simulated price paths.",
        "input_schema": {
            "type": "object",
            "properties": {
                "asset_type": {"type": "string", "enum": ["bess", "wind", "wind_bess"]},
                "capacity_mwh": {"type": "number"},
                "power_mw": {"type": "number"},
                "roundtrip_eff": {"type": "number"},
                "installed_mw": {"type": "number", "description": "Wind installed capacity MW"},
            },
            "required": ["asset_type"],
        },
    },
    {
        "name": "run_project_cashflow",
        "description": "Compute project IRR, equity IRR, DSCR, and NPV for given financial assumptions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "capex_total_yuan": {"type": "number"},
                "annual_revenue_yuan": {"type": "number", "description": "Single annual revenue figure (repeated over project life)"},
                "annual_om_yuan": {"type": "number"},
                "debt_ratio": {"type": "number"},
                "interest_rate": {"type": "number"},
                "loan_term_years": {"type": "integer"},
                "project_life_years": {"type": "integer"},
                "hurdle_rate": {"type": "number"},
            },
            "required": ["capex_total_yuan", "annual_revenue_yuan", "annual_om_yuan"],
        },
    },
    {
        "name": "run_monte_carlo",
        "description": "Full MC simulation: price paths → dispatch revenue → project cashflow. Returns IRR/NPV distributions and tornado chart.",
        "input_schema": {
            "type": "object",
            "properties": {
                "province": {"type": "string"},
                "asset_type": {"type": "string", "enum": ["bess", "wind", "wind_bess"]},
                "capacity_mwh": {"type": "number"},
                "power_mw": {"type": "number"},
                "installed_mw": {"type": "number"},
                "capex_total_yuan": {"type": "number"},
                "annual_om_yuan": {"type": "number"},
                "n_simulations": {"type": "integer"},
                "mu": {"type": "number", "description": "OU long-run mean price yuan/MWh"},
                "annual_revenue_yuan": {"type": "number", "description": "Base annual revenue yuan for cashflow model"},
            },
            "required": ["province", "asset_type", "capex_total_yuan", "annual_om_yuan"],
        },
    },
    {
        "name": "price_deal_structure",
        "description": "Price a deal structure (floor, cap, collar, swap, tolling, PPA) against the last MC revenue paths.",
        "input_schema": {
            "type": "object",
            "properties": {
                "structure_type": {"type": "string", "enum": ["revenue_floor", "revenue_cap", "collar", "fixed_revenue_swap", "tolling", "ppa_fixed_price"]},
                "floor_yuan": {"type": "number"},
                "cap_yuan": {"type": "number"},
                "fixed_revenue_yuan": {"type": "number"},
                "toll_yuan": {"type": "number"},
                "fixed_price_yuan_mwh": {"type": "number"},
                "annual_volume_mwh": {"type": "number"},
            },
            "required": ["structure_type"],
        },
    },
    {
        "name": "list_deals",
        "description": "List recent deal briefs with their latest analysis result ids and recommendations. Use to find brief_id for other deal tools.",
        "input_schema": {"type": "object", "properties": {
            "limit": {"type": "integer", "description": "Max rows (default 10)"}}},
    },
    {
        "name": "get_deal_result",
        "description": "Get a deal's brief parameters, economics, committee sections (truncated), synthesis and recommendation. Deal can be a brief_id (int) or exact deal_name.",
        "input_schema": {"type": "object", "properties": {
            "deal": {"description": "brief_id (int) or exact deal_name (str)"}},
            "required": ["deal"]},
    },
    {
        "name": "update_deal_parameters",
        "description": "Patch deal brief parameters (whitelist only: province, node, capacity_mw, capacity_mwh, efficiency, cycles_per_day, installed_mw, capex_total_yuan, commissioning_year, tenor_years, debt_ratio, loan_rate, loan_term_years, deal_name, comp_rate_yuan_mwh; comp_rate_yuan_mwh=null resets to register-empirical auto). Always follow with rerun_analysis to recompute.",
        "input_schema": {"type": "object", "properties": {
            "brief_id": {"type": "integer"},
            "updates": {"type": "object", "description": "{field: value} within the whitelist"},
            "challenge_note": {"type": "string", "description": "The user's challenge, recorded in structure_notes"},
        }, "required": ["brief_id", "updates"]},
    },
    {
        "name": "rerun_analysis",
        "description": "Recompute after parameter change. scope: 'economics' (local, default), 'section:<key>' (one committee section), 'synthesis', 'daf' (rebuild PDF), 'full' (all 7 sections, requires confirmed=true). Saves a new versioned result row (rev N).",
        "input_schema": {"type": "object", "properties": {
            "brief_id": {"type": "integer"},
            "scope": {"type": "string"},
            "confirmed": {"type": "boolean", "description": "Required true for scope='full'"},
        }, "required": ["brief_id", "scope"]},
    },
    {
        "name": "read_uploaded_doc",
        "description": "Read a user-uploaded document (投委会 DAF PDF etc.) by page or keyword window. Call without args pattern: first list via error.available when unsure of filename.",
        "input_schema": {"type": "object", "properties": {
            "filename": {"type": "string"},
            "page": {"type": "integer"},
            "query": {"type": "string", "description": "Keyword to locate (returns ±1500 chars)"},
        }, "required": ["filename"]},
    },
    {
        "name": "get_option_value",
        "description": (
            "Latest option-engine value snapshot for one province × asset type × duration: "
            "intrinsic / extrinsic / policy-net / total / capture-adjusted value, extrinsic "
            "P10–P90, greeks, and the V_hist calibration check. All values are ¥/kW/yr as of "
            "the snapshot's asof_date. Returns an error when no snapshot exists (run the "
            "option batch first)."
        ),
        "input_schema": {"type": "object", "properties": {
            "province": {"type": "string", "description": "Province name e.g. 山东"},
            "asset_type": {"type": "string", "enum": ["bess", "wind", "solar"]},
            "duration_h": {"type": "number", "description": "BESS duration hours (e.g. 2.0, 4.0); 0 for wind/solar"},
        }, "required": ["province", "asset_type", "duration_h"]},
    },
    {
        "name": "screen_option_values",
        "description": (
            "Screen all latest option-value snapshot cells (province × asset type × duration) "
            "into an NPV-margin / implied-IRR ranking — the permit/M&A hunt list. sort_by: "
            "'margin' (implied IRR desc, unit-free, the screen tab's pinned sort), "
            "'extrinsic_share' (extrinsic/total desc), 'total' (total value desc). Returns the "
            "top 15 rows, each with grade badge, asof_date and freshness. Values are ¥/kW/yr "
            "as of each row's asof_date; margins use default capex assumptions "
            "(BESS 600 ¥/kWh, wind/solar 5500 ¥/kW, hurdle 8%, life 15y)."
        ),
        "input_schema": {"type": "object", "properties": {
            "sort_by": {"type": "string", "enum": ["margin", "extrinsic_share", "total"],
                        "description": "Ranking key (default 'margin')"},
            "min_freshness_days": {"type": "integer",
                                   "description": "Drop rows whose asof_date is older than this many days (default 14)"},
        }},
    },
    {
        "name": "get_policy_levers",
        "description": (
            "Confirmed policy levers for one province from the capacity-stack register: "
            "capacity price, capacity compensation, frequency-regulation (FR), discharge "
            "compensation and system-operation fee, each with a provenance note. ¥/kW/yr "
            "levers are duration-dependent (duration_h, default 2.0); ¥/kWh levers are rates "
            "(volume applied at valuation time). Confirmed policy only — no fallback defaults."
        ),
        "input_schema": {"type": "object", "properties": {
            "province": {"type": "string", "description": "Province name e.g. 山东"},
            "asset_type": {"type": "string", "enum": ["bess", "wind", "solar"]},
            "duration_h": {"type": "number", "description": "BESS duration hours (default 2.0)"},
        }, "required": ["province", "asset_type"]},
    },
    {
        "name": "list_opportunity_cards",
        "description": (
            "Scout opportunity-card queue (marketdata.option_opportunity_cards) — cards "
            "produced by the generative scout sweeps, newest first by scan_date. The status "
            "filter (default 'new') selects the queue slice: 'new' = awaiting review, "
            "'approved'/'rejected'/'snoozed' = human review decisions, 'carried' = candidate "
            "re-fired but suppressed by the delta gate, 'promoted' = converted to a deal "
            "brief. kind optionally narrows to one scan family ('trading', 'permit', 'mna'). "
            "Each row: id, kind, province, asset_type, validation verdict "
            "(confirmed/weakened/no_evidence/pending), scan_date (the asof of the scan that "
            "produced the card), and thesis truncated to its first 120 chars (null when the "
            "run had no LLM). Returns only DB rows; an empty queue returns a 'no cards' "
            "note, not an error."
        ),
        "input_schema": {"type": "object", "properties": {
            "status": {"type": "string",
                       "enum": ["new", "approved", "rejected", "snoozed", "carried",
                                "promoted"],
                       "description": "Queue slice (default 'new')"},
            "kind": {"type": "string",
                     "description": "Optional scan-kind filter: trading|permit|mna"},
        }},
    },
]


# ---------------------------------------------------------------------------
# Option-value engine tools — DB/engine numbers only, each result stamped
# with the snapshot's asof_date; engines are created lazily per call.
# ---------------------------------------------------------------------------

# Screen defaults mirror screen_tab's pinned sidebar values.
_SCREEN_CAPEX_BESS_YUAN_KWH = 600.0
_SCREEN_CAPEX_REN_YUAN_KW = 5500.0
_SCREEN_HURDLE = 0.08
_SCREEN_LIFE_YEARS = 15
_SCREEN_TOP_N = 15

_SCREEN_KEEP = (
    "province", "asset_type", "duration_h", "asof_date", "freshness", "grade",
    "intrinsic_yuan_kw", "extrinsic_yuan_kw", "extrinsic_p10", "extrinsic_p90",
    "policy_net_yuan_kw", "total_yuan_kw", "capture_adj_yuan_kw",
    "screen_value_yuan_kw", "capex_yuan_kw", "npv_margin_yuan_kwh",
    "npv_margin_yuan_kw", "implied_irr", "irr_uplift", "extrinsic_share",
    "masking",
)


def _asof_age_days(asof, today: date | None = None) -> int | None:
    """Age of an asof value in days; None when unparseable. Same parsing as
    screen_tab.freshness_label (which returns a label, not a number)."""
    if asof is None:
        return None
    if isinstance(asof, str):
        try:
            asof = date.fromisoformat(asof[:10])
        except ValueError:
            return None
    elif isinstance(asof, datetime):
        asof = asof.date()
    elif not isinstance(asof, date):
        return None
    return ((today or date.today()) - asof).days


def _calibration_note(eng, row: dict) -> dict:
    """V_hist calibration check for a BESS snapshot row (spec §8 band on
    (intrinsic+extrinsic)/V_hist); non-BESS rows have no benchmark. Read
    failures degrade to a note — never raise."""
    if row.get("asset_type") != "bess":
        return {"note": "no V_hist benchmark — calibration is BESS-only"}
    try:
        v_hist = _readers.read_hist_pf_per_kw_yr(
            eng, row.get("province"), float(row.get("duration_h")),
            row.get("window_start"), row.get("window_end"))
    except Exception as e:
        return {"note": f"calibration unavailable: {e}", "flag_ok": None}
    if not v_hist:
        return {"note": "no V_hist row over the snapshot window",
                "v_hist_yuan_kw": v_hist, "flag_ok": None}
    intr, extr = row.get("intrinsic_yuan_kw"), row.get("extrinsic_yuan_kw")
    energy = intr + extr if (intr is not None and extr is not None) else None
    ratio = energy / v_hist if energy is not None else None
    return {
        "v_hist_yuan_kw": v_hist,
        "ratio_energy": ratio,
        "flag_ok": (None if ratio is None
                    else bool(_calibration.RATIO_LO <= ratio
                              <= _calibration.RATIO_HI)),
    }


def _tool_get_option_value(inputs: dict) -> str:
    try:
        eng = _lazy_engine()
        row = _snapshot.read_latest(
            eng, inputs["province"], inputs["asset_type"],
            float(inputs["duration_h"]))
    except Exception as e:
        return _jo({"error": f"option snapshot read failed: {e}"})
    if row is None:
        return _jo({"error": "no snapshot — run batch",
                    "hint": "services/option_engine/run_option_batch.py"})
    return _jo({
        "province": row.get("province"),
        "asset_type": row.get("asset_type"),
        "duration_h": float(inputs["duration_h"]),
        "asof_date": row.get("asof_date"),
        "window": [row.get("window_start"), row.get("window_end")],
        "intrinsic_yuan_kw": row.get("intrinsic_yuan_kw"),
        "extrinsic_yuan_kw": row.get("extrinsic_yuan_kw"),
        "extrinsic_p10": row.get("extrinsic_p10"),
        "extrinsic_p90": row.get("extrinsic_p90"),
        "policy_net_yuan_kw": row.get("policy_net_yuan_kw"),
        "total_yuan_kw": row.get("total_yuan_kw"),
        "capture_adj_yuan_kw": row.get("capture_adj_yuan_kw"),
        "greeks": row.get("greeks_json"),
        "calibration": _calibration_note(eng, row),
        "masking": _screen_tab.masking_note(row.get("mc_params")),
        "units": "¥/kW/yr as of asof_date",
    })


def _tool_screen_option_values(inputs: dict) -> str:
    sort_by = inputs.get("sort_by") or "margin"
    if sort_by not in ("margin", "extrinsic_share", "total"):
        return _jo({"error": "sort_by must be margin|extrinsic_share|total, "
                             f"got {sort_by!r}"})
    max_age = int(inputs.get("min_freshness_days", 14))
    try:
        eng = _lazy_engine()
        rows = _screen_tab.read_latest_cells(eng)
    except Exception as e:
        return _jo({"error": f"screen read failed: {e}"})

    # Freshness floor: drop cells staler than min_freshness_days, and cells
    # with no parseable asof (freshness unverifiable).
    fresh = []
    for r in rows:
        age = _asof_age_days(r.get("asof_date"))
        if age is not None and age <= max_age:
            fresh.append(r)
    if not fresh:
        return _jo({"rows": [], "sort_by": sort_by,
                    "min_freshness_days": max_age,
                    "note": "no snapshot cells within the freshness window — "
                            "run services/option_engine/run_option_batch.py"})

    df = _screen_tab.screen_rows_to_frame(
        fresh, capex_yuan_kwh_bess=_SCREEN_CAPEX_BESS_YUAN_KWH,
        capex_yuan_kw_renewable=_SCREEN_CAPEX_REN_YUAN_KW,
        hurdle=_SCREEN_HURDLE, life_years=_SCREEN_LIFE_YEARS)
    records = _json_clean(df.to_dict("records"))

    # extrinsic_share on every row (sort key for one mode, grounding context
    # for the others); None when total is absent/zero.
    for rec in records:
        extr, tot = rec.get("extrinsic_yuan_kw"), rec.get("total_yuan_kw")
        rec["extrinsic_share"] = (extr / tot) if (extr is not None and tot) \
            else None

    def _key(field):
        return lambda rec: (rec.get(field)
                            if rec.get(field) is not None else float("-inf"))

    if sort_by == "extrinsic_share":
        records.sort(key=_key("extrinsic_share"), reverse=True)
    elif sort_by == "total":
        records.sort(key=_key("total_yuan_kw"), reverse=True)
    # sort_by == "margin": the frame's pinned implied_irr-desc order stands.

    n_cells = len(records)
    records = records[:_SCREEN_TOP_N]

    # Grade badges (screen_tab semantics): BESS rows upgrade to bid-grade
    # only when a V_hist calibration row exists over the snapshot's own
    # window; checked only for the emitted rows, never upgraded on doubt.
    windows = {}
    for r in fresh:
        try:
            windows[(r.get("province"), r.get("asset_type"),
                     float(r.get("duration_h") or 0.0))] = (
                r.get("window_start"), r.get("window_end"))
        except (TypeError, ValueError):
            continue
    for rec in records:
        if rec.get("asset_type") != "bess":
            continue
        ws, we = windows.get((rec.get("province"), "bess",
                              float(rec.get("duration_h") or 0.0)),
                             (None, None))
        try:
            has_cal = _readers.read_hist_pf_per_kw_yr(
                eng, rec.get("province"), float(rec.get("duration_h")),
                ws, we) is not None
        except Exception:
            has_cal = False
        rec["grade"] = _screen_tab.grade_badge("bess", has_cal)

    return _jo({
        "rows": [{k: rec.get(k) for k in _SCREEN_KEEP} for rec in records],
        "n_cells": n_cells,
        "n_returned": len(records),
        "sort_by": sort_by,
        "min_freshness_days": max_age,
        "capex_assumptions": {
            "bess_yuan_kwh": _SCREEN_CAPEX_BESS_YUAN_KWH,
            "renewable_yuan_kw": _SCREEN_CAPEX_REN_YUAN_KW,
            "hurdle": _SCREEN_HURDLE,
            "life_years": _SCREEN_LIFE_YEARS,
        },
        "units": "¥/kW/yr as of each row's asof_date",
    })


def _tool_get_policy_levers(inputs: dict) -> str:
    duration_h = float(inputs.get("duration_h", 2.0))
    try:
        eng = _lazy_engine()
        levers = _readers.read_policy_levers(
            eng, inputs["province"], inputs["asset_type"], duration_h)
    except Exception as e:
        return _jo({"error": f"policy lever read failed: {e}"})
    return _jo({
        "province": inputs["province"],
        "asset_type": inputs["asset_type"],
        "duration_h": duration_h,
        "capacity_price_yuan_kw_yr": levers.capacity_price_yuan_kw_yr,
        "cap_comp_yuan_kw_yr": levers.cap_comp_yuan_kw_yr,
        "fr_yuan_kw_yr": levers.fr_yuan_kw_yr,
        "discharge_comp_yuan_kwh": levers.discharge_comp_yuan_kwh,
        "sysopfee_yuan_kwh": levers.sysopfee_yuan_kwh,
        "mechanism_premium_yuan_mwh": levers.mechanism_premium_yuan_mwh,
        "green_cert_yuan_mwh": levers.green_cert_yuan_mwh,
        "notes": levers.notes,
        "units": ("capacity_price/cap_comp/FR are ¥/kW/yr (duration-dependent); "
                  "discharge_comp/sysopfee are ¥/kWh rates; "
                  "mechanism_premium/green_cert are ¥/MWh"),
    })


def _tool_list_opportunity_cards(inputs: dict) -> str:
    """Scout queue reader — DB rows only, newest first (store.list_cards
    orders by scan_date then id). Engine lazy; DB failures surface as a JSON
    error string, never raise through dispatch_tool."""
    status = inputs.get("status") or "new"
    kind = inputs.get("kind") or None
    try:
        eng = _lazy_engine()
        rows = _scout_store.list_cards(eng, status=status, kind=kind)
    except Exception as e:
        return _jo({"error": f"opportunity card read failed: {e}"})
    if not rows:
        return _jo({"rows": [], "status": status, "kind": kind,
                    "note": "no cards in the queue for this filter — run "
                            "services/option_engine/run_scout.py"})
    return _jo({
        "rows": [{
            "id": r.get("id"),
            "kind": r.get("kind"),
            "province": r.get("province"),
            "asset_type": r.get("asset_type"),
            "validation": r.get("validation"),
            "scan_date": r.get("scan_date"),
            "thesis": (r.get("thesis") or "")[:120] or None,
        } for r in rows],
        "n": len(rows),
        "status": status,
        "kind": kind,
    })


def dispatch_tool(name: str, inputs: dict) -> str:
    """Route a tool call from the Claude API to the appropriate function."""
    global _last_mc_result, _last_price_paths
    try:
        if name in ("list_deals", "get_deal_result", "update_deal_parameters",
                    "rerun_analysis", "read_uploaded_doc"):
            from services.deal_structurer import structurer_agent as _sa
            if name == "list_deals":
                return _j(_sa.tool_list_deals(inputs.get("limit", 10)))
            if name == "get_deal_result":
                return _j(_sa.tool_get_deal_result(inputs["deal"]))
            if name == "update_deal_parameters":
                return _j(_sa.tool_update_deal_parameters(
                    int(inputs["brief_id"]), inputs.get("updates") or {},
                    inputs.get("challenge_note", "")))
            if name == "rerun_analysis":
                return _j(_sa.tool_rerun_analysis(
                    int(inputs["brief_id"]), inputs["scope"],
                    bool(inputs.get("confirmed", False)),
                    api_key=__import__("os").environ.get("ANTHROPIC_API_KEY", "")))
            if name == "read_uploaded_doc":
                return _j(_sa.tool_read_uploaded_doc(
                    inputs["filename"], inputs.get("page"), inputs.get("query")))

        if name == "get_option_value":
            return _tool_get_option_value(inputs)
        if name == "screen_option_values":
            return _tool_screen_option_values(inputs)
        if name == "get_policy_levers":
            return _tool_get_policy_levers(inputs)
        if name == "list_opportunity_cards":
            return _tool_list_opportunity_cards(inputs)

        if name == "run_price_simulation":
            ou = OUParams(
                kappa=inputs.get("kappa", 2.0),
                mu=inputs.get("mu", 300.0),
                sigma=inputs.get("sigma", 80.0),
            )
            req = PriceSimRequest(
                province=inputs["province"],
                n_simulations=inputs.get("n_simulations", 500),
                model=inputs.get("model", "ou"),
                ou_params=ou,
            )
            paths = simulate_prices(req)
            _last_price_paths = paths
            return _j({
                "province": req.province,
                "n_simulations": req.n_simulations,
                "p10_yuan_mwh": float(np.percentile(paths, 10)),
                "p50_yuan_mwh": float(np.percentile(paths, 50)),
                "p90_yuan_mwh": float(np.percentile(paths, 90)),
            })

        if name == "run_dispatch_valuation":
            if _last_price_paths is None:
                return _j({"error": "Run run_price_simulation first"})
            req = DispatchRequest(
                asset_type=inputs["asset_type"],
                capacity_mwh=inputs.get("capacity_mwh", 0.0),
                power_mw=inputs.get("power_mw", 0.0),
                installed_mw=inputs.get("installed_mw", 0.0),
                roundtrip_eff=inputs.get("roundtrip_eff", 0.85),
            )
            result = dispatch_annual(_last_price_paths, req)
            return _j({"p10": result.p10, "p50": result.p50, "p90": result.p90, "mean": result.mean})

        if name == "run_project_cashflow":
            n = inputs.get("project_life_years", 20)
            rev = inputs["annual_revenue_yuan"]
            fin = ProjectFinancials(
                capex_total_yuan=inputs["capex_total_yuan"],
                annual_revenue_yuan=[rev] * n,
                annual_om_yuan=inputs["annual_om_yuan"],
                debt_ratio=inputs.get("debt_ratio", 0.7),
                interest_rate=inputs.get("interest_rate", 0.05),
                loan_term_years=inputs.get("loan_term_years", 10),
                project_life_years=n,
                hurdle_rate=inputs.get("hurdle_rate", 0.08),
            )
            cf = compute_cashflow(fin)
            return _j({
                "project_irr": cf.project_irr,
                "equity_irr": cf.equity_irr,
                "roace": cf.roace,
                "dscr_min": cf.dscr_min,
                "dscr_avg": cf.dscr_avg,
                "npv": cf.npv,
                "payback_years": cf.payback_years,
            })

        if name == "run_monte_carlo":
            n_sim = inputs.get("n_simulations", 500)
            n = inputs.get("project_life_years", 20)
            base_rev = inputs.get("annual_revenue_yuan", inputs.get("mu", 300.0) * 50.0 * 8760 * 0.3)
            req = MCRequest(
                price_sim=PriceSimRequest(
                    province=inputs["province"],
                    n_simulations=n_sim,
                    model="ou",
                    ou_params=OUParams(kappa=2.0, mu=inputs.get("mu", 300.0), sigma=80.0),
                ),
                dispatch=DispatchRequest(
                    asset_type=inputs["asset_type"],
                    capacity_mwh=inputs.get("capacity_mwh", 0.0),
                    power_mw=inputs.get("power_mw", 0.0),
                    installed_mw=inputs.get("installed_mw", 0.0),
                ),
                financials=ProjectFinancials(
                    capex_total_yuan=inputs["capex_total_yuan"],
                    annual_revenue_yuan=[base_rev] * n,
                    annual_om_yuan=inputs["annual_om_yuan"],
                ),
                n_simulations=n_sim,
            )
            _last_mc_result = run_monte_carlo(req)
            return _j({
                "revenue_p10": _last_mc_result.revenue_p10,
                "revenue_p50": _last_mc_result.revenue_p50,
                "revenue_p90": _last_mc_result.revenue_p90,
                "equity_irr_p10": _last_mc_result.equity_irr_p10,
                "equity_irr_p50": _last_mc_result.equity_irr_p50,
                "equity_irr_p90": _last_mc_result.equity_irr_p90,
                "irr_prob_below_hurdle": _last_mc_result.irr_prob_below_hurdle,
                "tornado_top3": _last_mc_result.tornado[:3],
            })

        if name == "price_deal_structure":
            if _last_mc_result is None:
                return _j({"error": "Run run_monte_carlo first to get revenue paths"})
            struct = inputs["structure_type"]
            from libs.deal_models.registry import get
            spec = get(struct)
            param_fields = {k: v for k, v in inputs.items() if k != "structure_type" and v is not None}
            params = spec.params_schema(**param_fields)
            result = _ds_mod.price_structure(struct, _last_mc_result.revenue_paths, params)
            return _j({
                "expected_cost": result.expected_cost,
                "p95_cost": result.p95_cost,
                "min_premium": result.min_premium,
                "suggested_premium": result.suggested_premium,
            })

        return _j({"error": f"Unknown tool: {name}"})
    except Exception as e:
        return _j({"error": str(e)})
