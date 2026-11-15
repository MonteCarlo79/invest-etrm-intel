"""Risk Agent tools for the market-maker central risk book (Streamlit page + Hermes headless).

No streamlit / apps imports. Every tool returns plain dicts: NaN -> None, dates -> ISO strings,
and any month without a curve mark is listed in `marks_missing` (its mtm is None, never 0).
Exposure/MtM cover all months with a per-month `delivered` flag; VaR and Greeks are
forward-only (R16: delivered energy carries no forward price risk).
"""
from __future__ import annotations

import math
from datetime import date, timedelta

import pandas as pd

from libs.curves.store import load_curve, load_spot_hourly
from libs.risk.bess_greeks import bess_monthly_greeks
from libs.risk.exposure import exposure
from libs.risk.var import monthly_price_scenarios, portfolio_var
from services.market_maker.positions import (
    HORIZON_MONTHS, assemble_positions, books_with_province, forward_only, month_start,
)
from services.market_maker.refresh import horizon

SCENARIO_HISTORY_DAYS = 400
BESS_RTE, BESS_VOL = 0.85, 0.6  # same defaults as the VaR & Greeks page

AGENT_BASE_SYSTEM = """\
Your knowledge comes exclusively from the data tools below — never from general training data or external information. Do not state any position, exposure, price, MtM, VaR or Greek figure unless it was returned by a tool call in this conversation.

## Domain definitions
- Units: volumes in MWh, prices in ¥/MWh, MtM / VaR / P&L in ¥.
- Sign convention: buy = long (+), sell = short (-). Generation assets are long, loads are short. net_mwh = physical_mwh + financial_mwh; hedge_ratio = -financial / physical.
- Books: asset books (wind/solar/BESS) and load books (retail). Marks come from the reference forward curve per province, month and TOU block.
- `delivered` = delivery month before the as-of month: no forward price risk. Exposure and MtM show all months; VaR and Greeks are forward-only (as-of month on).
- If marks_missing or provinces_missing are non-empty, say those figures are unavailable — never describe them as zero. A null value means unknown, not zero.
- If a question cannot be answered by a tool, say so rather than guessing.

## Question -> tool
- Which books exist / what they are: get_book_list.
- Net position, physical vs financial, hedge ratio: get_exposure.
- Mark-to-market, which months lack a mark: get_mtm.
- Value-at-risk (historical / parametric): get_var.
- Delta by month and BESS strip Greeks: get_greeks.
- Reference forward curve for a province: get_curve.
Always cite the as-of date returned by the tool. Keep answers concise.
"""

_BOOK = {"book_id": {"type": "integer", "description": "Optional. Omit for all books."}}
tools = [
    {"name": "get_book_list",
     "description": "List risk books: id, name, type (asset/load), asset type, province, capacity.",
     "input_schema": {"type": "object", "properties": {}, "required": []}},
    {"name": "get_exposure",
     "description": "Exposure by book/province/delivery month/TOU block: physical, financial, net MWh, "
                    "hedge ratio, delivered flag. Months without a curve mark are in marks_missing.",
     "input_schema": {"type": "object", "properties": _BOOK, "required": []}},
    {"name": "get_mtm",
     "description": "Mark-to-market (¥) per book/month/block against the reference curve, with total. "
                    "Unmarked months appear in marks_missing and have mtm_cny null.",
     "input_schema": {"type": "object", "properties": _BOOK, "required": []}},
    {"name": "get_var",
     "description": "Portfolio VaR (¥, 1-day and 10-day, historical and parametric) on forward-only net "
                    "delta. Returns provinces_missing / delta_missing_mwh for provinces lacking price history.",
     "input_schema": {"type": "object",
                      "properties": {"confidence": {"type": "number",
                                                    "description": "0.95 (default) or 0.99"}},
                      "required": []}},
    {"name": "get_greeks",
     "description": "Forward-only net delta by month (MWh) and Margrabe spread-call strip Greeks "
                    "(delta, vega) for BESS asset books.",
     "input_schema": {"type": "object", "properties": _BOOK, "required": []}},
    {"name": "get_curve",
     "description": "Latest saved reference forward curve for a province (¥/MWh by month and TOU block).",
     "input_schema": {"type": "object",
                      "properties": {"province": {"type": "string", "description": "e.g. 蒙西"}},
                      "required": ["province"]}},
]


def _today() -> date:
    return date.today()


def _clean(v):
    """NaN / NaT / None -> None; dates -> ISO; numpy scalars -> python."""
    if v is None:
        return None
    if isinstance(v, (pd.Timestamp, date)):
        return None if pd.isna(v) else pd.Timestamp(v).date().isoformat()
    if hasattr(v, "item"):
        v = v.item()
    if isinstance(v, float) and math.isnan(v):
        return None
    return v


def _records(df: pd.DataFrame) -> list[dict]:
    return [{k: _clean(v) for k, v in r.items()} for r in df.to_dict("records")]


def _month_iso(v) -> str:
    return pd.Timestamp(v).date().isoformat()


def _ids(inputs: dict):
    b = inputs.get("book_id")
    return None if b is None else [int(b)]


def _exposure_frame(engine, inputs: dict, as_of: date) -> pd.DataFrame:
    pos = assemble_positions(engine, _ids(inputs), as_of)
    return exposure(pos, load_curve(engine, None, as_of))


def _price_scenarios(engine, province: str, as_of: date) -> pd.Series:
    hourly = load_spot_hourly(engine, province, as_of - timedelta(days=SCENARIO_HISTORY_DAYS), as_of)
    return monthly_price_scenarios(hourly) if not hourly.empty else pd.Series(dtype=float)


def _exposure_payload(exp: pd.DataFrame, as_of: date, cols: list[str]) -> dict:
    exp = exp.copy()
    exp["delivered"] = [pd.Timestamp(m) < pd.Timestamp(month_start(as_of)) for m in exp["delivery_month"]]
    missing = sorted({_month_iso(m) for m in exp.loc[exp["mark_cny_mwh"].isna(), "delivery_month"]})
    return {"as_of": as_of.isoformat(), "records": _records(exp[cols + ["delivered"]]),
            "marks_missing": missing}


def get_book_list(engine, inputs: dict) -> dict:
    books = books_with_province(engine)
    return {"records": _records(books)}


def get_exposure(engine, inputs: dict) -> dict:
    as_of = _today()
    exp = _exposure_frame(engine, inputs, as_of)
    cols = ["book_id", "province", "delivery_month", "tou_block", "physical_mwh", "financial_mwh",
            "net_mwh", "hedge_ratio"]
    return _exposure_payload(exp, as_of, cols)


def get_mtm(engine, inputs: dict) -> dict:
    as_of = _today()
    exp = _exposure_frame(engine, inputs, as_of)
    out = _exposure_payload(exp, as_of, ["book_id", "province", "delivery_month", "tou_block",
                                         "net_mwh", "mark_cny_mwh", "mtm_cny"])
    total = exp["mtm_cny"].sum(min_count=1) if not exp.empty else None
    out["total_mtm_cny"] = _clean(total)
    out["note"] = "total_mtm_cny sums marked rows only; months in marks_missing are excluded, not zero."
    return out


def get_var(engine, inputs: dict) -> dict:
    as_of = _today()
    conf = float(inputs.get("confidence", 0.95))
    exp = forward_only(_exposure_frame(engine, inputs, as_of), as_of)
    deltas = exp.groupby("province")["net_mwh"].sum().to_dict() if not exp.empty else {}
    scen = {}
    for p, d in deltas.items():
        if d != 0:
            s = _price_scenarios(engine, p, as_of)
            if not s.empty:
                scen[p] = s
    out = {"as_of": as_of.isoformat(), "confidence": conf, "net_delta_mwh_by_province": deltas}
    for h in (1, 10):
        r = portfolio_var(deltas, scen, confidence=conf, horizon_days=h)
        out[f"horizon_{h}d"] = {"historical": _clean(r["historical"]),
                                "parametric": _clean(r["parametric"]),
                                "n_scenarios": r["n_scenarios"]}
        if h == 1:
            out["provinces_missing"] = r["provinces_missing"]
            out["delta_missing_mwh"] = r["delta_missing_mwh"]
    out["historical"] = out["horizon_1d"]["historical"]
    out["parametric"] = out["horizon_1d"]["parametric"]
    out["n_scenarios"] = out["horizon_1d"]["n_scenarios"]
    return out


def get_greeks(engine, inputs: dict) -> dict:
    as_of = _today()
    curve = load_curve(engine, None, as_of)
    exp = forward_only(exposure(assemble_positions(engine, _ids(inputs), as_of), curve), as_of)
    delta = []
    if not exp.empty:
        by = exp.groupby(["book_id", "delivery_month"], as_index=False)["net_mwh"].sum()
        delta = _records(by.rename(columns={"net_mwh": "net_delta_mwh"}))
        tot = exp.groupby("delivery_month")["net_mwh"].sum()
        delta_total = [{"delivery_month": _month_iso(m), "net_delta_mwh": _clean(v)} for m, v in tot.items()]
    else:
        delta_total = []
    books = books_with_province(engine)
    bess = books[books["asset_type"] == "bess"]
    if inputs.get("book_id") is not None:
        bess = bess[bess["book_id"] == int(inputs["book_id"])]
    strips = []
    months = horizon(as_of, HORIZON_MONTHS)
    for b in bess.itertuples(index=False):
        if pd.isna(b.capacity_mw) or pd.isna(b.bess_duration_h) or not isinstance(b.province, str):
            strips.append({"book_id": _clean(b.book_id), "error": "capacity_mw / bess_duration_h / province missing"})
            continue
        g = bess_monthly_greeks(float(b.capacity_mw), float(b.bess_duration_h), BESS_RTE, months,
                                curve, b.province, BESS_VOL, BESS_VOL, as_of=as_of)
        strips.append({"book_id": _clean(b.book_id), "province": b.province,
                       "assumptions": {"roundtrip_eff": BESS_RTE, "peak_vol": BESS_VOL,
                                       "offpeak_vol": BESS_VOL},
                       "months": _records(g)})
    return {"as_of": as_of.isoformat(), "delta_by_month": delta_total, "delta_by_book_month": delta,
            "bess_strips": strips}


def get_curve(engine, inputs: dict) -> dict:
    as_of = _today()
    prov = inputs["province"]
    c = load_curve(engine, prov, as_of)
    return {"as_of": as_of.isoformat(), "province": prov, "records": _records(c),
            "note": "" if not c.empty else "No saved curve for this province."}


_DISPATCH = {"get_book_list": get_book_list, "get_exposure": get_exposure, "get_mtm": get_mtm,
             "get_var": get_var, "get_greeks": get_greeks, "get_curve": get_curve}


def execute_tool(name: str, inputs: dict, engine) -> dict:
    """Run a tool; never raises (errors are returned as data so the model can self-correct)."""
    fn = _DISPATCH.get(name)
    if fn is None:
        return {"error": f"Unknown tool: {name}"}
    try:
        return fn(engine, inputs or {})
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}
