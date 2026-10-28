"""Tab 期权价值 (Option Value) — pipeline head of the deal-structurer surface.

Renders the latest ``marketdata.option_value_snapshot`` row for one
(province, asset_type, duration_h) cell — value waterfall (intrinsic →
+extrinsic → +policy → total → capture-adj), Greeks, policy components,
calibration vs the historical perfect-foresight benchmark (BESS) or the
empirical price–volume correlation (wind/solar) — and offers an on-demand
recompute via ``services.option_engine.batch.run_one`` (the ONLY writer of
the snapshot table; the app never hand-writes rows).

Honesty rules (hard): extrinsic is always displayed with its P10/P90 band;
wind/solar rows carry a "筛选级" badge, BESS rows "bid-grade" only when a
calibration row exists; the read-time masking counts from ``mc_params`` are
visible in an expander.

Session contract (consumed by the Project Finance / Deal Pricing tabs and
the agent tools)::

    st.session_state["ov_result"]: dict | None = {
        province, asset_type, duration_h, asof_date,
        intrinsic_yuan_kw, extrinsic_yuan_kw, policy_net_yuan_kw,
        total_yuan_kw, capture_adj_yuan_kw, extrinsic_p10, extrinsic_p90,
        mc_mean_volume_mwh_per_kw,   # None on rows predating the column
        greeks: dict, policy_json: dict, mc_params: dict,
        revenue_paths_yuan: np.ndarray | None,   # None when snapshot-sourced
        power_mw: float,
    }

The acquisition panel below the value section turns the cell's V_asset into
a GO/NO-GO on real money: 许可指标 (permit) mode values a development permit
as a scenario-weighted build option (``build_option.value_permit``); 并购
(M&A) mode values a distressed-asset purchase against its ask
(``build_option.value_acquisition``). Scenario-sourced values are labeled
情景假设/判断 (judgment), never data; the verdict text decomposes which
policy bet carries the value. Its own session contract::

    st.session_state["acq_result"]: dict | None = {
        mode: "permit" | "mna",
        result: PermitResult | AcquisitionResult,
        inputs_echo: dict,   # serializable echo incl. energy basis + volume note
    }

All data assembly lives in pure functions (no streamlit imports) so it is
unit-testable; ``render()`` is the only function that imports streamlit.
Units: values ¥/kW/yr; prices ¥/MWh; capex ¥/kWh; NPV outputs ¥.
"""
from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from libs.option_value.contracts import PermitInputs, PolicyScenario

# spec §8 acceptance band for the calibration ratio (energy-only).
RATIO_LO, RATIO_HI = 0.7, 1.4

# Province selector fallback when readers.list_provinces fails on a live DB.
_FALLBACK_PROVINCES = ["山东", "蒙西", "广东", "山西", "江苏"]

# Asset radio → (asset_type, duration_h). duration_h = 0.0 is the wind/solar
# convention (snapshot DDL comment).
_ASSET_OPTIONS = {
    "BESS 2h": ("bess", 2.0),
    "BESS 4h": ("bess", 4.0),
    "风电 wind": ("wind", 0.0),
    "光伏 solar": ("solar", 0.0),
}


# ---------------------------------------------------------------------------
# Pure helpers (no streamlit) — unit-tested
# ---------------------------------------------------------------------------

def waterfall_data(row: dict) -> list[tuple[str, float | None, tuple | None]]:
    """Waterfall series: (label, value, band) per stage, pinned order.

    Stages: intrinsic, extrinsic, policy, total, capture_adj. ``total`` is
    the SUM of the three components (not the row's rounded total column);
    ``capture_adj`` is displayed absolute, not cumulative; the extrinsic
    stage carries its (P10, P90) band tuple when both are present, else
    None. None inputs pass through as None — no NaN fabrication (wind/solar
    have no capture_adj).
    """
    intr = row.get("intrinsic_yuan_kw")
    extr = row.get("extrinsic_yuan_kw")
    pol = row.get("policy_net_yuan_kw")
    total = (
        intr + extr + pol
        if intr is not None and extr is not None and pol is not None
        else None
    )
    p10, p90 = row.get("extrinsic_p10"), row.get("extrinsic_p90")
    band = (p10, p90) if (p10 is not None and p90 is not None) else None
    return [
        ("intrinsic", intr, None),
        ("extrinsic", extr, band),
        ("policy", pol, None),
        ("total", total, None),
        ("capture_adj", row.get("capture_adj_yuan_kw"), None),
    ]


def calibration_line(row: dict, v_hist_yuan_kw: float | None) -> dict:
    """Engine value vs the historical perfect-foresight benchmark V_hist.

    ``ratio_energy = (intrinsic + extrinsic) / v_hist`` — the spec §8 gate is
    the engine's ENERGY economics against an energy-only benchmark;
    ``in_band`` rides it ([0.7, 1.4]). ``ratio_with_policy`` (total / v_hist)
    is informational only. All ratios None when v_hist is absent/0.
    """
    intr = row.get("intrinsic_yuan_kw")
    extr = row.get("extrinsic_yuan_kw")
    energy = intr + extr if (intr is not None and extr is not None) else None
    ratio = energy / v_hist_yuan_kw if (energy is not None and v_hist_yuan_kw) else None
    total = row.get("total_yuan_kw")
    return {
        "v_hist_yuan_kw": v_hist_yuan_kw,
        "energy_yuan_kw": energy,
        "ratio_energy": ratio,
        "in_band": None if ratio is None else bool(RATIO_LO <= ratio <= RATIO_HI),
        "ratio_with_policy": (
            total / v_hist_yuan_kw if (total is not None and v_hist_yuan_kw) else None
        ),
    }


def cell_label(row: dict, has_calibration: bool) -> str:
    """Grade badge (honesty rule): BESS rows are bid-grade ONLY when a
    calibration row exists; wind/solar and uncalibrated BESS are 筛选级."""
    if row.get("asset_type") == "bess" and has_calibration:
        return "bid-grade（含 V_hist 校准）"
    return "筛选级 screen-grade"


def to_ov_result(row: dict, *, power_mw: float, revenue_paths_yuan=None) -> dict:
    """Normalize a snapshot/recompute row into the ov_result session contract.

    ``policy_net_yuan_kw`` falls back to ``total − intrinsic − extrinsic``
    (exact by the engine's construction) for rows predating the column —
    never fabricated when it cannot be derived. The extrinsic band is NOT
    derivable after the fact: legacy rows keep None and the UI says so.
    ``greeks`` accepts the snapshot's ``greeks_json`` key.
    """
    intr = _f(row.get("intrinsic_yuan_kw"))
    extr = _f(row.get("extrinsic_yuan_kw"))
    total = _f(row.get("total_yuan_kw"))
    pol = _f(row.get("policy_net_yuan_kw"))
    if pol is None and None not in (intr, extr, total):
        pol = total - intr - extr
    return {
        "province": row.get("province"),
        "asset_type": row.get("asset_type"),
        "duration_h": _f(row.get("duration_h")),
        "asof_date": str(row.get("asof_date")),
        "intrinsic_yuan_kw": intr,
        "extrinsic_yuan_kw": extr,
        "policy_net_yuan_kw": pol,
        "total_yuan_kw": total,
        "capture_adj_yuan_kw": _f(row.get("capture_adj_yuan_kw")),
        "extrinsic_p10": _f(row.get("extrinsic_p10")),
        "extrinsic_p90": _f(row.get("extrinsic_p90")),
        "mc_mean_volume_mwh_per_kw": _f(row.get("mc_mean_volume_mwh_per_kw")),
        "greeks": dict(row.get("greeks_json") or row.get("greeks") or {}),
        "policy_json": dict(row.get("policy_json") or {}),
        "mc_params": dict(row.get("mc_params") or {}),
        "revenue_paths_yuan": revenue_paths_yuan,
        "power_mw": float(power_mw),
    }


def resolve_ov_result(row: dict | None, *, prev: dict | None,
                      power_mw: float) -> dict | None:
    """Decide the new ``ov_result`` session value (contract producer).

    A None row (no snapshot / snapshot read raised) yields None — the empty
    state CLEARS the contract: a previous cell's dict must never survive a
    cell switch (review fix round 1). A present row normalizes via
    ``to_ov_result``; a same-cell ``prev`` recompute's ``revenue_paths_yuan``
    is preserved across reruns (the snapshot itself never persists paths).
    The preservation key includes ``power_mw``: paths are scaled
    ×power_mw×1000, so a power change drops them to None (honest) rather
    than silently carrying a stale scale — the UI notes the recompute path.
    """
    if row is None:
        return None
    ov = to_ov_result(row, power_mw=power_mw)
    if prev is not None:
        d_prev, d_ov = prev.get("duration_h"), ov["duration_h"]
        same_cell = (
            prev.get("province") == ov["province"]
            and prev.get("asset_type") == ov["asset_type"]
            and d_prev is not None and d_ov is not None
            and float(d_prev) == float(d_ov)  # 0.0 (wind/solar) is a valid duration
            and str(prev.get("asof_date")) == ov["asof_date"]
            and prev.get("power_mw") == ov["power_mw"]
        )
        if same_cell:
            ov["revenue_paths_yuan"] = prev.get("revenue_paths_yuan")
    return ov


def _f(v) -> float | None:
    return None if v is None else float(v)


# ---------------------------------------------------------------------------
# Acquisition panel — pure helpers (no streamlit) — unit-tested
# ---------------------------------------------------------------------------

# Lever registry: short name (ASSET_MODELS applicable_levers) →
# (PolicyLevers field, display unit). The scenario editor's dropdown is
# filtered to the cell asset's applicable levers and stores FIELD names
# (what PolicyScenario.lever_overrides keys on).
_LEVER_FIELD: dict[str, tuple[str, str]] = {
    "capacity_price": ("capacity_price_yuan_kw_yr", "¥/kW/yr"),
    "cap_comp": ("cap_comp_yuan_kw_yr", "¥/kW/yr"),
    "fr": ("fr_yuan_kw_yr", "¥/kW/yr"),
    "discharge_comp": ("discharge_comp_yuan_kwh", "¥/kWh"),
    "sysopfee": ("sysopfee_yuan_kwh", "¥/kWh"),
    "mechanism_premium": ("mechanism_premium_yuan_mwh", "¥/MWh"),
    "green_cert": ("green_cert_yuan_mwh", "¥/MWh"),
}
_LEVER_FIELDS = frozenset(f for f, _ in _LEVER_FIELD.values())

_ACQ_MAX_SCENARIOS = 4  # build_option.value_permit is designed for n <= 4


def applicable_lever_fields(asset_type: str) -> list[tuple[str, str, str]]:
    """(short_name, PolicyLevers field, unit) for the asset's applicable
    levers, read from the ASSET_MODELS registry (ruling: dropdown filter)."""
    from libs.option_value.asset_models import ASSET_MODELS

    return [(s, *_LEVER_FIELD[s])
            for s in ASSET_MODELS[asset_type].applicable_levers]


def preset_scenario_rows(preset: str, asset_type: str) -> list[dict]:
    """Editor rows for a preset. ``current_policy`` is empty; the other two
    map to the asset's applicable analogue (BESS capacity/FR ↔ renewable
    mechanism/green-cert). All preset names carry the 情景假设 label."""
    d = date(date.today().year + 1, 7, 1)
    if preset == "current_policy":
        return []
    if preset == "capacity_x1.5":
        if asset_type == "bess":
            return [{"name": "容量电价×1.5（情景假设）",
                     "lever": "capacity_price_yuan_kw_yr", "mode": "multiplier",
                     "value": 1.5, "effective_date": d, "probability": 0.5}]
        return [{"name": "机制电价×1.5（情景假设）",
                 "lever": "mechanism_premium_yuan_mwh", "mode": "multiplier",
                 "value": 1.5, "effective_date": d, "probability": 0.5}]
    if preset == "fr_pool_halved":
        if asset_type == "bess":
            return [{"name": "调频池减半（情景假设）", "lever": "fr_yuan_kw_yr",
                     "mode": "multiplier", "value": 0.5,
                     "effective_date": d, "probability": 0.5}]
        return [{"name": "绿证减半（情景假设）", "lever": "green_cert_yuan_mwh",
                 "mode": "multiplier", "value": 0.5,
                 "effective_date": d, "probability": 0.5}]
    return []


def scenario_rows_to_scenarios(rows: list[dict],
                               base_levers_value=0.0) -> list[PolicyScenario]:
    """Convert scenario-editor rows into engine ``PolicyScenario`` objects.

    ``mode == "multiplier"`` scales the lever's base value;
    ``mode == "absolute"`` sets it. ``base_levers_value`` is either a float
    applied to every multiplier row, or a dict {lever_field: base_value}
    (the UI passes the current PolicyLevers values so each row scales its
    OWN lever's base). Same-date scenarios are mutually exclusive reveals:
    their probabilities must sum to <= 1 (mirrors the engine's own guard,
    raised here with the Chinese explanation so the UI never surprises the
    engine). Unknown lever fields are rejected loudly — a bogus key would
    otherwise surface as a dataclasses.replace TypeError deep in the tree.
    """
    scenarios: list[PolicyScenario] = []
    for r in rows:
        lever = str(r.get("lever", ""))
        if lever not in _LEVER_FIELDS:
            raise ValueError(
                f"未知政策杠杆 {lever!r} — 情景编辑器只允许适用杠杆下拉列表中的字段"
            )
        if r.get("mode") == "multiplier":
            base = (
                float(base_levers_value.get(lever, 0.0))
                if isinstance(base_levers_value, dict)
                else float(base_levers_value)
            )
            override = base * float(r["value"])
        else:  # absolute
            override = float(r["value"])
        prob = float(r.get("probability", 0.0))
        if not 0.0 <= prob <= 1.0:
            raise ValueError(
                f"情景 {r.get('name')!r} 的概率 {prob} 超出 [0, 1] 区间"
            )
        scenarios.append(PolicyScenario(
            name=str(r.get("name") or lever),
            lever_overrides={lever: override},
            effective_date=r["effective_date"],
            probability=prob,
        ))
    by_date: dict[date, float] = {}
    for sc in scenarios:
        by_date[sc.effective_date] = by_date.get(sc.effective_date, 0.0) + sc.probability
    for d, total in by_date.items():
        if total > 1.0 + 1e-9:
            raise ValueError(
                f"生效日期 {d} 的情景概率合计 {total:.2%} > 100% — "
                "同一生效日期的情景互为互斥揭示，概率之和不得超过 1"
            )
    return scenarios


def energy_value_from_ov(ov: dict) -> tuple[float | None, str]:
    """Capture-adjusted ENERGY part of the cell for the permit tree
    (ruling 1): ``capture_adj − policy_net`` when capture_adj is present,
    else ``intrinsic + extrinsic``. Returns (¥/kW/yr, basis label) — the
    basis is echoed in the panel's results expander (honesty rule)."""
    ca, pol = ov.get("capture_adj_yuan_kw"), ov.get("policy_net_yuan_kw")
    if ca is not None and pol is not None:
        return (float(ca) - float(pol),
                "capture_adj − policy_net（捕获率调整后的能量部分）")
    intr, extr = ov.get("intrinsic_yuan_kw"), ov.get("extrinsic_yuan_kw")
    if intr is not None and extr is not None:
        return (float(intr) + float(extr),
                "intrinsic + extrinsic（该行无捕获率基准 — 未做捕获率折减）")
    return None, "快照缺少价值分量 — 无法推导能量价值"


def volume_from_ov(ov: dict) -> tuple[float, str]:
    """MC-mean volume (MWh/kW) for volume-based policy levers.

    Preferred source: the snapshot's persisted ``mc_mean_volume_mwh_per_kw``
    column (carried through ``to_ov_result``). Rows predating the column
    carry None → legacy fallback via ``volume_from_mc_params`` (0.0 with the
    loud note). A fresh「按需重算」writes the column and clears the note.
    """
    v = ov.get("mc_mean_volume_mwh_per_kw")
    if v is not None:
        return float(v), "快照 mc_mean_volume_mwh_per_kw（MC 均值路径体量）"
    return volume_from_mc_params(ov.get("mc_params") or {})


def volume_from_mc_params(mc: dict) -> tuple[float, str]:
    """Legacy-row fallback for the MC-mean volume (MWh/kW): the snapshot's
    mc_params when it records one, else 0.0 with a loud note (exact whenever
    all volume-based levers are zero — build_option contract). Since the
    mc_mean_volume_mwh_per_kw column patch the engine persists the volume,
    so「按需重算」backfills it and clears the note."""
    for key in ("volume_mwh_per_kw", "mean_volume_mwh_per_kw", "vol_kw_mean"):
        v = mc.get(key)
        if v is not None:
            return float(v), f"快照 mc_params[{key}]"
    return 0.0, ("快照行无 MC 均值体量（旧版行，早于 "
                 "mc_mean_volume_mwh_per_kw 列）→ 体量型政策杠杆"
                 "（放电补偿/系统运行费/机制电价/绿证）按 0 体量估值，"
                 "这些杠杆全为 0 时结果精确（引擎 build_option 约定）；"
                 "「按需重算」会写入体量并消除本提示")


def permit_verdict_text(result) -> str:
    """Committee one-liner for a PermitResult: GO/NO-GO, net value,
    breakeven permit fee, and WHICH policy bet carries the value — each
    scenario's leave-one-out contribution with its share of the net permit
    value. Accepts both the engine shape (decomposition={"per_scenario":
    {...}, "notes": [...]}) and a flat {name: contribution} dict."""
    verdict = "GO" if result.go else "NO-GO"
    decomp = result.decomposition or {}
    per = decomp.get("per_scenario", decomp)
    lines = [
        f"{verdict}：许可净价值 {result.v_permit_yuan:,.0f} ¥"
        f"（盈亏平衡许可费上限 {result.breakeven_permit_yuan:,.0f} ¥）"
    ]
    if per:
        lines.append("价值来源（政策赌注分解，均为情景假设/判断，非数据）：")
        v = result.v_permit_yuan
        for name, contrib in sorted(per.items(), key=lambda kv: -abs(kv[1])):
            share = f"{contrib / v:.0%}" if v else "—"
            lines.append(f"- {name}: {contrib:,.0f} ¥（占许可净价值 {share}）")
    return "\n".join(lines)


def acquisition_verdict_text(result) -> str:
    """M&A counterpart: GO/NO-GO on margin = fair NPV − ask, with the
    per-uplift NPV decomposition (each uplift a judgment input)."""
    verdict = "GO" if result.go else "NO-GO"
    lines = [
        f"{verdict}：公允 NPV {result.fair_npv_yuan:,.0f} ¥，"
        f"margin（NPV − 要价）{result.margin_yuan:,.0f} ¥"
    ]
    if result.rows:
        lines.append("扭亏提升分解（均为情景假设/判断，非数据）：")
        for r in result.rows:
            lines.append(
                f"- {r['name']}: +{r['annual_yuan_kw']:,.1f} ¥/kW/yr → "
                f"NPV {r['npv_yuan']:,.0f} ¥"
            )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# On-demand recompute (engine pieces, same seed — engine consumed, not modified)
# ---------------------------------------------------------------------------

def _recompute_row_and_paths(eng, province: str, asset_type: str,
                             duration_h: float, n_paths: int,
                             power_mw: float, seed: int = 42):
    """Persist a fresh snapshot via batch.run_one and regenerate the per-path
    revenues under the SAME seed (run_one's row does not carry the paths).

    Chain mirrors batch.run_one exactly (readers → build_expected_curve →
    fit_pca_v2 → simulate_pca_v2 → ASSET_MODELS.value_paths with the engine's
    copula seed), so the regenerated distribution is the one the persisted
    row was valued from. ``revenue_paths_yuan`` = per-kW path revenues
    (¥/kW over the simulated year) × power_mw × 1000 → absolute ¥ for the
    user's asset size. Returns (row, revenue_paths_yuan).
    """
    import dataclasses
    from datetime import date

    from libs.option_value.asset_models import ASSET_MODELS
    from libs.option_value.contracts import PCASimConfig
    from libs.option_value.curves import build_expected_curve
    from libs.option_value.pca_simulator import fit_pca_v2, simulate_pca_v2
    from libs.option_value.value_stack import _CF_COPULA_SEED
    from services.option_engine import batch, readers

    row = batch.run_one(eng, province, asset_type, duration_h,
                        n_paths=n_paths, seed=seed)
    if "skipped" in row:
        raise RuntimeError(
            f"引擎跳过该单元：{row.get('reason')}（历史价格数据不足或有缺失月份）"
        )

    asof = date.today()  # run_one's asof convention: the run date
    series = readers.read_price_series(eng, province, asof,
                                       months=batch._HISTORY_MONTHS)
    curve = build_expected_curve(series, asof, months=batch._HISTORY_MONTHS)
    curve = dataclasses.replace(curve, province=province)
    cfg = PCASimConfig(n_components=batch._N_COMPONENTS,
                       n_paths=int(n_paths), seed=int(seed))
    sim_params = fit_pca_v2(series, curve, cfg)
    sim_days = min(365, curve.n_days)  # ¥/kW/yr basis pin (engine convention)
    paths = simulate_pca_v2(sim_params, curve, sim_days, cfg)

    # Same params run_one valued with (fallback notes discarded — they are
    # already recorded in the persisted row's mc_params).
    params = batch._build_params(eng, province, asset_type, duration_h, [])
    model = ASSET_MODELS[asset_type](params)
    horizon = dataclasses.replace(
        curve, hourly=curve.hourly[: paths.shape[1]], n_days=sim_days
    )
    rev_paths, _vol = model.value_paths(paths, curve=horizon,
                                        seed=_CF_COPULA_SEED)
    unit_kw = float(
        getattr(params, "power_mw", getattr(params, "capacity_mw", 1.0))
    ) * 1000.0
    rev_per_kw = np.asarray(rev_paths, dtype=float) / unit_kw
    revenue_paths_yuan = rev_per_kw * float(power_mw) * 1000.0
    return row, revenue_paths_yuan


# ---------------------------------------------------------------------------
# Streamlit rendering (the only function that imports streamlit)
# ---------------------------------------------------------------------------

def render() -> None:
    import plotly.graph_objects as go
    import streamlit as st

    from services.option_engine import readers, snapshot

    st.header("期权价值 · Option Value")
    st.caption("价值的四个层次：内在价值 → 外在价值（波动率期权）→ 政策净值 → "
               "捕获率调整后。单位：¥/kW/yr。")

    # ── DB guard (graceful degradation: banner, no traceback) ───────────────
    eng = None
    try:
        from sqlalchemy import text
        from services.common.db_utils import get_engine
        eng = get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        st.warning(f"⚠️ 数据库不可用（PGURL 未配置或 RDS 不可达）：{e}\n\n"
                   "期权价值页需要数据库连接；本地运行请先加载 config/.env。")
        return

    # ── (a) selectors ────────────────────────────────────────────────────────
    try:
        provinces = readers.list_provinces(eng) or _FALLBACK_PROVINCES
    except Exception:
        provinces = _FALLBACK_PROVINCES
        st.caption("省份列表读取失败，使用默认五省。")

    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        province = st.selectbox("省份", provinces, key="ov_province")
    with c2:
        asset_label = st.radio("资产类型", list(_ASSET_OPTIONS),
                               horizontal=True, key="ov_asset")
    asset_type, duration_h = _ASSET_OPTIONS[asset_label]
    with c3:
        power_mw = st.number_input("装机功率 power (MW)", 1.0, 2000.0, 100.0,
                                   key="ov_power")

    # ── latest snapshot read (read_latest raises when the table is absent) ──
    row = None
    try:
        row = snapshot.read_latest(eng, province, asset_type, duration_h)
    except Exception:
        row = None  # table not created yet → empty state below

    # ── (b) value display ────────────────────────────────────────────────────
    # Contract producer: empty state clears ov_result (never a stale cell's
    # dict); same-cell paths survive reruns only while power_mw is unchanged.
    prev_ov = st.session_state.get("ov_result")
    ov = resolve_ov_result(row, prev=prev_ov, power_mw=power_mw)
    st.session_state["ov_result"] = ov

    if ov is None:
        st.info("该省份/资产类型暂无价值快照。请运行批量引擎 "
                "（services/option_engine/run_option_batch.py），"
                "或使用下方「按需重算」生成。")
    else:
        if (
            prev_ov
            and prev_ov.get("revenue_paths_yuan") is not None
            and ov["revenue_paths_yuan"] is None
            and prev_ov.get("power_mw") != ov["power_mw"]
        ):
            st.caption(
                f"⚠️ 装机功率已变更：原路径分布按旧功率 "
                f"({prev_ov.get('power_mw')} MW) 缩放，已置空 — "
                "重新「按需重算」以生成新功率下的分布。"
            )

        st.caption(
            f"快照日期 {ov['asof_date']} · 窗口 "
            f"{row.get('window_start')} → {row.get('window_end')}"
        )

        # Calibration first: the grade badge depends on it (BESS only).
        v_hist, cal = None, None
        if asset_type == "bess":
            try:
                v_hist = readers.read_hist_pf_per_kw_yr(
                    eng, province, duration_h,
                    row.get("window_start"), row.get("window_end"),
                )
            except Exception:
                v_hist = None
            cal = calibration_line(ov, v_hist)
        st.subheader(cell_label(ov, has_calibration=v_hist is not None))

        # Value waterfall (extrinsic carries its P10/P90 band, spec §11.17)
        wf = waterfall_data(ov)
        labels = {
            "intrinsic": "内在价值 intrinsic",
            "extrinsic": "外在价值 extrinsic",
            "policy": "政策净值 policy",
            "total": "合计 total",
            "capture_adj": "捕获率调整后",
        }
        shown = [w for w in wf if w[1] is not None]
        fig = go.Figure(go.Waterfall(
            x=[labels[w[0]] for w in shown],
            y=[w[1] for w in shown],
            measure=[
                {"intrinsic": "relative", "extrinsic": "relative",
                 "policy": "relative", "total": "total",
                 "capture_adj": "absolute"}[w[0]]
                for w in shown
            ],
            decreasing={"marker": {"color": "#d62728"}},
            increasing={"marker": {"color": "#2ca02c"}},
            totals={"marker": {"color": "#1f77b4"}},
        ))
        band = wf[1][2]
        if band is not None and ov["extrinsic_yuan_kw"] is not None:
            extr = ov["extrinsic_yuan_kw"]
            top = (ov["intrinsic_yuan_kw"] or 0.0) + extr
            fig.add_trace(go.Scatter(
                x=[labels["extrinsic"]], y=[top], mode="markers",
                marker={"size": 1, "color": "rgba(0,0,0,0)"},
                error_y={
                    "type": "data", "symmetric": False,
                    "array": [max(band[1] - extr, 0.0)],
                    "arrayminus": [max(extr - band[0], 0.0)],
                    "color": "#333333", "thickness": 2, "width": 10,
                },
                name="extrinsic P10–P90", showlegend=True,
            ))
        else:
            st.caption("⚠️ 该快照行不含外在价值 P10/P90 区间（旧版行）— "
                       "请用下方「按需重算」刷新，区间随新快照写入。")
        fig.update_layout(title="价值瀑布 (¥/kW/yr)", height=360)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("外在价值区间为其路径收入分布的 P10–P90（从不以裸点估计展示）。")
        if asset_type != "bess" and ov["capture_adj_yuan_kw"] is None:
            st.caption("风光资产无捕获率基准（capture 管道仅覆盖 BESS）— "
                       "捕获率调整列不适用。")

        # Greeks
        g = ov["greeks"] or {}
        gc = st.columns(4)
        gc[0].metric("Delta（+10¥/MWh）",
                     _fmt(g.get("delta"), "¥/kW/yr"))
        gc[1].metric("Vega（波动 +10%）", _fmt(g.get("vega"), "¥/kW/yr"))
        gc[2].metric("Theta（窗口 −30d）", _fmt(g.get("theta"), "¥/kW/yr"))
        gc[3].metric("政策净值 policy_net",
                     _fmt(ov["policy_net_yuan_kw"], "¥/kW/yr"))
        if g.get("theta_note"):
            st.caption(f"theta 说明：{g['theta_note']}")

        # Policy components (policy_json = PolicyLevers asdict + notes)
        pj = ov["policy_json"] or {}
        if pj:
            with st.expander("政策构成 policy components", expanded=False):
                lever_rows = [
                    ("容量电价 capacity_price", "¥/kW/yr",
                     pj.get("capacity_price_yuan_kw_yr")),
                    ("容量补偿 cap_comp", "¥/kW/yr",
                     pj.get("cap_comp_yuan_kw_yr")),
                    ("调频 FR", "¥/kW/yr", pj.get("fr_yuan_kw_yr")),
                    ("放电补偿 discharge_comp（费率）", "¥/kWh",
                     pj.get("discharge_comp_yuan_kwh")),
                    ("系统运行费 sysopfee（费率）", "¥/kWh",
                     pj.get("sysopfee_yuan_kwh")),
                    ("机制电价溢价 mechanism_premium", "¥/MWh",
                     pj.get("mechanism_premium_yuan_mwh")),
                    ("绿证 green_cert", "¥/MWh", pj.get("green_cert_yuan_mwh")),
                ]
                st.dataframe(
                    pd.DataFrame(
                        [(n, u, v) for n, u, v in lever_rows],
                        columns=["杠杆 lever", "单位", "数值"],
                    ),
                    use_container_width=True, hide_index=True,
                )
                st.caption(
                    f"按均值路径体量估值的政策净值合计："
                    f"{_fmt(ov['policy_net_yuan_kw'], '¥/kW/yr')}"
                )
                if pj.get("notes"):
                    st.caption(f"换算记录：{pj['notes']}")

        # Calibration (BESS) / empirical price–volume correlation (wind/solar)
        if asset_type == "bess" and cal is not None:
            st.subheader("校准 calibration vs V_hist")
            if cal["ratio_energy"] is None:
                st.caption("无 V_hist 基准（bess_capture_daily 缺该窗口数据）"
                           "— 本行按筛选级对待。")
            else:
                cc = st.columns(3)
                cc[0].metric("V_hist（历史完美前瞻 LP）",
                             _fmt(cal["v_hist_yuan_kw"], "¥/kW/yr"))
                cc[1].metric("ratio_energy = (内在+外在)/V_hist",
                             f"{cal['ratio_energy']:.2f}")
                cc[2].metric(
                    "判定（接受带 [0.7, 1.4]）",
                    "✅ 在带内" if cal["in_band"] else "⚠️ 出带",
                )
                if cal["ratio_with_policy"] is not None:
                    st.caption(
                        f"参考：含政策比值 total/V_hist = "
                        f"{cal['ratio_with_policy']:.2f}（V_hist 不含政策堆叠，"
                        "仅作信息列）。"
                    )
        elif asset_type in ("wind", "solar"):
            st.subheader("实测价格-电量相关性")
            corr = None
            try:
                corr = readers.read_price_volume_corr(eng, province, asset_type)
            except Exception:
                corr = None
            if corr is None:
                st.caption(
                    "无实测相关性估计"
                    + ("（solar 无数据源 — 引擎按 0.0 保守处理）"
                       if asset_type == "solar"
                       else "（重叠月份不足或数据缺失）")
                    + "；模拟使用 mc_params 记录的回退值。"
                )
            else:
                st.metric("月均价 × 月度出力 Pearson 相关", f"{corr:.2f}")
                st.caption("负相关 = 高发时段压价（同类相食 cannibalization），"
                           "已计入外在价值符号。")

        # Data-transparency expander: read-time masking counts (hard rule)
        mc = ov["mc_params"] or {}
        with st.expander("数据透明度 · mc_params（掩码计数 / 复现参数）",
                         expanded=False):
            fb = mc.get("fallback") or {}
            units = mc.get("units") or {}
            rt_u, da_u = units.get("rt_price") or {}, units.get("da_price") or {}
            mc1, mc2, mc3, mc4 = st.columns(4)
            mc1.metric("模拟路径 / 种子",
                       f"{mc.get('n_paths', '—')} / {mc.get('seed', '—')}")
            mc2.metric("RT→DA 回退填补小时数", _fmt_int(fb.get("filled")))
            mc3.metric("双缺小时数（未填充）", _fmt_int(fb.get("missing_both")))
            mc4.metric("保留的负电价小时数",
                       _fmt_int(fb.get("negatives_kept")))
            st.caption(
                f"单位换算：rt_price 重标定 "
                f"{_fmt_int(rt_u.get('days_scaled'))}/{_fmt_int(rt_u.get('days_total'))} 天"
                f"，da_price {_fmt_int(da_u.get('days_scaled'))}/"
                f"{_fmt_int(da_u.get('days_total'))} 天 · "
                f"覆盖 {mc.get('coverage_months', '—')} 个月 · "
                f"模拟 {mc.get('sim_days', '—')} 天 · "
                f"clip [{mc.get('clip_floor_yuan_mwh', '—')}, "
                f"{mc.get('clip_cap_yuan_mwh', '—')}] ¥/MWh · "
                f"capture_rate {mc.get('capture_rate', '—')} "
                f"({mc.get('capture_model') or '—'}) · "
                f"git {mc.get('git_sha', '—')}"
            )
            for note in (mc.get("parametric_fallbacks") or []):
                st.caption(f"参数化回退：{note}")
            st.json(mc)

    # ── acquisition panel (permit / M&A) — below the value section ─────────
    st.divider()
    st.subheader("收购决策 · Acquisition（开发指标 / 并购）")

    if ov is None:
        # Contract producer: no cell value → no acquisition result (never
        # leave a stale cell's acq_result behind, same discipline as ov_result).
        st.session_state["acq_result"] = None
        st.info("收购决策以该单元的 V_asset 为标的 — 请先获得价值快照"
                "（上方快照读取或「按需重算」）。")
    else:
        from libs.option_value.build_option import (
            value_acquisition,
            value_permit,
        )

        acq_mode = st.radio("模式", ["许可指标 (permit)", "并购 (M&A)"],
                            horizontal=True, key="acq_mode")

        energy_val, energy_basis = energy_value_from_ov(ov)
        vol_mwh_kw, vol_note = volume_from_ov(ov)
        rte = 0.85 if asset_type == "bess" else 1.0  # 引擎 BESSParams.rte 默认

        base_levers = None
        try:
            base_levers = readers.read_policy_levers(
                eng, province, asset_type, duration_h=duration_h)
        except Exception as e:
            st.warning(f"政策杠杆读取失败：{e} — 收购估值不可用。")
        if base_levers is not None and energy_val is None:
            st.warning(f"无法推导能量价值：{energy_basis}")
            base_levers = None

        if base_levers is None:
            st.session_state["acq_result"] = None
        elif acq_mode == "许可指标 (permit)":
            st.caption("把开发指标视为「建设期权」：政策情景在各自生效日揭示"
                       "（互斥/独立规则见引擎），NPV>0 才建设。能量价值基准与"
                       "体量取值见结果展开栏；hurdle 8% / 寿命 15 年为引擎默认。")
            pc = st.columns(4)
            permit_cost = pc[0].number_input("指标费用 permit cost (¥)", 0.0, 1e10,
                                             5_000_000.0, step=100_000.0,
                                             key="acq_permit_cost")
            validity = pc[1].number_input("有效期 validity (年)", 0.5, 10.0, 2.0,
                                          step=0.5, key="acq_validity")
            capex_unit = "¥/kWh" if asset_type == "bess" else "¥/kW"
            capex_default = (600.0 if asset_type == "bess"
                             else 5_000.0 if asset_type == "wind" else 3_200.0)
            capex = pc[2].number_input(f"capex 今日造价 ({capex_unit})", 0.0, 1e6,
                                       capex_default, step=50.0, key="acq_capex")
            decline_pct = pc[3].number_input("capex 年降 (%/yr)", 0.0, 30.0, 5.0,
                                             step=1.0, key="acq_decline")
            if asset_type != "bess":
                st.caption("风光造价按 ¥/kW 口径：引擎 capex 公式含 ×duration_h，"
                           "风光单元 duration_h=0 → 面板按 duration=1.0 传入"
                           "（纯调用方约定，引擎未改动）。")

            # ── scenario editor (≤4 rows; presets fill; all scenario-sourced
            #    values are 情景假设/判断, never data) ──────────────────────
            st.markdown("**政策情景编辑器**（≤4 个情景；情景价值均为 "
                        "**情景假设/判断**，非数据）")
            pr1, pr2, pr3 = st.columns([2, 1, 1])
            with pr1:
                preset = st.selectbox(
                    "预设", ["current_policy", "capacity_x1.5", "fr_pool_halved"],
                    key="acq_preset")
            with pr2:
                if st.button("载入预设", key="acq_load"):
                    st.session_state["acq_scen_rows"] = preset_scenario_rows(
                        preset, asset_type)
                    for j in range(_ACQ_MAX_SCENARIOS):
                        for f in ("name", "lever", "mode", "value", "date", "prob"):
                            st.session_state.pop(f"acq_scen_{j}_{f}", None)
                    st.rerun()
            with pr3:
                if st.button("清空情景", key="acq_clear"):
                    st.session_state["acq_scen_rows"] = []
                    for j in range(_ACQ_MAX_SCENARIOS):
                        for f in ("name", "lever", "mode", "value", "date", "prob"):
                            st.session_state.pop(f"acq_scen_{j}_{f}", None)
                    st.rerun()

            rows_state = st.session_state.setdefault("acq_scen_rows", [])
            lever_opts = applicable_lever_fields(asset_type)
            field_names = [f for _, f, _ in lever_opts]
            fmt = {f: f"{s} ({f}, {u})" for s, f, u in lever_opts}

            if st.button("＋ 添加情景", key="acq_add",
                         disabled=len(rows_state) >= _ACQ_MAX_SCENARIOS):
                rows_state.append({
                    "name": f"情景{len(rows_state) + 1}（情景假设）",
                    "lever": field_names[0], "mode": "multiplier", "value": 1.0,
                    "effective_date": date(date.today().year + 1, 7, 1),
                    "probability": 0.5,
                })
                for j in range(_ACQ_MAX_SCENARIOS):
                    for f in ("name", "lever", "mode", "value", "date", "prob"):
                        st.session_state.pop(f"acq_scen_{j}_{f}", None)
                st.rerun()

            current_rows = []
            for i, r in enumerate(rows_state):
                ec = st.columns([3, 3, 2, 2, 2, 2, 1])
                name = ec[0].text_input(
                    "名称", value=r.get("name", f"情景{i + 1}（情景假设）"),
                    key=f"acq_scen_{i}_name")
                lever = ec[1].selectbox(
                    "杠杆", field_names,
                    index=(field_names.index(r["lever"])
                           if r.get("lever") in field_names else 0),
                    format_func=lambda f: fmt[f], key=f"acq_scen_{i}_lever")
                mode = ec[2].selectbox(
                    "方式", ["multiplier", "absolute"],
                    index=0 if r.get("mode") == "multiplier" else 1,
                    key=f"acq_scen_{i}_mode")
                val = ec[3].number_input(
                    "值", value=float(r.get("value", 1.0)), step=0.05,
                    format="%.6g", key=f"acq_scen_{i}_value")
                eff = ec[4].date_input(
                    "生效日", value=r.get("effective_date", date.today()),
                    key=f"acq_scen_{i}_date")
                prob = ec[5].slider(
                    "概率", 0.0, 1.0, float(r.get("probability", 0.5)), 0.05,
                    key=f"acq_scen_{i}_prob")
                if ec[6].button("✕", key=f"acq_scen_{i}_del"):
                    rows_state.pop(i)
                    for j in range(_ACQ_MAX_SCENARIOS):
                        for f in ("name", "lever", "mode", "value", "date", "prob"):
                            st.session_state.pop(f"acq_scen_{j}_{f}", None)
                    st.rerun()
                current_rows.append({"name": name, "lever": lever, "mode": mode,
                                     "value": val, "effective_date": eff,
                                     "probability": prob})
            # Sync the scaffold with the widgets so structural edits next
            # rerun (add/delete/preset) rebuild from CURRENT values.
            st.session_state["acq_scen_rows"] = current_rows

            lever_base = {f: float(getattr(base_levers, f))
                          for f in _LEVER_FIELDS}
            try:
                scenarios = scenario_rows_to_scenarios(
                    current_rows, base_levers_value=lever_base)
                inputs = PermitInputs(
                    permit_cost_yuan=float(permit_cost),
                    validity_years=float(validity),
                    capex_yuan_kwh_today=float(capex),
                    capex_decline_rate=float(decline_pct) / 100.0,
                    power_mw=float(power_mw),
                    # 风光 capex 按 ¥/kW 口径 → duration=1.0（调用方约定）
                    duration_h=(float(duration_h) if asset_type == "bess"
                                else 1.0),
                )
                res = value_permit(
                    scenarios, base_levers, float(energy_val), asset_type,
                    float(rte), inputs,
                    volume_mwh_per_kw=float(vol_mwh_kw), asof=date.today())
            except ValueError as e:
                # Engine/converter guard (e.g. same-date probability sum > 1):
                # Chinese explanation, never a traceback.
                st.error(f"情景校验失败：{e}")
                st.session_state["acq_result"] = None
            else:
                echo = {
                    "province": province, "asset_type": asset_type,
                    "duration_h": float(duration_h), "power_mw": float(power_mw),
                    "permit_cost_yuan": float(permit_cost),
                    "validity_years": float(validity),
                    "capex_yuan_kwh_today": float(capex),
                    "capex_decline_rate": float(decline_pct) / 100.0,
                    "capex_duration_convention": (
                        "bess：¥/kWh × duration_h" if asset_type == "bess"
                        else "风光：¥/kW（duration 按 1.0 传入）"),
                    "energy_value_yuan_kw": float(energy_val),
                    "energy_basis": energy_basis,
                    "volume_mwh_per_kw": float(vol_mwh_kw),
                    "volume_note": vol_note,
                    "rte": float(rte), "hurdle": 0.08, "life_years": 15,
                    "asof": str(date.today()),
                    "snapshot_asof": str(ov["asof_date"]),
                    "scenarios": [
                        dict(r, effective_date=str(r["effective_date"]))
                        for r in current_rows
                    ],
                }
                st.session_state["acq_result"] = {
                    "mode": "permit", "result": res, "inputs_echo": echo,
                }
                vc = st.columns(4)
                vc[0].metric("判定", "GO ✅" if res.go else "NO-GO ❌")
                vc[1].metric("许可净价值 v_permit", _fmt(res.v_permit_yuan, "¥"))
                vc[2].metric("盈亏平衡许可费",
                             _fmt(res.breakeven_permit_yuan, "¥"))
                vc[3].metric("Dixit-Pindyck 交叉检验",
                             _fmt(res.dp_check_value, "¥"))
                st.text(permit_verdict_text(res))
                with st.expander("结果明细 · 输入回显 / 能量基准 / 情景假设",
                                 expanded=False):
                    st.caption(f"能量价值基准：{energy_basis} "
                               f"= {energy_val:,.1f} ¥/kW/yr")
                    st.caption(f"体量：{vol_note}（volume = "
                               f"{vol_mwh_kw:,.3f} MWh/kW）")
                    st.caption(f"RTE 假设 {rte}（引擎 BESS 默认；风光不适用）· "
                               f"hurdle 8% / 寿命 15 年（引擎默认）· "
                               f"估值日 {date.today()}（快照 asof "
                               f"{ov['asof_date']}）")
                    for note in (res.decomposition or {}).get("notes", []):
                        st.caption(f"DP 交叉检验说明：{note}")
                    st.json(echo)

        else:  # 并购 (M&A)
            st.caption("困境资产并购：公允 NPV vs 要价。实测锚非空时替代模型"
                       "能量价值（引擎约定）；扭亏提升为判断输入（情景假设）。")
            ma1, ma2 = st.columns(2)
            ask = ma1.number_input("要价 ask (¥)", 0.0, 1e11, 300_000_000.0,
                                   step=1_000_000.0, key="acq_ask")
            use_anchor = ma2.checkbox("使用实测锚 actuals anchor",
                                      key="acq_use_anchor")
            anchor = None
            if use_anchor:
                anchor = ma2.number_input("实测锚 (¥/kW/yr)", 0.0, 1e5, 200.0,
                                          step=10.0, key="acq_anchor")
            st.caption("实测锚口径：引擎实测锚取自资产的结算历史，仅适用于在管"
                       "范围内的资产（蒙西 fleet）— 本期方案仅支持手工输入。")

            st.markdown("**扭亏提升 uplift**（名称 + ¥/kW/yr 增量，≤4 行；"
                        "均为 **情景假设/判断**，非数据）")
            up_state = st.session_state.setdefault("acq_up_rows", [])
            if st.button("＋ 添加提升项", key="acq_up_add",
                         disabled=len(up_state) >= 4):
                up_state.append({"name": f"提升{len(up_state) + 1}", "delta": 0.0})
                for j in range(4):
                    st.session_state.pop(f"acq_up_{j}_name", None)
                    st.session_state.pop(f"acq_up_{j}_delta", None)
                st.rerun()

            uplift_rows = []
            for i, r in enumerate(up_state):
                uc = st.columns([4, 2, 1])
                nm = uc[0].text_input("名称", value=r.get("name", f"提升{i + 1}"),
                                      key=f"acq_up_{i}_name")
                dl = uc[1].number_input("¥/kW/yr", value=float(r.get("delta", 0.0)),
                                        step=5.0, key=f"acq_up_{i}_delta")
                if uc[2].button("✕", key=f"acq_up_{i}_del"):
                    up_state.pop(i)
                    for j in range(4):
                        st.session_state.pop(f"acq_up_{j}_name", None)
                        st.session_state.pop(f"acq_up_{j}_delta", None)
                    st.rerun()
                uplift_rows.append((nm, float(dl)))
            st.session_state["acq_up_rows"] = [
                {"name": n, "delta": d} for n, d in uplift_rows]

            try:
                res = value_acquisition(
                    ask_yuan=float(ask),
                    energy_value_yuan_kw=float(energy_val),
                    levers=base_levers, power_mw=float(power_mw),
                    duration_h=float(duration_h), asset_type=asset_type,
                    rte=float(rte), uplift_rows=uplift_rows,
                    actuals_anchor_yuan_kw=anchor,
                    volume_mwh_per_kw=float(vol_mwh_kw))
            except ValueError as e:
                st.error(f"并购估值失败：{e}")
                st.session_state["acq_result"] = None
            else:
                echo = {
                    "province": province, "asset_type": asset_type,
                    "duration_h": float(duration_h), "power_mw": float(power_mw),
                    "ask_yuan": float(ask),
                    "actuals_anchor_yuan_kw": anchor,
                    "energy_value_yuan_kw": float(energy_val),
                    "energy_basis": (
                        "实测锚（替代模型能量价值）" if anchor is not None
                        else energy_basis),
                    "volume_mwh_per_kw": float(vol_mwh_kw),
                    "volume_note": vol_note,
                    "rte": float(rte), "hurdle": 0.08, "life_years": 15,
                    "snapshot_asof": str(ov["asof_date"]),
                    "uplift_rows": [{"name": n, "delta_yuan_kw_yr": d}
                                    for n, d in uplift_rows],
                }
                st.session_state["acq_result"] = {
                    "mode": "mna", "result": res, "inputs_echo": echo,
                }
                ac = st.columns(3)
                ac[0].metric("判定", "GO ✅" if res.go else "NO-GO ❌")
                ac[1].metric("公允 NPV", _fmt(res.fair_npv_yuan, "¥"))
                ac[2].metric("margin（NPV − 要价）", _fmt(res.margin_yuan, "¥"))
                st.text(acquisition_verdict_text(res))
                with st.expander("结果明细 · 输入回显 / 能量基准 / 情景假设",
                                 expanded=False):
                    st.caption(f"能量价值基准：{echo['energy_basis']} "
                               f"= {energy_val if anchor is None else anchor:,.1f} "
                               "¥/kW/yr")
                    st.caption(f"体量：{vol_note}（volume = "
                               f"{vol_mwh_kw:,.3f} MWh/kW）")
                    st.caption(f"RTE 假设 {rte}（引擎 BESS 默认；风光不适用）· "
                               f"hurdle 8% / 寿命 15 年（引擎默认）· "
                               f"快照 asof {ov['asof_date']}")
                    st.json(echo)

    # ── (c) on-demand recompute ──────────────────────────────────────────────
    st.divider()
    st.subheader("按需重算 on-demand recompute")
    rc1, rc2 = st.columns([1, 3])
    with rc1:
        n_paths = st.selectbox("模拟路径数", [200, 500, 1000], index=2,
                               key="ov_npaths")
    with rc2:
        st.caption("写入快照表（仅经 batch.run_one），耗时约 10–60 秒；"
                   "完成后本页与 ov_result 同步刷新。")

    running = bool(st.session_state.get("_ov_running", False))
    if running:
        st.warning("重算进行中，请稍候…")
    btn = st.button("🔄 重算该单元", type="primary",
                    disabled=running, key="ov_recompute")
    if btn:
        st.session_state["_ov_running"] = True
        try:
            with st.spinner("重算中…（10–60 秒，含快照写入与路径再生成）"):
                new_row, rev_paths = _recompute_row_and_paths(
                    eng, province, asset_type, duration_h,
                    int(n_paths), float(power_mw),
                )
            st.session_state["ov_result"] = to_ov_result(
                new_row, power_mw=power_mw, revenue_paths_yuan=rev_paths
            )
            st.success("重算完成，快照已更新。")
        except Exception as e:
            st.error(f"重算失败：{e}")
        finally:
            st.session_state["_ov_running"] = False
        st.rerun()


def _fmt(v, unit: str) -> str:
    return "—" if v is None else f"{float(v):,.2f} {unit}"


def _fmt_int(v) -> str:
    return "—" if v is None else f"{int(v):,}"
