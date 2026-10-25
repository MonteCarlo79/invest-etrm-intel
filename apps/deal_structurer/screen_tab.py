"""Tab 价值筛选 (Value Screen) — province × asset-type margin ranking.

The permit/M&A hunt list: every latest ``marketdata.option_value_snapshot``
cell (one row per province × asset_type × duration_h, latest asof) is turned
into an NPV margin against user capex and an implied IRR, and ranked by IRR.

Units (pinned, hard): values ¥/kW/yr; BESS margin ¥/kWh-energy —
``annuitize(v) / duration_h − capex_¥kWh`` (1 kW = duration_h kWh; the
¥/kW→¥/kWh conversion is ÷duration_h, NO ×1000 anywhere); wind/solar
(duration_h=0) margin ¥/kW — ``annuitize(v) − capex_¥kW`` — with
``v = capture_adj if not None else total``. The two margin columns coexist;
the ranking key is ``implied_irr(v)`` only (unit-free, comparable across
classes); NaN IRR sorts last and never crashes.

``implied_irr`` inverts the annuity: bisection on
``annuitize(annual, r, life) − capex = 0`` over r ∈ (−0.5, 1.0), tol 1e-4;
None when ``annual × life_years < capex`` (no undiscounted payback → no
meaningful IRR — ratified ruling G1, 2026-10-26: the annuity factor's
singularity near r = −1 would otherwise fabricate a deeply negative root)
or when the bracket has no sign change (IRR beyond +100%). Degenerate inputs
(non-finite, non-positive annual/capex, life ≤ 0) also yield None — a screen
cell must show "n/a", never crash.

``irr_uplift = implied_irr(v) − implied_irr(intrinsic + policy_net)`` (capex
basis matched per class): how much of the IRR is the extrinsic option value
on top of the deterministic-plus-policy stack.

Honesty rules (hard): extrinsic values keep their sign (negative shown red,
never floored) and every row carries its extrinsic P10/P90 columns; wind/solar
rows are 筛选级 screen-grade, BESS rows bid-grade only when a V_hist
calibration row exists (render-time check via
``readers.read_hist_pf_per_kw_yr`` over the snapshot's own window); freshness
is visible per row (``stale (Nd)`` when asof is older than 10 days from
today — pinned ruling) and so are the read-time masking counts from
``mc_params`` (missing/empty mc_params → "no-meta" note, never an error —
pinned ruling).

Degradation ladder (no traceback without DB, dual-environment rule):
DB unreachable → warning banner, tab returns; snapshot table absent or empty
→ empty state with guidance to run the batch; asset_registry / nodal prices
absent → the portfolio-fit section is hidden with a note while the table
still renders.

Portfolio-fit section (carried forward from the retired screening_tab per the
spec §6 consolidation map): the user selects a screen row, sizes it (MW) and
— because screen rows are province-level while the fleet registry and nodal
prices are 蒙西-only — explicitly picks a proxy zone node (判断 judgment
input, labeled as such); ``portfolio_contribution.compute_contribution``
returns the pairwise correlation, vol-per-MW before/after and the
diversification benefit.

All data assembly lives in pure functions (no streamlit imports) so it is
unit-testable; ``render()`` is the only function that imports streamlit.
"""
from __future__ import annotations

import json
import math
from datetime import date, datetime

import numpy as np
import pandas as pd

from libs.option_value.value_stack import annuitize

# Freshness ruling: asof older than this many days from date.today() is stale.
_STALE_DAYS = 10

# Pinned SQL (plan Task 3): latest snapshot row per cell. Also consumed by the
# Structurer agent tool screen_option_values (Task 6) — keep importable.
_LATEST_CELLS_SQL = """
    SELECT DISTINCT ON (province, asset_type, duration_h) *
    FROM marketdata.option_value_snapshot
    ORDER BY province, asset_type, duration_h, asof_date DESC
"""

_IRR_LO, _IRR_HI, _IRR_TOL = -0.5, 1.0, 1e-4


# ---------------------------------------------------------------------------
# Pure helpers (no streamlit) — unit-tested
# ---------------------------------------------------------------------------

def implied_irr(annual_yuan_kw: float, capex_yuan_kw: float,
                life_years: int = 15) -> float | None:
    """IRR of {−capex; annual × life_years} via bisection on
    ``annuitize(annual, r, life) − capex = 0``, r ∈ (−0.5, 1.0), tol 1e-4.

    None (pinned ruling G1) when ``annual × life_years < capex`` — no
    undiscounted payback, so no meaningful IRR (the in-bracket root near the
    r → −1 annuity singularity is a formula artifact) — or when the bracket
    shows no sign change (IRR beyond +100%). Degenerate inputs → None.
    """
    try:
        annual = float(annual_yuan_kw)
        capex = float(capex_yuan_kw)
        life = int(life_years)
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(annual) and math.isfinite(capex)) or life <= 0:
        return None
    if annual <= 0 or capex <= 0:
        return None
    if annual * life < capex:  # G1: no undiscounted payback
        return None

    def f(r: float) -> float:
        return annuitize(annual, r, life) - capex

    lo, hi = _IRR_LO, _IRR_HI
    flo, fhi = f(lo), f(hi)
    if flo == 0.0:
        return lo
    if fhi == 0.0:
        return hi
    if flo * fhi > 0:
        return None  # no sign change → IRR outside the bracket
    for _ in range(200):
        mid = 0.5 * (lo + hi)
        fm = f(mid)
        if hi - lo < _IRR_TOL:
            break
        if flo * fm <= 0:
            hi, fhi = mid, fm
        else:
            lo, flo = mid, fm
    return 0.5 * (lo + hi)


def freshness_label(asof_date, today: date | None = None) -> str:
    """``stale (Nd)`` when asof is older than 10 days from today (pinned
    ruling); ``fresh (Nd)`` otherwise; ``no-asof`` when absent."""
    if asof_date is None:
        return "no-asof"
    if isinstance(asof_date, str):
        try:
            asof = date.fromisoformat(asof_date[:10])
        except ValueError:
            return "no-asof"
    elif isinstance(asof_date, datetime):
        asof = asof_date.date()
    elif isinstance(asof_date, date):
        asof = asof_date
    else:
        return "no-asof"
    ref = today or date.today()
    age = (ref - asof).days
    return f"stale ({age}d)" if age > _STALE_DAYS else f"fresh ({age}d)"


def grade_badge(asset_type: str | None, has_calibration: bool) -> str:
    """Grade badge (honesty rule): BESS rows are bid-grade ONLY when a V_hist
    calibration row exists; wind/solar and uncalibrated BESS are 筛选级."""
    if asset_type == "bess" and has_calibration:
        return "bid-grade（含 V_hist 校准）"
    return "筛选级 screen-grade"


def masking_note(mc_params) -> str:
    """Read-time masking counts from mc_params (honesty rule: visible).

    Missing/empty/unparseable mc_params → "no-meta" (pinned ruling: a note,
    not an error). Otherwise a compact "填补 Xh · 双缺 Yh" summary of the
    RT→DA fallback fill / both-missing hour counts.
    """
    mc = mc_params
    if isinstance(mc, str):
        try:
            mc = json.loads(mc)
        except Exception:
            mc = None
    if not isinstance(mc, dict) or not mc:
        return "no-meta"
    fb = mc.get("fallback") or {}
    filled, missing = fb.get("filled"), fb.get("missing_both")
    if filled is None and missing is None:
        return "no-meta"
    return f"填补 {int(filled or 0)}h · 双缺 {int(missing or 0)}h"


def screen_rows_to_frame(rows: list[dict], capex_yuan_kwh_bess: float,
                         capex_yuan_kw_renewable: float, hurdle: float,
                         life_years: int) -> pd.DataFrame:
    """Assemble the margin-ranked screen frame from latest-per-cell rows.

    Pinned unit-correct margins: BESS per-kWh-energy margin
    ``annuitize(v) / duration_h − capex_yuan_kwh_bess`` (÷duration_h, NO
    ×1000); wind/solar (duration_h=0) per-kW margin
    ``annuitize(v) − capex_yuan_kw_renewable``; ``v = capture_adj if not None
    else total``. Sorted by ``implied_irr(v)`` descending (unit-free,
    comparable across classes); NaN IRR last. Rows with no derivable value
    keep None margins/IRR — never fabricated, never crash.
    """
    out: list[dict] = []
    for row in rows:
        asset_type = row.get("asset_type")
        duration_h = _f(row.get("duration_h"))
        duration_h = 0.0 if duration_h is None else duration_h
        is_bess = asset_type == "bess"

        capture_adj = _f(row.get("capture_adj_yuan_kw"))
        total = _f(row.get("total_yuan_kw"))
        v = capture_adj if capture_adj is not None else total
        intr = _f(row.get("intrinsic_yuan_kw"))
        pol = _f(row.get("policy_net_yuan_kw"))
        base = intr + pol if (intr is not None and pol is not None) else None

        if is_bess:
            capex_kw = (
                float(capex_yuan_kwh_bess) * duration_h if duration_h > 0 else None
            )
            margin_kwh = (
                annuitize(v, hurdle, life_years) / duration_h
                - float(capex_yuan_kwh_bess)
                if (v is not None and duration_h > 0)
                else None
            )
            margin_kw = None
        else:
            capex_kw = float(capex_yuan_kw_renewable)
            margin_kw = (
                annuitize(v, hurdle, life_years) - float(capex_yuan_kw_renewable)
                if v is not None
                else None
            )
            margin_kwh = None

        irr_v = (
            implied_irr(v, capex_kw, life_years)
            if (v is not None and capex_kw)
            else None
        )
        irr_base = (
            implied_irr(base, capex_kw, life_years)
            if (base is not None and capex_kw)
            else None
        )
        uplift = (
            irr_v - irr_base
            if (irr_v is not None and irr_base is not None)
            else None
        )

        out.append({
            "province": row.get("province"),
            "asset_type": asset_type,
            "duration_h": duration_h,
            "asof_date": None if row.get("asof_date") is None else str(row.get("asof_date")),
            "freshness": freshness_label(row.get("asof_date")),
            "grade": grade_badge(asset_type, bool(row.get("has_calibration"))),
            "intrinsic_yuan_kw": intr,
            "extrinsic_yuan_kw": _f(row.get("extrinsic_yuan_kw")),
            "extrinsic_p10": _f(row.get("extrinsic_p10")),
            "extrinsic_p90": _f(row.get("extrinsic_p90")),
            "policy_net_yuan_kw": pol,
            "total_yuan_kw": total,
            "capture_adj_yuan_kw": capture_adj,
            "screen_value_yuan_kw": v,
            "capex_yuan_kw": capex_kw,
            "npv_margin_yuan_kwh": margin_kwh,
            "npv_margin_yuan_kw": margin_kw,
            "implied_irr": irr_v,
            "irr_uplift": uplift,
            "masking": masking_note(row.get("mc_params")),
            "mc_params": row.get("mc_params"),  # carried for the detail expander
        })
    df = pd.DataFrame(out)
    if df.empty:
        return df
    # Sort by implied IRR descending; NaN last (mergesort = stable).
    return df.sort_values(
        "implied_irr", ascending=False, na_position="last", kind="mergesort"
    ).reset_index(drop=True)


def read_latest_cells(eng) -> list[dict]:
    """Latest snapshot row per (province, asset_type, duration_h) cell.

    Raises when the table is absent — the caller's degradation ladder catches
    Exception and renders the empty state. Pure query via the caller's engine.
    """
    from sqlalchemy import text

    with eng.connect() as conn:
        rows = conn.execute(text(_LATEST_CELLS_SQL)).mappings().fetchall()
    return [dict(r) for r in rows]


def cell_label(row) -> str:
    """Display label for one screen row: 省份 资产类型 时长."""
    at = row["asset_type"] if not isinstance(row, dict) else row.get("asset_type")
    dur = row["duration_h"] if not isinstance(row, dict) else row.get("duration_h")
    prov = row["province"] if not isinstance(row, dict) else row.get("province")
    if at == "bess":
        return f"{prov} BESS {float(dur):g}h"
    name = {"wind": "风电", "solar": "光伏"}.get(at, str(at))
    return f"{prov} {name}"


def _f(v) -> float | None:
    """float(v), with None and non-finite (NaN/Inf) mapped to None — the
    screen never propagates NaN into margins, IRRs or the sort."""
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


# ---------------------------------------------------------------------------
# Streamlit rendering (the only function that imports streamlit)
# ---------------------------------------------------------------------------

def render() -> None:
    import plotly.graph_objects as go
    import streamlit as st

    st.header("价值筛选 · Value Screen")
    st.caption(
        "各省 × 资产类型最新价值快照 → 造价边际与隐含 IRR 排名（许可/并购猎取清单）。"
        "口径：价值 ¥/kW/yr；BESS 边际 ¥/kWh（= annuitize(v)÷时长 − 造价¥/kWh，不含 ×1000）；"
        "风光边际 ¥/kW（= annuitize(v) − 造价¥/kW）；v = capture_adj（缺省回退 total）；"
        "排名键为 implied IRR（跨类可比，无量纲），NaN 排末。"
    )

    # ── DB guard (graceful degradation: banner, no traceback) ───────────────
    try:
        from sqlalchemy import text
        from services.common.db_utils import get_engine
        eng = get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as e:
        st.warning(f"⚠️ 数据库不可用（PGURL 未配置或 RDS 不可达）：{e}\n\n"
                   "价值筛选页需要数据库连接；本地运行请先加载 config/.env。")
        return

    # ── sidebar form: capex / hurdle / life inputs ───────────────────────────
    with st.sidebar.form("screen_params"):
        st.markdown("**价值筛选参数**")
        capex_bess = st.number_input("BESS 造价 capex (¥/kWh)", 0.0, 5000.0,
                                     600.0, step=10.0, key="screen_capex_bess")
        capex_ren = st.number_input("风光造价 capex (¥/kW)", 0.0, 20000.0,
                                    5500.0, step=100.0, key="screen_capex_ren")
        hurdle = st.number_input("hurdle 折现率", 0.0, 0.50, 0.08, step=0.01,
                                 format="%.4f", key="screen_hurdle")
        life = st.number_input("寿命 life (年)", 1, 40, 15, step=1,
                               key="screen_life")
        st.form_submit_button("重算排名", use_container_width=True)

    # ── latest-per-cell snapshot rows (table absent → empty state) ───────────
    try:
        rows = read_latest_cells(eng)
    except Exception:
        rows = []  # table not created yet → empty state below

    if not rows:
        st.info("暂无价值快照。请运行批量引擎 "
                "（services/option_engine/run_option_batch.py）生成各省×资产类型快照后，"
                "本页自动出现排名表。")
        st.caption("组合契合度分析在存在快照行后可用。")
        return

    # ── BESS calibration existence → bid-grade badge (best-effort; any read
    #    failure leaves the row 筛选级 — honesty rule: never upgrade on doubt) ─
    from services.option_engine import readers
    for r in rows:
        if r.get("asset_type") == "bess":
            try:
                vh = readers.read_hist_pf_per_kw_yr(
                    eng, r["province"], float(r["duration_h"]),
                    r.get("window_start"), r.get("window_end"),
                )
                r["has_calibration"] = vh is not None
            except Exception:
                r["has_calibration"] = False

    df = screen_rows_to_frame(rows, capex_yuan_kwh_bess=float(capex_bess),
                              capex_yuan_kw_renewable=float(capex_ren),
                              hurdle=float(hurdle), life_years=int(life))

    # ── margin-ranked table (extrinsic red when negative; badges; freshness) ──
    show = df.copy()
    show["extrinsic_band"] = show.apply(
        lambda r: (
            f"[{r['extrinsic_p10']:,.0f}, {r['extrinsic_p90']:,.0f}]"
            if r["extrinsic_p10"] is not None and r["extrinsic_p90"] is not None
            else "—"
        ),
        axis=1,
    )
    cols = [
        "province", "asset_type", "duration_h", "grade", "freshness",
        "intrinsic_yuan_kw", "extrinsic_yuan_kw", "extrinsic_band",
        "policy_net_yuan_kw", "total_yuan_kw", "capture_adj_yuan_kw",
        "screen_value_yuan_kw", "capex_yuan_kw",
        "npv_margin_yuan_kwh", "npv_margin_yuan_kw",
        "implied_irr", "irr_uplift", "masking", "asof_date",
    ]
    styled = (
        show[cols]
        .style
        .map(lambda v: "color: #d62728" if (v is not None and v < 0) else "",
             subset=["extrinsic_yuan_kw"])
        .format({
            "duration_h": "{:g}",
            "intrinsic_yuan_kw": "{:,.0f}",
            "extrinsic_yuan_kw": "{:,.0f}",
            "policy_net_yuan_kw": "{:,.0f}",
            "total_yuan_kw": "{:,.0f}",
            "capture_adj_yuan_kw": "{:,.0f}",
            "screen_value_yuan_kw": "{:,.0f}",
            "capex_yuan_kw": "{:,.0f}",
            "npv_margin_yuan_kwh": "{:,.0f}",
            "npv_margin_yuan_kw": "{:,.0f}",
            "implied_irr": "{:.1%}",
            "irr_uplift": "{:+.1%}",
        }, na_rep="—")
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)
    st.caption(
        "外在价值（extrinsic）带符号展示，负值标红、从不截断为 0；"
        "extrinsic_band 为其路径分布 P10–P90。badge：风光与无校准 BESS 为筛选级；"
        "bid-grade 仅限存在 V_hist 校准行的 BESS 单元。"
        "freshness：asof 距今 >10 天标 stale。masking 列为快照读取时的 "
        "RT→DA 填补 / 双缺小时计数（no-meta = 该行无 mc_params 记录）。"
        "两列边际单位不同（BESS ¥/kWh、风光 ¥/kW），并存展示；排名仅以 IRR 为键。"
    )

    # ── margin bar chart (dual axes: the two unit bases never mix) ───────────
    import pandas as pd

    fig = go.Figure()
    labels = [cell_label(r) for r in df.to_dict("records")]
    for col, name, color, ax in (
        ("npv_margin_yuan_kwh", "BESS 边际 (¥/kWh)", "#1565C0", "y"),
        ("npv_margin_yuan_kw", "风光边际 (¥/kW)", "#2E7D32", "y2"),
    ):
        vals = df[col]
        ok = vals.notna()
        if ok.any():
            fig.add_bar(x=[l for l, k in zip(labels, ok) if k],
                        y=[float(v) for v, k in zip(vals, ok) if k],
                        name=name, marker_color=color, yaxis=ax)
    fig.update_layout(
        title="造价边际（左轴 BESS ¥/kWh · 右轴 风光 ¥/kW — 双单位不同轴）",
        height=380,
        yaxis={"title": "BESS ¥/kWh"},
        yaxis2={"title": "风光 ¥/kW", "overlaying": "y", "side": "right"},
        legend={"orientation": "h", "y": -0.2},
    )
    st.plotly_chart(fig, use_container_width=True)

    # ── portfolio-fit section (fleet registry / nodal prices absent → hidden) ─
    st.divider()
    st.subheader("组合契合度 · Portfolio fit")

    fleet = None
    try:
        from services.common.asset_registry import fleet_by_zone
        fleet = fleet_by_zone(eng)
    except Exception:
        fleet = None
    if fleet is None or fleet.empty:
        st.caption("⚠️ 组合契合度需要 asset_registry 在运资产与节点价格数据 — "
                   "当前不可用，本节隐藏（筛选表不受影响）。")
        return

    from libs.deal_models.workflows.portfolio_contribution import (
        compute_contribution,
    )

    records = df.to_dict("records")
    labels = [cell_label(r) for r in records]
    sel = st.selectbox("选择筛选行", range(len(records)),
                       format_func=lambda i: labels[i], key="screen_pf_row")
    row = records[sel]

    pc = st.columns(2)
    cap_mw = pc[0].number_input("候选容量 capacity (MW)", 1.0, 5000.0, 100.0,
                                step=10.0, key="screen_pf_cap")
    nodes = sorted(fleet["zone_price_node"].dropna().unique())
    proxy_opts = ["无 proxy"] + nodes
    default_idx = 0
    if row.get("province") == "蒙西" and nodes:
        # Default proxy for 蒙西 rows: the fleet's largest-throughput node —
        # still a judgment input, labeled as such below.
        w = (fleet.assign(_w=fleet["capacity_mw"] * fleet["duration_h"].fillna(1.0))
                  .groupby("zone_price_node")["_w"].sum())
        top = w.idxmax()
        default_idx = proxy_opts.index(top) if top in proxy_opts else 0
    proxy = pc[1].selectbox(
        "代理价格节点（判断输入 — 筛选行为省级，节点价格为蒙西口径）",
        proxy_opts, index=default_idx, key="screen_pf_proxy")

    candidate = {
        "asset_code": f"screen:{row.get('province')}:{row.get('asset_type')}"
                      f":{float(row.get('duration_h') or 0.0):g}h",
        "zone_price_node": None,  # province-level row: no own node
        "capacity_mw": float(cap_mw),
        "duration_h": float(row.get("duration_h") or 0.0),
    }
    res = compute_contribution(
        eng, candidate, fleet,
        proxy_node=None if proxy == "无 proxy" else proxy,
    )
    for w in res["warnings"]:
        st.warning(w)
    if res.get("used_proxy_node"):
        st.info(f"⚠️ 筛选行无自有价格节点，使用代理节点 "
                f"{res['candidate_node']}（判断输入，非数据）。")

    m = res.get("metrics") or {}
    mc1, mc2, mc3 = st.columns(3)
    mc1.metric("与组合相关性", "—" if m.get("corr") is None else f"{m['corr']:.2f}")
    vpm_b, vpm_a = m.get("vol_per_mw_before"), m.get("vol_per_mw_after")
    mc2.metric("组合波动率/MW（前 → 后）",
               "—" if vpm_b is None else f"{vpm_b:,.0f} → {vpm_a:,.0f}")
    div = m.get("diversification")
    mc3.metric("多元化收益", "—" if div is None else f"{div:.1%}")
    if m.get("corr") is not None:
        if m["corr"] < 0.4:
            st.caption("🟢 低相关 — 该区域对组合有显著对冲/分散价值")
        elif m["corr"] < 0.75:
            st.caption("🟡 中等相关 — 分散价值一般")
        else:
            st.caption("🔴 高相关 — 与现有组合同向波动，多元化价值有限")
    st.caption("口径：组合现金流代理 = Σ 容量×时长×区域节点日均价（120 日窗口，"
               "蒙西节点价格）；波动率/MW 下降 = 多元化收益。省级筛选行以代理节点"
               "近似，未含预测偏差与运维风险。")

    with st.expander("该行快照元数据 · mc_params（掩码计数 / 复现参数）",
                     expanded=False):
        mc = records[sel].get("mc_params")
        if mc:
            st.json(mc if isinstance(mc, dict) else json.loads(mc))
        else:
            st.caption("no-meta：该行快照无 mc_params 记录。")
