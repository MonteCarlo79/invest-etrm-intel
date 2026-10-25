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
        greeks: dict, policy_json: dict, mc_params: dict,
        revenue_paths_yuan: np.ndarray | None,   # None when snapshot-sourced
        power_mw: float,
    }

All data assembly lives in pure functions (no streamlit imports) so it is
unit-testable; ``render()`` is the only function that imports streamlit.
Units: values ¥/kW/yr; prices ¥/MWh; capex ¥/kWh.
"""
from __future__ import annotations

import numpy as np

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
        "greeks": dict(row.get("greeks_json") or row.get("greeks") or {}),
        "policy_json": dict(row.get("policy_json") or {}),
        "mc_params": dict(row.get("mc_params") or {}),
        "revenue_paths_yuan": revenue_paths_yuan,
        "power_mw": float(power_mw),
    }


def _f(v) -> float | None:
    return None if v is None else float(v)


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
    import pandas as pd
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
    if row is None:
        st.info("该省份/资产类型暂无价值快照。请运行批量引擎 "
                "（services/option_engine/run_option_batch.py），"
                "或使用下方「按需重算」生成。")
    else:
        ov = to_ov_result(row, power_mw=power_mw)

        # Preserve a same-cell recompute's paths across reruns (the snapshot
        # itself never persists paths).
        prev = st.session_state.get("ov_result")
        if (
            prev
            and prev.get("province") == ov["province"]
            and prev.get("asset_type") == ov["asset_type"]
            and float(prev.get("duration_h") or -1) == ov["duration_h"]
            and str(prev.get("asof_date")) == ov["asof_date"]
        ):
            ov["revenue_paths_yuan"] = prev.get("revenue_paths_yuan")
        st.session_state["ov_result"] = ov

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
