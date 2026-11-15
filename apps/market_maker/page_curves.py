"""Curves page: reference curve by province, manual CSV upload, Admin refresh."""
from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from apps.market_maker.data import (
    DASH, SOURCE_COLORS, clear_caches, fmt_num, get_curve, is_admin, month_label,
)
from libs.curves.curve import CURVE_COLUMNS, reference_curve
from libs.curves.store import load_curve, save_curve
from libs.positions.conventions import TOU_BLOCKS, canonical_province

UPLOAD_COLUMNS = ["province", "delivery_month", "tou_block", "price_cny_mwh"]


def parse_manual_csv(raw: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """-> (valid manual curve rows, error messages). Invalid rows are dropped, never guessed."""
    missing = [c for c in UPLOAD_COLUMNS if c not in raw.columns]
    if missing:
        return pd.DataFrame(columns=CURVE_COLUMNS), [f"missing columns: {', '.join(missing)}"]
    errors, rows = [], []
    for i, r in raw.iterrows():
        line = i + 2  # header is line 1
        try:
            prov = canonical_province(str(r["province"]).strip())
        except ValueError as e:
            errors.append(f"line {line}: {e}")
            continue
        blk = str(r["tou_block"]).strip()
        if blk not in TOU_BLOCKS:
            errors.append(f"line {line}: tou_block {blk!r} not in {TOU_BLOCKS}")
            continue
        try:
            ts = pd.Timestamp(r["delivery_month"])
            price = float(r["price_cny_mwh"])
        except (ValueError, TypeError):
            errors.append(f"line {line}: bad delivery_month or price")
            continue
        if pd.isna(ts) or pd.isna(price):
            errors.append(f"line {line}: empty delivery_month or price")
            continue
        rows.append([prov, date(ts.year, ts.month, 1), blk, price, "manual"])
    return pd.DataFrame(rows, columns=CURVE_COLUMNS), errors


def read_manual_csv(f) -> tuple[pd.DataFrame, list[str]]:
    """parse_manual_csv on an uploaded file; unreadable files become an error, not a traceback."""
    try:
        return parse_manual_csv(pd.read_csv(f))
    except Exception as e:  # noqa: BLE001 — malformed upload: report it
        return pd.DataFrame(columns=CURVE_COLUMNS), [f"could not read the CSV: {type(e).__name__}: {e}"]


def merge_manual(engine, manual: pd.DataFrame, curve_date: date) -> pd.DataFrame:
    """save_curve replaces the whole (province, curve_date) set, so carry the latest saved
    curve forward. reference_curve resolves duplicate (province, month, block) points by source
    priority (manual > traded > spot_seasonal); between two manual points the new upload wins
    because it is listed first (stable sort)."""
    frames = [manual]
    for prov in sorted(manual["province"].unique()):
        frames.append(load_curve(engine, prov, curve_date))
    return reference_curve(*frames)


def _chart(sub: pd.DataFrame, block: str):
    fig = go.Figure()
    b = sub[sub["tou_block"] == block]
    for src, color in SOURCE_COLORS.items():
        s = b[b["source"] == src]
        if s.empty:
            continue
        fig.add_scatter(x=s["month"], y=s["price_cny_mwh"], mode="markers", name=src,
                        marker=dict(size=9, color=color, line=dict(color="#fcfcfb", width=2)),
                        hovertemplate="%{x}<br>" + src + ": ¥%{y:,.1f}/MWh<extra></extra>")
    fig.update_layout(title=block, height=300, margin=dict(l=10, r=10, t=50, b=10),
                      yaxis_title="¥/MWh", plot_bgcolor="#fcfcfb",
                      legend=dict(orientation="h", y=1.15, x=0))
    fig.update_yaxes(gridcolor="#e8e7e3")
    fig.update_xaxes(showgrid=False, type="category")
    return fig


def _curve_section(engine, sel: dict) -> None:
    curve = get_curve(engine, sel["as_of"])
    if curve.empty:
        st.info("No reference curve saved yet. Upload manual points below or run Refresh (Admin).")
        return
    provs = sorted(curve["province"].unique())
    pref = [p for p in provs if p in sel["provinces"]] or provs
    prov = st.selectbox("Province", pref, key="mm_curve_province")
    sub = curve[curve["province"] == prov].copy()
    sub["month"] = sub["delivery_month"].map(month_label)
    cols = st.columns(len(TOU_BLOCKS))
    for c, blk in zip(cols, TOU_BLOCKS):
        c.plotly_chart(_chart(sub, blk), width="stretch")
    months = [month_label(m) for m in sel["months"]]
    tbl = sub.pivot_table(index="month", columns="tou_block", values="price_cny_mwh",
                          aggfunc="first").reindex(index=months, columns=list(TOU_BLOCKS))
    src = sub.pivot_table(index="month", columns="tou_block", values="source",
                          aggfunc="first").reindex(index=months, columns=list(TOU_BLOCKS))
    view = tbl.map(lambda v: fmt_num(v, 1)) + src.map(lambda s: f" ({s})" if isinstance(s, str) else "")
    st.dataframe(view, width="stretch")
    unmarked = [m for m in months if tbl.loc[m].isna().all()]
    if unmarked:
        st.caption(f"{DASH} = no mark. Horizon months with no point for {prov}: {', '.join(unmarked)}")


def _upload_section(engine) -> None:
    st.markdown("**Manual curve upload** — CSV columns `province, delivery_month, tou_block, "
                "price_cny_mwh`; saved with source `manual`, curve date today")
    f = st.file_uploader("Manual points CSV", type=["csv"], key="mm_curve_csv")
    if f is None:
        return
    manual, errors = read_manual_csv(f)
    for e in errors:
        st.error(e)
    if manual.empty:
        st.warning("No valid rows to save.")
        return
    st.dataframe(manual, hide_index=True, width="stretch")
    if not is_admin():
        st.caption("Saving manual curve points requires the Admin role.")
        return
    if st.button(f"Save {len(manual)} manual points", key="mm_curve_save"):
        today = date.today()
        n = save_curve(engine, merge_manual(engine, manual, today), curve_date=today)
        clear_caches()
        st.success(f"Saved {n} curve points for {today} ({len(manual)} manual).")


def _refresh_section(engine, sel: dict) -> None:
    if not is_admin():
        return
    st.markdown("**Refresh physical positions & curves** (Admin)")
    if st.button("Refresh physical positions & curves", key="mm_refresh"):
        from services.market_maker.refresh import refresh
        with st.spinner("Refreshing…"):
            st.session_state["mm_refresh_result"] = refresh(engine, sel["as_of"], dry_run=False)
        clear_caches()
    res = st.session_state.get("mm_refresh_result")
    if res:
        for key, msg in res["errors"]:
            st.error(f"{key}: {msg}")
        for key, msg in res["warnings"]:
            st.warning(f"{key}: {msg}")
        st.success(f"Books: {len(res['books'])} · curves: {len(res['curves'])} provinces · "
                   f"skipped: {len(res['skipped'])} · errors: {len(res['errors'])}")
        st.json(res, expanded=False)


def render_curves(engine, sel: dict) -> None:
    st.subheader("Reference curves")
    _curve_section(engine, sel)
    st.divider()
    _upload_section(engine)
    st.divider()
    _refresh_section(engine, sel)
