# services/option_engine/scout/scans_trading.py
"""Deterministic trading-opportunity scans (generative scout, Task 2).

Three SELECT-only scans producing opportunity-card candidates. No LLM in this
module — candidates come only from threshold statistics with the driving
numbers attached as ``evidence`` (spec §5.9 rule 12: deterministic generate,
LLM explain; thesis drafting is a later task).

Structure per the Task-2 brief: ``scan_xxx(eng, ...)`` = thin SQL reader +
``compute_xxx(...)`` pure pandas core; the tests pin the pure cores (and
``map_province``), the readers are thin enough to audit by eye.

Unit hygiene (load-bearing): spot prices pass through
``services.option_engine.readers._normalize_units`` — and, for the basis
scan's RT leg, ``_rt_da_fallback`` — at READ time. ``spot_prices_hourly``
carries mixed ¥/kWh / ¥/MWh windows inside one rolling window
(knowledge/spot_market/mixed_unit_price_days.md); any scan querying raw
columns inherits that bias. The interconnector scan's DA legs are normalized
per (province, day) by the same helper (per-province frames — the helper's
day grouping is not province-aware, so it must be applied one province at a
time).

Candidate contract (compatible with ``store.build_signature`` /
``store.save_card``)::

    {"kind": "trading", "province": <canonical label>, "asset_type": "bess",
     "scan_key": <scan identifier>, "evidence": {...numbers...},
     "signature": store.build_signature(card)}

``scan_key`` carries the sub-entity for scans that fire multiple candidates
per province (nodal node, channel pair) so the delta gate's signature stays
per-entity; the basis scan fires at most once per province and stays flat.
"""
from __future__ import annotations

import logging

import numpy as np
import pandas as pd
from sqlalchemy import text

from services.option_engine import readers
from services.option_engine.scout import store

log = logging.getLogger(__name__)

# ── Pinned lists ─────────────────────────────────────────────────────────────
# Nodal-supported province labels (18), pinned from
# apps/mengxi-dashboard/nodal_maps_tab.py:30-31 (_PROVINCES). Re-declared here
# because importing from a Streamlit app is not allowed (same rationale as
# readers.py's _RANKING_EXCLUDE re-declaration).
NODAL_PROVINCES = ["蒙西", "山西", "陕西", "湖南", "浙江", "云南", "贵州", "广东",
                   "广西", "海南", "甘肃", "山东", "河北南网", "黑龙江", "辽宁",
                   "湖北", "安徽", "江西"]

# Province-name variants → canonical spot/exchange labels, extending the
# CAP_COMP_NAME_MAP pattern (services/bess_map/capacity_stack.py:~200-208).
# Only UNAMBIGUOUS variants are mapped: "内蒙古" fans out to [蒙西, 蒙东] in
# the capacity map (policy coverage of both sub-grids), which is NOT a name
# equivalence — it passes through unmapped here rather than silently picking
# a side. Both fullwidth and ASCII parens spellings are covered.
_PROVINCE_NAME_MAP = {
    "内蒙古（蒙西）": "蒙西", "内蒙古(蒙西)": "蒙西",
    "内蒙古（蒙东）": "蒙东", "内蒙古(蒙东)": "蒙东",
    "冀南": "河北南网",
}


def map_province(name):
    """Variant province label → canonical label.

    Identity pass-through for names already canonical or unknown (canonical
    → canonical is the identity case, so the helper works in both
    directions); an unknown name is never silently rewritten.
    """
    if name is None:
        return None
    s = str(name).strip()
    return _PROVINCE_NAME_MAP.get(s, s)


def _card(province: str, scan_key: str, evidence: dict) -> dict:
    card = {"kind": "trading", "province": province, "asset_type": "bess",
            "scan_key": scan_key, "evidence": evidence}
    card["signature"] = store.build_signature(card)
    return card


def _skip(skips, province, scan, reason):
    if skips is not None:
        skips.append({"province": province, "scan": scan, "reason": reason})
    log.info("%s %s skipped: %s", scan, province, reason)


# ── 1. DA−RT basis regimes ───────────────────────────────────────────────────
_BASIS_MIN_DAYS = 120    # Review Focus #2: z-scores on thin history are meaningless
_BASIS_ROLL_DAYS = 30
_BASIS_DIST_DAYS = 365
_BASIS_READ_DAYS = 395   # 365d distribution + 30d rolling warmup


def compute_da_rt_basis(data: dict[str, pd.DataFrame], z_threshold: float = 2.0,
                        skips: list | None = None) -> list[dict]:
    """Pure core: {province: hourly df(datetime, rt_price, da_price)} → candidates.

    Per province: daily mean(rt−da) → rolling-30d mean → the latest 30d-mean
    z-scored against the trailing-365d distribution (mean, std) of the
    30d-mean series; |z| ≥ z_threshold fires. Guards: < 120 days of history →
    skip with reason (appended to ``skips`` when given, always logged);
    degenerate (std = 0 / non-finite) distribution → skip. ``direction`` is
    "rt_premium" when the current 30d basis is positive (rt above da), else
    "da_premium".
    """
    out: list[dict] = []
    for prov, df in (data or {}).items():
        cand = _basis_candidate(prov, df, z_threshold, skips)
        if cand is not None:
            out.append(cand)
    return out


def _basis_candidate(prov: str, df: pd.DataFrame, z_threshold: float,
                     skips: list | None) -> dict | None:
    if df is None or df.empty:
        _skip(skips, prov, "da_rt_basis", "no rows")
        return None
    d = df.copy()
    d["datetime"] = pd.to_datetime(d["datetime"])
    rt = pd.to_numeric(d["rt_price"], errors="coerce")
    da = pd.to_numeric(d["da_price"], errors="coerce")
    daily = (rt - da).groupby(d["datetime"].dt.date).mean().dropna()
    if len(daily) < _BASIS_MIN_DAYS:
        _skip(skips, prov, "da_rt_basis",
              f"thin history: {len(daily)}d < {_BASIS_MIN_DAYS}d")
        return None
    roll = (daily.rolling(_BASIS_ROLL_DAYS, min_periods=_BASIS_ROLL_DAYS)
            .mean().dropna().iloc[-_BASIS_DIST_DAYS:])
    basis_30d = float(roll.iloc[-1])
    mean = float(roll.mean())
    std = float(roll.std(ddof=1))
    if not np.isfinite(std) or std <= 0:
        _skip(skips, prov, "da_rt_basis", "degenerate 30d-mean distribution (std=0)")
        return None
    z = (basis_30d - mean) / std
    if abs(z) < z_threshold:
        return None
    direction = "rt_premium" if basis_30d > 0 else "da_premium"
    return _card(prov, "da_rt_basis", {
        "basis_30d": basis_30d, "basis_mean": mean, "basis_std": std,
        "z": float(z), "direction": direction})


def scan_da_rt_basis(eng, provinces: list[str] | None = None,
                     z_threshold: float = 2.0) -> list[dict]:
    """Basis-regime scan over spot_prices_hourly (SELECT-only).

    Reads the trailing 395 days hourly per province (365d distribution + 30d
    rolling warmup), applies ``readers._normalize_units`` then
    ``readers._rt_da_fallback`` at read time (mixed-unit hygiene, module
    docstring), and hands {province: df} to the pure core. ``provinces=None``
    → all provinces from ``readers.list_provinces`` (ranking-excluded labels
    already filtered there).
    """
    provs = list(provinces) if provinces else readers.list_provinces(eng)
    data: dict[str, pd.DataFrame] = {}
    for p in provs:
        df = _read_price_hours(eng, p, days=_BASIS_READ_DAYS)
        if not df.empty:
            data[p] = df
    skips: list[dict] = []
    return compute_da_rt_basis(data, z_threshold=z_threshold, skips=skips)


def _read_price_hours(eng, province: str, days: int) -> pd.DataFrame:
    sql = text("""
        SELECT datetime, rt_price, da_price
        FROM marketdata.spot_prices_hourly
        WHERE province = :p AND datetime >= CURRENT_DATE - CAST(:days AS integer)
        ORDER BY datetime
    """)
    with eng.connect() as conn:
        df = pd.read_sql(sql, conn, params={"p": province, "days": int(days)})
    if df.empty:
        return df
    df["datetime"] = pd.to_datetime(df["datetime"])
    if df["datetime"].dt.tz is not None:
        df["datetime"] = df["datetime"].dt.tz_localize(None)
    df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime")
    df, _units = readers._normalize_units(df)
    df, _fb = readers._rt_da_fallback(df)
    return df


# ── 2. Nodal congestion-rent outliers ────────────────────────────────────────
_NODAL_MIN_DAYS = 20
_NODAL_MULTIPLE = 1.5


def compute_nodal_pf_outliers(df: pd.DataFrame, top_n: int = 5,
                              min_days: int = _NODAL_MIN_DAYS,
                              multiple_threshold: float = _NODAL_MULTIPLE,
                              skips: list | None = None) -> list[dict]:
    """Pure core: rows (data_date, province, node_name, revenue_cny,
    discharge_mwh) — the stored-config filter (100 MW / 2h / 85% RTE) is
    applied by the reader — → per-node candidates.

    rev_per_mwh = mean over valid days of revenue_cny / discharge_mwh; days
    with discharge_mwh ≤ 0 or null revenue are excluded (SQL NULLIF
    semantics). Guard: a node needs ≥ min_days valid days. Province median is
    taken over guard-passing nodes; candidates are the top_n nodes strictly
    above multiple_threshold × median, ranked by multiple desc. A province
    whose median is ≤ 0 is skipped — multiples against a non-positive base
    are meaningless.
    """
    out: list[dict] = []
    if df is None or df.empty:
        return out
    d = df.copy()
    d["revenue_cny"] = pd.to_numeric(d["revenue_cny"], errors="coerce")
    d["discharge_mwh"] = pd.to_numeric(d["discharge_mwh"], errors="coerce")
    rev = d["revenue_cny"] / d["discharge_mwh"].where(d["discharge_mwh"] > 0)
    d = d.assign(rev_per_mwh=rev).dropna(subset=["rev_per_mwh"])
    if d.empty:
        return out
    for prov in sorted(d["province"].unique()):
        g = d[d["province"] == prov]
        stats = g.groupby("node_name").agg(
            rev_per_mwh=("rev_per_mwh", "mean"),
            days=("data_date", "nunique"),
        )
        stats = stats[stats["days"] >= min_days]
        if stats.empty:
            _skip(skips, prov, "nodal_pf_outlier",
                  f"no node with ≥ {min_days} valid days")
            continue
        median = float(stats["rev_per_mwh"].median())
        if not np.isfinite(median) or median <= 0:
            _skip(skips, prov, "nodal_pf_outlier",
                  f"province median {median} ≤ 0 — multiples meaningless")
            continue
        stats = stats.assign(multiple=stats["rev_per_mwh"] / median)
        hits = (stats[stats["multiple"] > multiple_threshold]
                .sort_values("multiple", ascending=False).head(top_n))
        for node, row in hits.iterrows():
            out.append(_card(prov, f"nodal_pf_outlier:{node}", {
                "node_name": node,
                "rev_per_mwh": float(row["rev_per_mwh"]),
                "province_median": median,
                "multiple": float(row["multiple"]),
                "days": int(row["days"]),
            }))
    return out


def scan_nodal_pf_outliers(eng, provinces: list[str] | None = None,
                           top_n: int = 5, days: int = 30) -> list[dict]:
    """Nodal PF-revenue outlier scan (SELECT-only).

    Reads reports.nodal_pf_node_daily at the fixed stored config
    (power_mw=100, duration_h=2, rte_pct=85 — pinned by the batch writer,
    services/mengxi_nodal/pf_results.py CONFIG) over the trailing ``days``.
    ``provinces=None`` → the 18 nodal-supported labels (NODAL_PROVINCES).
    """
    provs = list(provinces) if provinces else NODAL_PROVINCES
    sql = text("""
        SELECT data_date, province, node_name, revenue_cny, discharge_mwh
        FROM reports.nodal_pf_node_daily
        WHERE power_mw = 100 AND duration_h = 2 AND rte_pct = 85
          AND data_date >= CURRENT_DATE - CAST(:days AS integer)
          AND province = ANY(:provs)
    """)
    with eng.connect() as conn:
        df = pd.read_sql(sql, conn, params={"days": int(days), "provs": provs})
    skips: list[dict] = []
    return compute_nodal_pf_outliers(df, top_n=top_n, skips=skips)


# ── 3. Interconnector spread windows ─────────────────────────────────────────
_IC_MIN_MONTHS = 6       # a monthly std on fewer points is not a distribution
_IC_READ_DAYS = 400      # trailing 12m + current month


def compute_interconnector_windows(trades: pd.DataFrame,
                                   spot_monthly: pd.DataFrame,
                                   exchange_monthly: pd.DataFrame,
                                   z_threshold: float = 1.0,
                                   skips: list | None = None) -> list[dict]:
    """Pure core → channel-window candidates.

    trades: rows (send_anchor, recv_province, month_start, channel_fee,
        vol_post_mwh, channel_1/2/3) — the reader passes the whole snapshot;
        windowing happens here.
    spot_monthly: (province, month, price) — DA monthly mean from
        spot_prices_hourly, unit-normalized at read time (primary source).
    exchange_monthly: (province, month, price) — exchange-reported
        spot_avg_price_yuan_mwh; fills (province, month) gaps the spot table
        misses (e.g. 上海 has no LingFeng series). Spot wins on overlap.
        contract_avg_price_yuan_mwh is not used: this scan measures
        REALIZED spreads, not month-ahead proxies.

    Per (send, recv) pair (both legs through map_province): trailing-12m net
    spread series net(m) = recv_price(m) − send_price(m) − pair fee (VWAP of
    the pair's channel_fee, matching interconnector/data.py::_pair_vwap);
    months missing either leg are dropped; ≥ 6 valid months and std > 0
    required. Candidates: trade rows in the two most recent month_starts
    present in the snapshot (data-driven — the table is a cumulative upload,
    not anchored to CURRENT_DATE) whose realized net (row's own channel_fee,
    pair VWAP fallback) exceeds mean + z_threshold × std AND is positive.
    Multiple rows per (pair, month) collapse to the largest-vol_post row.
    """
    out: list[dict] = []
    if trades is None or trades.empty:
        return out
    t = trades.copy()
    t["month_start"] = pd.to_datetime(t["month_start"]).dt.to_period("M")
    t["send"] = t["send_anchor"].map(map_province)
    t["recv"] = t["recv_province"].map(map_province)
    for c in ("channel_fee", "vol_post_mwh"):
        t[c] = pd.to_numeric(t[c], errors="coerce")

    prices: dict[tuple, float] = {}
    for df in (exchange_monthly, spot_monthly):  # exchange first: spot overwrites
        if df is None or df.empty:
            continue
        d = df.copy()
        d["month"] = pd.to_datetime(d["month"]).dt.to_period("M")
        price = pd.to_numeric(d["price"], errors="coerce")
        for prov, m, p in zip(d["province"].map(map_province), d["month"], price):
            if prov and pd.notna(p):
                prices[(prov, m)] = float(p)

    months_present = sorted(t["month_start"].dropna().unique())
    if not months_present:
        return out
    latest = months_present[-1]
    cur_months = set(months_present[-2:])
    dist_months = [latest - i for i in range(12)]

    for (send, recv), g in t.groupby(["send", "recv"]):
        if not send or not recv:
            continue
        cur = g[g["month_start"].isin(cur_months)]
        if cur.empty:
            continue
        fee_pair = _pair_fee(g)
        nets = {}
        for m in dist_months:
            ps, pr = prices.get((send, m)), prices.get((recv, m))
            if ps is None or pr is None or fee_pair is None:
                continue
            nets[m] = pr - ps - fee_pair
        if len(nets) < _IC_MIN_MONTHS:
            _skip(skips, recv, "interconnector_window",
                  f"{send}>{recv}: {len(nets)} months < {_IC_MIN_MONTHS}")
            continue
        vals = pd.Series(nets).sort_index()
        mean, std = float(vals.mean()), float(vals.std(ddof=1))
        if not np.isfinite(std) or std <= 0:
            _skip(skips, recv, "interconnector_window",
                  f"{send}>{recv}: degenerate spread distribution (std=0)")
            continue
        for m, grp in cur.groupby("month_start"):
            row = grp.loc[grp["vol_post_mwh"].fillna(-1).idxmax()]
            fee_row = row["channel_fee"] if pd.notna(row["channel_fee"]) else fee_pair
            ps, pr = prices.get((send, m)), prices.get((recv, m))
            if ps is None or pr is None or fee_row is None:
                _skip(skips, recv, "interconnector_window",
                      f"{send}>{recv} {m}: missing leg price or fee")
                continue
            net_now = float(pr - ps - fee_row)
            if net_now > 0 and net_now > mean + z_threshold * std:
                channel = next((row[c] for c in ("channel_1", "channel_2", "channel_3")
                                if isinstance(row.get(c), str) and row.get(c)), None)
                out.append(_card(recv, f"interconnector_window:{send}>{recv}", {
                    "channel": channel, "send": send, "recv": recv,
                    "net_spread_now": net_now,
                    "net_spread_mean": mean, "net_spread_std": std,
                    "month": str(m)}))
    return out


def _pair_fee(g: pd.DataFrame) -> float | None:
    """Pair VWAP channel_fee (vol_post-weighted; plain mean when all weights
    missing) — mirrors services/interconnector/data.py::_pair_vwap."""
    g = g.dropna(subset=["channel_fee"])
    if g.empty:
        return None
    w = g["vol_post_mwh"].fillna(0)
    if w.sum() > 0:
        return float((g["channel_fee"] * w).sum() / w.sum())
    return float(g["channel_fee"].mean())


def scan_interconnector_windows(eng, z_threshold: float = 1.0) -> list[dict]:
    """Interconnector spread-window scan (SELECT-only).

    Realized net spread per channel trade row (latest 2 months) vs that
    pair's trailing-12m net spread distribution. Leg prices: DA monthly mean
    from spot_prices_hourly (unit-normalized per province via
    readers._normalize_units — the RT-leg fallback is not applicable to a
    DA-only read), with exchange_monthly_metrics.spot_avg_price_yuan_mwh
    filling provinces missing from the spot table.
    """
    trades = _read_trades(eng)
    if trades.empty:
        return []
    provs = sorted({p for p in
                    [map_province(v) for v in trades["send_anchor"]]
                    + [map_province(v) for v in trades["recv_province"]] if p})
    spot_monthly = _read_spot_monthly(eng, provs, days=_IC_READ_DAYS)
    exchange_monthly = _read_exchange_monthly(eng, days=_IC_READ_DAYS)
    skips: list[dict] = []
    return compute_interconnector_windows(trades, spot_monthly, exchange_monthly,
                                          z_threshold=z_threshold, skips=skips)


def _read_trades(eng) -> pd.DataFrame:
    # Whole snapshot — the table is a small cumulative upload; windowing is
    # done in the pure core.
    sql = text("""
        SELECT send_anchor, recv_province, month_start, channel_fee,
               vol_post_mwh, channel_1, channel_2, channel_3
        FROM staging.interconnector_trades
        WHERE month_start IS NOT NULL
    """)
    with eng.connect() as conn:
        return pd.read_sql(sql, conn)


def _read_spot_monthly(eng, provs: list[str], days: int) -> pd.DataFrame:
    """DA monthly means per province, unit-normalized per (province, day).

    Per-province hourly reads (not one concatenated query) because
    ``_normalize_units`` groups by calendar day only — applying it to a
    multi-province frame would pool same-day maxima across provinces.
    """
    sql = text("""
        SELECT datetime, da_price
        FROM marketdata.spot_prices_hourly
        WHERE province = :p AND datetime >= CURRENT_DATE - CAST(:days AS integer)
        ORDER BY datetime
    """)
    frames = []
    with eng.connect() as conn:
        for p in provs:
            df = pd.read_sql(sql, conn, params={"p": p, "days": int(days)})
            if df.empty:
                continue
            df["datetime"] = pd.to_datetime(df["datetime"])
            if df["datetime"].dt.tz is not None:
                df["datetime"] = df["datetime"].dt.tz_localize(None)
            df, _units = readers._normalize_units(df)
            df["month"] = df["datetime"].dt.to_period("M").dt.to_timestamp()
            frames.append(pd.DataFrame({
                "province": p,
                "month": df.groupby("month")["da_price"].mean().index,
                "price": df.groupby("month")["da_price"].mean().values,
            }))
    if not frames:
        return pd.DataFrame(columns=["province", "month", "price"])
    return pd.concat(frames, ignore_index=True)


def _read_exchange_monthly(eng, days: int) -> pd.DataFrame:
    sql = text("""
        SELECT province, report_month AS month, spot_avg_price_yuan_mwh AS price
        FROM staging.exchange_monthly_metrics
        WHERE report_month >= CURRENT_DATE - CAST(:days AS integer)
    """)
    with eng.connect() as conn:
        return pd.read_sql(sql, conn, params={"days": int(days)})
