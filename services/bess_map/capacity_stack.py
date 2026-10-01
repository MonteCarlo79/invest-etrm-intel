# services/bess_map/capacity_stack.py
"""Capacity payment + ancillary revenue stacking for the Province Ranking chart.

Capacity payment conversion (approved formula):
    per MWh_installed = rate × (1000/D) × min(D, base_hours)/base_hours × coefficient
where D = discharge duration (h). For D ≤ base_hours this simplifies to
rate × 1000/base_hours — duration-independent; for D > base_hours the
pro-rating caps at base_hours so per-MWh payment falls as 1/D.

Reference data: 114号文 xlsx (data/114号文/独立储能容量电价情况更新20260909.xlsx),
吉林 吉发改价格联〔2026〕240号 PDF, user-provided 辽宁 370, default 165/6h.
Statuses: formal (stack), default (stack), draft / none / legacy (no stack).
"""
from __future__ import annotations

DEFAULT_RATE = 165.0          # ¥/kW/yr, user rule: default when no policy found
DEFAULT_BASE_HOURS = 6.0      # 6-hour equivalent basis

# province → (rate ¥/kW/yr, base_hours, coefficient, status, source)
CAPACITY_PRICE_SEED: dict[str, tuple[float, float, float, str, str]] = {
    "甘肃":   (330.0, 6.0, 0.89, "formal", "甘发改价格规〔2025〕4号"),
    "吉林":   (330.0, 8.0, 1.0,  "formal", "吉发改价格联〔2026〕240号"),
    "青海":   (185.0, 8.0, 0.92, "formal", "青发改价格规〔2026〕365号"),
    "陕西":   (165.0, 6.0, 1.0,  "formal", "陕发改价格〔2026〕924号"),
    "湖北":   (165.0, 10.0, 1.0, "formal", "鄂发改价格〔2025〕408号"),
    "辽宁":   (370.0, 6.0, 1.0,  "formal", "user-provided 2026-09-29 (source doc TBD)"),
    "宁夏":   (165.0, 6.0, 1.0,  "draft",  "征求意见稿 2025-09, not effective"),
    "天津":   (231.0, 6.0, 1.0,  "none",   "行业讨论, no formal doc"),
    "山西":   (165.0, 6.0, 1.0,  "none",   "行业讨论, no formal doc"),
    "蒙西":   (0.0,   6.0, 1.0,  "legacy", "旧政策: 按调度放电电量补偿 (user 2026-09-29: keep old scheme, do not stack)"),
    "浙江":   (0.0,   6.0, 1.0,  "legacy", "旧政策: 容量补偿资金分配方案"),
    "山东":   (0.0,   6.0, 1.0,  "legacy", "旧政策: 现货容量补偿资金池 2×权重 (user 2026-09-29: keep old scheme, do not stack)"),
    "河北南网": (0.0, 6.0, 1.0,  "legacy", "旧政策: 容量补偿政策"),
}
_STATUSES = {"formal", "default", "draft", "none", "legacy"}
STACKABLE_STATUSES = {"formal", "default"}


def capacity_payment_per_mwh_yr(rate: float, base_hours: float,
                                coefficient: float, duration_h: float) -> float:
    """¥/MWh_installed/yr for a unit of `duration_h` under a capacity price."""
    if duration_h <= 0 or base_hours <= 0:
        raise ValueError("durations must be positive")
    return rate * (1000.0 / duration_h) * (min(duration_h, base_hours) / base_hours) * coefficient


def seed_rows() -> list[tuple[str, float, float, float, str, str]]:
    """(province, rate, base_hours, coefficient, status, source) for DB seeding."""
    for prov, (r, h, c, st, src) in CAPACITY_PRICE_SEED.items():
        assert st in _STATUSES, f"bad status {st} for {prov}"
    return [(p, r, h, c, st, src) for p, (r, h, c, st, src) in CAPACITY_PRICE_SEED.items()]


_DDL = """
CREATE TABLE IF NOT EXISTS marketdata.province_capacity_price (
    province        TEXT PRIMARY KEY,
    rate_yuan_kw_yr DOUBLE PRECISION NOT NULL,
    base_hours      DOUBLE PRECISION NOT NULL,
    coefficient     DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    status          TEXT NOT NULL,           -- formal/default/draft/none/legacy
    source_doc      TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

_UPSERT = """
INSERT INTO marketdata.province_capacity_price
    (province, rate_yuan_kw_yr, base_hours, coefficient, status, source_doc)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (province) DO UPDATE SET
    rate_yuan_kw_yr = EXCLUDED.rate_yuan_kw_yr,
    base_hours      = EXCLUDED.base_hours,
    coefficient     = EXCLUDED.coefficient,
    status          = EXCLUDED.status,
    source_doc      = EXCLUDED.source_doc,
    updated_at      = NOW()
"""


def ensure_table(cur) -> None:
    cur.execute(_DDL)


def seed_capacity_prices(pg_url: str) -> int:
    """Create the table and upsert the seed rows. Returns rows written."""
    import psycopg2
    conn = psycopg2.connect(pg_url)
    try:
        with conn.cursor() as cur:
            ensure_table(cur)
            for row in seed_rows():
                cur.execute(_UPSERT, row)
        conn.commit()
    finally:
        conn.close()
    return len(seed_rows())


def capacity_stack_map(rows: list[tuple], duration_h: float) -> dict[str, float]:
    """province → ¥/MWh_installed/yr capacity payment for stackable rows.

    rows: (province, rate, base_hours, coefficient, status) from the DB.
    Provinces not present get the DEFAULT (165/6h/1.0)."""
    out: dict[str, float] = {}
    for prov, rate, base_h, coef, status in rows:
        if status not in STACKABLE_STATUSES:
            continue
        out[prov] = capacity_payment_per_mwh_yr(float(rate), float(base_h),
                                                float(coef), duration_h)
    return out


def ancillary_per_mwh_yr(annual_total_yuan: float, bess_mw: float, duration_h: float) -> float:
    """Fleet-average 调频 revenue as ¥/MWh_installed/yr for a `duration_h` unit.

    调频 revenue is power-rated (MW of regulation capacity): per-kW-yr =
    annual_total / (bess_mw × 1000), then × (1000/D) for the MWh_installed
    of a D-hour unit (a 2h unit carries 2× the power per MWh)."""
    if bess_mw <= 0 or duration_h <= 0:
        return 0.0
    return annual_total_yuan / (bess_mw * 1000.0) * (1000.0 / duration_h)


# ── 调频 fallback: pool-share model (user spec 2026-10-01) ───────────────────
# When mileage-based compensation isn't disclosed, allocate a share of the
# provincial FR fund pool to BESS and divide by the province's BESS capacity.
SOUTH_GRID_PROVINCES = frozenset({"广东", "广西", "云南", "贵州", "海南"})
FR_POOL_SHARE_DEFAULT = 0.30   # 30% of the pool to BESS
FR_POOL_SHARE_SOUTH = 0.20     # 20% for Southern Grid provinces


def fr_pool_share(province: str) -> float:
    return FR_POOL_SHARE_SOUTH if province in SOUTH_GRID_PROVINCES else FR_POOL_SHARE_DEFAULT


def fr_pool_per_mwh_yr(fr_pool_yuan: float, bess_mw: float, duration_h: float,
                       province: str) -> float:
    """调频 income per MWh_installed/yr from the pool-share model:
    pool × share ÷ (bess_mw × 1000 kW)  → ¥/kW/yr, then × (1000/D)."""
    return ancillary_per_mwh_yr(fr_pool_yuan * fr_pool_share(province),
                                bess_mw, duration_h)


def fr_component(province: str, duration_h: float,
                 confirmed_actual_yuan: float | None,
                 fr_pool_yuan: float | None,
                 bess_mw: float) -> float:
    """Precedence (user spec): mileage-settled actuals > pool-share > 0."""
    if confirmed_actual_yuan:
        return ancillary_per_mwh_yr(confirmed_actual_yuan, bess_mw, duration_h)
    if fr_pool_yuan:
        return fr_pool_per_mwh_yr(fr_pool_yuan, bess_mw, duration_h, province)
    return 0.0


# ── 系统运行费: per-kWh adder on charging energy ─────────────────────────────

def sysopfee_annual_cost_per_mwh(fee_yuan_kwh: float, cycles_per_day: float,
                                 roundtrip_eff: float = 0.85) -> float:
    """¥/MWh_installed/yr deduction: the fee is charged per kWh of charging
    energy (charge = discharge ÷ RTE), at the province's measured cycles.
    Duration-independent per MWh of installed energy."""
    if fee_yuan_kwh <= 0 or cycles_per_day <= 0 or roundtrip_eff <= 0:
        return 0.0
    return fee_yuan_kwh * 1000.0 * cycles_per_day * 365.0 / roundtrip_eff


# ── 放电量补偿 (per-discharge-energy capacity compensation) ─────────────────
# 蒙西/蒙东: 0.28 元/kWh discharged (280 ¥/MWh); 山东: 0.0705 元/kWh (dynamic).
# In province_cap_comp these rows carry cap_comp_yuan_kw < 1 (元/kWh), while
# per-kW-year rates are always ≥ 100 — the threshold discriminates the modes
# without a schema change.
DISCHARGE_MODE_RATE_THRESHOLD = 1.0


def discharge_comp_per_mwh_yr(rate_yuan_kwh: float, cycles_per_day: float) -> float:
    """¥/MWh_installed/yr for a per-discharge-energy compensation:
    rate (元/kWh) × 1000 (→ ¥/MWh) × annual discharged MWh per MWh_installed
    (cycles/day × 365). Duration-independent per MWh of installed energy."""
    if rate_yuan_kwh <= 0 or cycles_per_day <= 0:
        return 0.0
    return rate_yuan_kwh * 1000.0 * cycles_per_day * 365.0


# cap_comp province-name variants → spot price-series names.
# 内蒙古(蒙东) covers BOTH 蒙东 and 蒙西 (same policy per user 2026-10-01).
CAP_COMP_NAME_MAP: dict[str, list[str]] = {
    "内蒙古（蒙东）": ["蒙东", "蒙西"],
    "内蒙古（蒙西）": ["蒙西"],
    "内蒙古": ["蒙西", "蒙东"],
    "冀南": ["河北南网"],
}


def load_capacity_rows(eng) -> list[tuple]:
    """All province_capacity_price rows for capacity_stack_map."""
    from sqlalchemy import text as _t
    with eng.connect() as conn:
        return conn.execute(_t(
            "SELECT province, rate_yuan_kw_yr, base_hours, coefficient, status "
            "FROM marketdata.province_capacity_price"
        )).fetchall()


def load_cap_comp_latest(eng) -> dict[str, tuple[float, float]]:
    """province → (cap_comp_yuan_kw, peak_duration_hours) from the curated
    容量补偿 tab (province_cap_comp, confirmed rows, latest effective_date).
    This is the PRIMARY capacity-rate source; province_capacity_price only
    supplies coefficients and legacy exclusions.
    cap_comp_yuan_kw < 1 means a per-kWh discharge compensation (元/kWh).
    Name variants (内蒙古（蒙东）, 冀南) fan out per CAP_COMP_NAME_MAP."""
    from sqlalchemy import text as _t
    with eng.connect() as conn:
        rows = conn.execute(_t("""
            SELECT DISTINCT ON (province)
                   province, cap_comp_yuan_kw, peak_duration_hours
            FROM marketdata.province_cap_comp
            WHERE status = 'confirmed' AND cap_comp_yuan_kw IS NOT NULL
            ORDER BY province, effective_date DESC, ingested_at DESC
        """)).fetchall()
    out: dict[str, tuple[float, float]] = {}
    for p, r, h in rows:
        val = (float(r), float(h) if h else DEFAULT_BASE_HOURS)
        out[p] = val
        for mapped in CAP_COMP_NAME_MAP.get(p, []):
            out.setdefault(mapped, val)
    return out


def load_fr_pool(eng) -> dict[str, float]:
    """province → latest FR fund pool (¥/yr) from province_fr_market
    (fr_pool_billion_yuan 亿元 → 元), confirmed rows."""
    from sqlalchemy import text as _t
    with eng.connect() as conn:
        rows = conn.execute(_t("""
            SELECT DISTINCT ON (province) province, fr_pool_billion_yuan
            FROM marketdata.province_fr_market
            WHERE status = 'confirmed' AND fr_pool_billion_yuan IS NOT NULL
            ORDER BY province, effective_date DESC
        """)).fetchall()
    return {p: float(v) * 1e8 for p, v in rows}


def load_sysopfee_latest(eng) -> dict[str, float]:
    """province → latest monthly 系统运行费 rate (¥/kWh)."""
    from sqlalchemy import text as _t
    with eng.connect() as conn:
        rows = conn.execute(_t("""
            SELECT DISTINCT ON (province) province, fee_yuan_kwh
            FROM province_sysopfee_monthly
            ORDER BY province, year_month DESC
        """)).fetchall()
    return {p: float(v) for p, v in rows}


def load_ancillary_annual(eng) -> dict[str, float]:
    """province → annualized confirmed 调频 revenue (¥/yr).

    Uses the latest 12 months ending at the max confirmed month; if fewer
    than 12 months exist, scales the available average to 12."""
    from sqlalchemy import text as _t
    with eng.connect() as conn:
        rows = conn.execute(_t("""
            SELECT province, month, SUM(amount_yuan) AS amt
            FROM marketdata.province_ancillary_revenue
            WHERE status = 'confirmed'
            GROUP BY province, month
        """)).fetchall()
    by_prov: dict[str, list[tuple]] = {}
    for prov, month, amt in rows:
        by_prov.setdefault(prov, []).append((month, float(amt)))
    out: dict[str, float] = {}
    for prov, ms in by_prov.items():
        ms.sort()
        last12 = [a for _, a in ms[-12:]]
        out[prov] = sum(last12) / len(last12) * 12.0
    return out
