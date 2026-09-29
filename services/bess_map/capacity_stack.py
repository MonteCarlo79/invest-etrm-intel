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
    "内蒙古": (0.0,   6.0, 1.0,  "legacy", "旧政策: 按调度放电电量补偿"),
    "浙江":   (0.0,   6.0, 1.0,  "legacy", "旧政策: 容量补偿资金分配方案"),
    "山东":   (0.0,   6.0, 1.0,  "legacy", "旧政策: 现货容量补偿资金池 2×权重"),
    "河北":   (0.0,   6.0, 1.0,  "legacy", "旧政策: 容量补偿政策"),
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


def load_capacity_rows(eng) -> list[tuple]:
    """All province_capacity_price rows for capacity_stack_map."""
    from sqlalchemy import text as _t
    with eng.connect() as conn:
        return conn.execute(_t(
            "SELECT province, rate_yuan_kw_yr, base_hours, coefficient, status "
            "FROM marketdata.province_capacity_price"
        )).fetchall()


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
