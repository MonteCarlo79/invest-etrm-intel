# Design Spec: IRR Full Revenue Stack + Geo Map Overlays
_2026-08-05 — bess-map app.py_

## Goal

Two related improvements to `apps/bess-map/app.py`:

1. **IRR Calculator tab**: expand revenue basis from spot-arbitrage-only to include 调频, 容量补偿, and 系统运行费 — auto-populated from existing loaded DataFrames, editable by user, broken out in the cashflow chart.
2. **Geo Map tab**: allow users to optionally overlay 系统运行费, 容量补偿, and/or 调频 onto the payback-period choropleth so the map reflects a more complete revenue/cost picture.

---

## Scope

Single file: `apps/bess-map/app.py`  
No new DB queries — reuse already-loaded variables `_sof_df`, `_cc_df`, `_fr_df`.

---

## Data Sources

| Variable | Table | Key column | Unit |
|---|---|---|---|
| `_sof_df` | `province_sysopfee_monthly` | `fee_yuan_kwh` | ¥/kWh |
| `_cc_df` | `marketdata.province_cap_comp` | `cap_comp_yuan_kw` | ¥/kW/year |
| `_fr_df` | `marketdata.province_fr_market` | `fr_price_yuan_kw_h` | ¥/kW·h |

All three are loaded in `tab_sysopfee` / `tab_aux` blocks, which execute before `tab_irr` and `tab_geo` in the render order.

---

## Unit Conversions (per MWh installed capacity per day)

These normalise each item to `¥/MWh/day` for a 1 MW plant, consistent with `theo_per_mwh_day` used by `build_cashflows()`.

| Item | Formula | Sign |
|---|---|---|
| 系统运行费 | `fee_yuan_kwh × 1000 / RTE / 365` | negative (cost on charge energy) |
| 容量补偿 | `cap_comp_yuan_kw / duration_h / 365` | positive |
| 调频 | `fr_price_yuan_kw_h × fr_util_pct × 8760 / duration_h / 365` | positive |

For province defaults:
- 系统运行费: average of last 12 available months
- 容量补偿: most recent `effective_date` row per province where `cap_comp_yuan_kw IS NOT NULL`
- 调频: most recent `effective_date` row per province
- Default `fr_util_pct = 0.30` (30% of installed MW participates in FR)

For the geo map annual adjustment (`¥/MWh/year`), multiply the per-day values above by 365.

### Edge Cases for 容量补偿

Some provinces use non-standard compensation models stored in `notes` rather than `cap_comp_yuan_kw`:

| Province | Issue | Handling |
|---|---|---|
| 山东 | `cap_comp_yuan_kw = NULL`; dynamic ¥/kWh model in `notes` | Show 0.0, source label "无标准数据（动态模型）" |
| 内蒙古（蒙东） | `cap_comp_yuan_kw = 0.28` but unit is ¥/kWh (discharge volume), not ¥/kW | Skip row if `notes` contains "kWh"; show 0.0 with label "无标准数据（放电量模式）" |

Detection rule: if `notes` field contains `kWh` (case-insensitive), treat `cap_comp_yuan_kw` as non-standard and return 0.0. This is conservative but avoids silently mis-converting a ¥/kWh figure as ¥/kW.

---

## Change 1: New Helper Functions

### `_irr_defaults_for_province(province, duration_h, sof_df, cc_df, fr_df, rte=0.85, fr_util_pct=0.30)`

Returns `dict` with keys:
```python
{
    "sysopfee_day":   float,   # ¥/MWh/day (negative)
    "cap_comp_day":   float,   # ¥/MWh/day (positive)
    "fr_day":         float,   # ¥/MWh/day (positive)
    "sysopfee_src":   str,     # "市场均值 (N月)" or "无数据"
    "cap_comp_src":   str,     # "最新数据 YYYY-MM" or "无数据"
    "fr_src":         str,     # "最新数据 YYYY-MM" or "无数据"
}
```

Missing data returns 0.0 with source label "无数据".

### `_build_extra_rev_map(sof_df, cc_df, fr_df, duration_h, selected_items, fr_util_pct=0.30, rte=0.85)`

- `selected_items`: list subset of `["sysopfee", "cap_comp", "fr"]`
- Returns `dict[str, float]`: province → net annual adjustment in `¥/MWh/year`
- For each province: sum of selected items converted to annual ¥/MWh

---

## Change 2: `build_cashflows()` — Parameter Expansion

Replace `subsidy_per_mwh: float` with four explicit parameters:

```python
def build_cashflows(
    theo_per_mwh_day: float,
    capture_rate: float,
    duration_h: float,
    capex_per_kwh: float,
    rte: float,
    om_per_kw_yr: float,
    degradation: float,
    equity_pct: float,
    loan_rate: float,
    loan_tenure: int,
    project_life: int,
    power_mw: float = 1.0,
    # revenue/cost components (¥/MWh/day, normalised to installed capacity)
    sysopfee_per_mwh_day: float = 0.0,   # negative
    cap_comp_per_mwh_day: float = 0.0,   # positive
    fr_per_mwh_day: float = 0.0,         # positive
    subsidy_per_mwh: float = 0.0,        # legacy catch-all, kept for back-compat
) -> tuple[list, dict]:
```

Annual revenue breakdown stored in `breakdown[yr]`:
```python
{
    "spot":      float,
    "cap_comp":  float,
    "fr":        float,
    "sysopfee":  float,   # negative
    "subsidy":   float,   # legacy
    "om":        float,
    "debt_svc":  float,
    "net":       float,
}
```

All components degrade at the same `degradation` rate.

---

## Change 3: IRR Tab UI

After the existing "Revenue basis" success box, add an expander **"收入/成本明细 ▾"**:

```
系统运行费   [number_input, default=computed]  [badge: 市场均值 N月]   (¥/MWh/day)
容量补偿     [number_input, default=computed]  [badge: 最新 YYYY-MM]   (¥/MWh/day)
调频         [number_input, default=computed]  [badge: 最新×util%]     (¥/MWh/day)
             调频利用率   [slider 0–100%, default 30%]
```

Source badge displayed as `st.caption()` inline.  
Number inputs are always editable; default recomputes on province or duration change.

Pass the four values to extended `build_cashflows()`.

### Cashflow Waterfall Chart (updated)

Six stacked bars per year (relative barmode):

| Bar | Colour | Sign |
|---|---|---|
| 现货套利 | `#2ecc71` (green) | + |
| 调频 | `#27ae60` (dark green) | + |
| 容量补偿 | `#1abc9c` (teal) | + |
| 系统运行费 | `#e67e22` (orange) | − |
| O&M | `#e74c3c` (red) | − |
| 债务服务 | `#c0392b` (dark red) | − |

Net cashflow line unchanged (navy, lines+markers).

---

## Change 4: Geo Map Tab UI

Below the `geo_capex` slider, add:

```
额外收入/成本项（叠加至回收期计算）:
  multiselect: ["系统运行费（成本）", "容量补偿（收益）", "调频（收益）"]
  (if 调频 selected): 调频利用率 [slider 0–100%, default 30%]
```

Build `_extra_map` via `_build_extra_rev_map(...)`.

Pass to `chart_bess_revenue_map()` as new optional parameter `extra_rev_map: dict[str, float] | None = None`.

### `chart_bess_revenue_map()` update

- If `extra_rev_map` provided: `adj_rev = rev + extra_rev_map.get(province, 0)` for payback colour and payback years
- Label format (when extra items active):
  ```
  ¥12,450 (+¥890)
  (4.2yr → 3.8yr)
  ```
- Legend title updated to `"Adjusted Payback"` when extra items active.
- Payback colour uses adjusted revenue.
- Original spot-only revenue still shown in label for reference.

---

## Sensitivity Table

The sensitivity table (capex × rev_multiplier) applies the multiplier only to `spot` revenue. The three additional components remain fixed. This preserves the table's meaning as a spot-arbitrage sensitivity check.

---

## i18n

Add Chinese/English label pairs for new UI strings in the existing `_T` dict at the top of `app.py`:

- `irr_components_title`: "收入/成本明细" / "Revenue & Cost Detail"
- `irr_fr_util`: "调频利用率" / "FR Utilisation"
- `irr_cf_fr`: "调频" / "Freq Reg"
- `irr_cf_cap_comp`: "容量补偿" / "Cap Comp"
- `irr_cf_sysopfee`: "系统运行费" / "Sys Op Fee"
- `geo_extra_items`: "额外收入/成本项" / "Add Revenue/Cost Items"
- `geo_extra_sysopfee`: "系统运行费（成本）" / "Sys Op Fee (cost)"
- `geo_extra_cap_comp`: "容量补偿（收益）" / "Cap Comp (revenue)"
- `geo_extra_fr`: "调频（收益）" / "Freq Reg (revenue)"

---

## Out of Scope

- Asset Risk settlement pre-fill (deferred — settlement data is per-asset, not per-province; addressed separately)
- Sensitivity table does not vary the three new components
- No new DB tables or migrations
