# services/option_engine/run_calibration.py
"""Thin CLI for the calibration report (spec §8).

    python services/option_engine/run_calibration.py --provinces 山东,蒙西,广东

Prints V_tot / V_hist / ratio / flag_ok per latest snapshot row, then the
per-province read-time masking counts from mc_params (unit rescaling and
RT→DA fallback: filled / missing_both / negatives_kept). Rows outside the
[0.7, 1.4] band are listed at the end for a cause hypothesis (data gap vs
model miss).

Engine from PGURL (fallback DB_DSN) — `set -a; source config/.env; set +a`
first.
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import create_engine  # noqa: E402

from services.option_engine import calibration  # noqa: E402


def _engine():
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise SystemExit(
            "PGURL (or DB_DSN) env var required — source config/.env first"
        )
    return create_engine(url, connect_args={"connect_timeout": 10})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Option-engine calibration report")
    ap.add_argument("--provinces", required=True,
                    help="comma-separated, e.g. 山东,蒙西,广东,山西,江苏")
    args = ap.parse_args(argv)
    provinces = [p.strip() for p in args.provinces.split(",") if p.strip()]

    eng = _engine()
    df = calibration.calibration_report(eng, provinces)
    print(f"Calibration report — band [{calibration.RATIO_LO}, "
          f"{calibration.RATIO_HI}] on ratio_energy = (intrinsic+extrinsic)/"
          f"V_hist (¥/kW/yr); ratio_with_policy is informational only")
    if df.empty:
        print("  (no snapshots found — run run_option_batch.py first)")
        return 1
    print(df.to_string(index=False))

    skewed = df[(df["asset_type"] == "bess")
                & (df["rt_days_scaled"].fillna(0) > 0)]
    if not skewed.empty:
        provs = ", ".join(sorted(skewed["province"].unique()))
        print(f"\nCaveat: {provs} — ¥/kWh flat-fill days were rescaled at "
              f"read time (rt days_scaled > 0); V_hist inherits the "
              f"mixed-unit skew until the Pillar-2 capture pipeline re-runs "
              f"on normalized prices.")

    print("\nRead-time masking counts (mc_params of latest snapshots):")
    for m in calibration.masking_summary(eng, provinces):
        units = m["units"] or {}
        unit_bits = ", ".join(
            f"{col}: scaled={u['scaled']} n={u['count']}"
            for col, u in units.items()
        ) or "n/a"
        print(
            f"  {m['province']} {m['asset_type']} {m['duration_h']}h "
            f"asof={m['asof_date']} | units[{unit_bits}] | fallback "
            f"filled={m['filled']} missing_both={m['missing_both']} "
            f"negatives_kept={m['negatives_kept']}"
        )

    flagged = df[df["flag_ok"] == False]  # noqa: E712 — pandas boolean mask
    if not flagged.empty:
        print("\nFLAGGED (energy-only ratio outside band — need a cause "
              "hypothesis: data gap vs model miss):")
        for _, r in flagged.iterrows():
            print(f"  {r['province']} {r['asset_type']} {r['duration_h']}h "
                  f"ratio_energy={r['ratio_energy']:.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
