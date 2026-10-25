# services/option_engine/run_option_batch.py
"""Thin CLI for the option-value batch runner (spec §5.8).

Single cell:
    python services/option_engine/run_option_batch.py \
        --province 山东 --asset-type bess --duration-h 2 --paths 1000

Full screen (29 provinces × bess 2h/4h/wind/solar, fast mode 200 paths):
    python services/option_engine/run_option_batch.py --screen

Engine from PGURL (fallback DB_DSN) — `set -a; source config/.env; set +a`
first. Scheduling deferred (manual/cron now; nodal-writer terraform pattern
when needed).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from sqlalchemy import create_engine  # noqa: E402

from services.option_engine import batch, snapshot  # noqa: E402


def _engine():
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise SystemExit(
            "PGURL (or DB_DSN) env var required — source config/.env first"
        )
    return create_engine(url, connect_args={"connect_timeout": 10})


def _print_row(row: dict) -> None:
    g = row["greeks_json"]
    mc = row["mc_params"]
    print(
        f"{row['province']} {row['asset_type']} {row['duration_h']}h "
        f"asof={row['asof_date']} "
        f"window=[{row['window_start']} → {row['window_end']}]"
    )
    print(
        f"  intrinsic {row['intrinsic_yuan_kw']:.2f} + extrinsic "
        f"{row['extrinsic_yuan_kw']:.2f} + policy "
        f"{row['total_yuan_kw'] - row['intrinsic_yuan_kw'] - row['extrinsic_yuan_kw']:.2f}"
        f" = total {row['total_yuan_kw']:.2f} ¥/kW | capture_adj "
        + (f"{row['capture_adj_yuan_kw']:.2f}"
           if row["capture_adj_yuan_kw"] is not None else "n/a")
    )
    print(f"  greeks: delta {g['delta']:.2f} | vega {g['vega']:.2f} | "
          f"theta {g['theta']:.2f}")
    print(f"  mc: seed={mc['seed']} paths={mc['n_paths']} "
          f"k={mc['n_components']} days={mc['n_days']} "
          f"clip=[{mc['clip_floor_yuan_mwh']:.1f}, "
          f"{mc['clip_cap_yuan_mwh']:.1f}] git={mc['git_sha']}")
    for note in mc.get("parametric_fallbacks") or []:
        print(f"  fallback: {note}")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Option-value batch runner")
    ap.add_argument("--province")
    ap.add_argument("--asset-type", default="bess",
                    choices=["bess", "wind", "solar"])
    ap.add_argument("--duration-h", type=float, default=2.0)
    ap.add_argument("--paths", type=int, default=None,
                    help="default 1000 (single) / 200 (--screen)")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--screen", action="store_true")
    ap.add_argument("--provinces", default=None,
                    help="comma-separated override for --screen")
    args = ap.parse_args(argv)

    eng = _engine()
    snapshot.ensure_table(eng)

    if args.screen:
        provs = (args.provinces.split(",") if args.provinces else None)
        rows = batch.run_screen(eng, provinces=provs,
                                n_paths=args.paths or 200, seed=args.seed)
        ok = [r for r in rows if "skipped" not in r]
        skipped = [r for r in rows if "skipped" in r]
        for r in ok:
            print(f"  OK   {r['province']} {r['asset_type']} "
                  f"{r['duration_h']}h total={r['total_yuan_kw']:.1f} ¥/kW")
        for r in skipped:
            print(f"  SKIP {r['skipped']}: {r['reason']}")
        print(f"screen done: {len(ok)} valued, {len(skipped)} skipped")
        return 0

    if not args.province:
        ap.error("--province required unless --screen")
    row = batch.run_one(eng, args.province, args.asset_type, args.duration_h,
                        n_paths=args.paths or 1000, seed=args.seed)
    if "skipped" in row:
        print(f"SKIPPED {row['skipped']}: {row['reason']}")
        return 1
    _print_row(row)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
