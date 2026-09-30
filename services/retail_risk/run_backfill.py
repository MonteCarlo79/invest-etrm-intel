# services/retail_risk/run_backfill.py
"""Retail-risk backfill orchestrator.

Usage:
  python services/retail_risk/run_backfill.py --root data/trading \
      --province 冀南 浙江 山东 安徽 [--kinds trades invoices mtm benchmarks] [--dry-run]

--dry-run parses everything and prints per-file/per-frame counts, writes nothing.
Live run requires the Task-1 migration applied (user-confirmed).
"""
from __future__ import annotations

import argparse
import datetime
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import pandas as pd

from services.retail_risk import loader, schemas
from services.retail_risk.parsers import (
    INVOICE_GLOBS, LOAD_PARSERS, TRADES_PARSERS,
    benchmark_infohub, mtm_workbook, trades_shandong,
)


def trades_to_volumes(trades: pd.DataFrame, load: pd.DataFrame | None = None) -> pd.DataFrame:
    """Hour-level trades -> VOLUMES_COLS. VWAP-merges same (date, hour, channel)
    rows (绿电 folds into its tenor channel). Load attaches nominated/settled.
    `estimated` is True when any contributing row was spread from daily data
    (source_term marked ' (est)', spec D5)."""
    if trades.empty:
        return pd.DataFrame(columns=schemas.VOLUMES_COLS)
    df = trades.copy()
    df["pv"] = df["volume_mwh"] * df["price_cny_mwh"].fillna(0)
    df["est"] = df["source_term"].fillna("").str.contains(r"\(est\)")
    g = df.groupby(["delivery_date", "hour", "channel"], as_index=False)
    out = g.agg(volume_mwh=("volume_mwh", "sum"), pv=("pv", "sum"), estimated=("est", "max"))
    out["vwap_cny_mwh"] = (out["pv"] / out["volume_mwh"].replace(0, float("nan"))).round(4)
    out["nominated_mwh"] = None
    out["settled_mwh"] = None
    if load is not None and not load.empty:
        out = out.merge(load, on=["delivery_date", "hour"], how="left",
                        suffixes=("", "_ld"))
        for c in ("nominated_mwh", "settled_mwh"):
            out[c] = out[c + "_ld"].combine_first(out[c])
            out = out.drop(columns=[c + "_ld"])
    return out.drop(columns=["pv"])[schemas.VOLUMES_COLS]


def _shandong_shapes(root: Path) -> pd.DataFrame:
    """Day-level actual hourly shapes from 日用电曲线 (not the MTM workbook —
    its 分月分时比例 sheet is a per-customer pivot, not a province shape)."""
    return trades_shandong.day_shapes_from_curves(root)


def run(root, provinces, kinds, dry_run: bool, echo=print) -> dict:
    root = Path(root)
    summary: dict = {}
    engine = None if dry_run else loader.get_engine()

    for province in provinces:
        summary[province] = {}
        if "trades" in kinds and province in TRADES_PARSERS:
            if province == "山东":
                trades = trades_shandong.parse_shandong_trades(root, _shandong_shapes(root))
            else:
                trades = TRADES_PARSERS[province](root)
            load = LOAD_PARSERS[province](root) if province in LOAD_PARSERS else None
            volumes = trades_to_volumes(trades, load)
            summary[province]["trades_rows"] = len(trades)
            summary[province]["volumes_rows"] = len(volumes)
            echo(f"[{province}] trades={len(trades)} volumes={len(volumes)}")
            if not dry_run:
                with engine.begin() as conn:
                    bid = loader.get_or_create_book(conn, province)
                    ym = datetime.date.today().strftime("%Y%m")
                    n1 = loader.write_trades(conn, bid, trades,
                                             f"{province}_{ym}_trades", province)
                    n2 = loader.write_volumes(conn, bid, volumes,
                                              f"{province}_{ym}_volumes")
                    echo(f"[{province}] wrote positions={n1} volumes={n2} (book {bid})")

        if "invoices" in kinds and province in INVOICE_GLOBS:
            pattern, parser = INVOICE_GLOBS[province]
            files = sorted(root.glob(pattern))
            echo(f"[{province}] {len(files)} invoice files")
            n_new = 0
            for f in files:
                try:
                    doc = parser(f)
                except Exception as e:                       # noqa: BLE001
                    echo(f"  !! {f.name}: {e}")
                    continue
                if dry_run:
                    echo(f"  {f.name}: items={len(doc.items)} total={doc.total_amount_cny}")
                    continue
                with engine.begin() as conn:
                    bid = loader.get_or_create_book(conn, province)
                    sid = loader.write_invoice(conn, bid, doc, f.name,
                                               loader.file_sha256(str(f)))
                    n_new += 1 if sid else 0
            summary[province]["invoices_new"] = n_new

    if "mtm" in kinds:
        n_curves = n_contracts = 0
        for f in mtm_workbook.mtm_files(root):
            out = mtm_workbook.parse_mtm_workbook(f)
            echo(f"[mtm] {f.name}: curves={len(out['curves'])} contracts={len(out['contracts'])}")
            if not dry_run:
                with engine.begin() as conn:
                    n_curves += loader.write_curves(conn, out["curves"])
                    if not out["contracts"].empty:
                        _, nc = loader.write_contracts(conn, out["province"], out["contracts"])
                        n_contracts += nc
        summary["mtm"] = {"curves": n_curves, "contracts": n_contracts}

    if "benchmarks" in kinds:
        n = 0
        for f in benchmark_infohub.infohub_files(root):
            df = benchmark_infohub.parse_infohub_benchmarks(f)
            echo(f"[bench] {f.name}: rows={len(df)}")
            if not dry_run and not df.empty:
                with engine.begin() as conn:
                    n += loader.write_benchmarks(conn, df)
        summary["benchmarks"] = n

    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default="data/trading")
    ap.add_argument("--province", nargs="+",
                    default=["冀南", "浙江", "山东", "安徽"])
    ap.add_argument("--kinds", nargs="+",
                    default=["trades", "invoices", "mtm", "benchmarks"])
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    run(args.root, args.province, args.kinds, args.dry_run)


if __name__ == "__main__":
    main()
