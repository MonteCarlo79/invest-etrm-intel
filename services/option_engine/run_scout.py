# services/option_engine/run_scout.py
"""Thin CLI for the generative investment scout (spec §5.9, Task 6).

    python services/option_engine/run_scout.py --sweep all [--provinces 山东,蒙西]

Chains deterministic scans → delta-gated LLM thesis drafting → realized-
monetization validation → opportunity cards in
marketdata.option_opportunity_cards (reviewed in the deal-structurer
价值筛选 queue, Task 7).

Engine from PGURL (fallback DB_DSN) — `set -a; source config/.env; set +a`
first. The LLM client is built here (and only when is_llm_available());
run_sweep itself never builds one — with no client, cards save without
theses (scan_params note "llm_unavailable") and the queue still works.
Exit 1 when zero scans ran (loud failure, nodal-writer pattern).
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def _engine():
    url = os.environ.get("PGURL") or os.environ.get("DB_DSN")
    if not url:
        raise SystemExit(
            "PGURL (or DB_DSN) env var required — source config/.env first"
        )
    from sqlalchemy import create_engine
    return create_engine(url, connect_args={"connect_timeout": 10})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Generative investment scout")
    ap.add_argument("--sweep", choices=["investment", "trading", "all"],
                    default="all")
    ap.add_argument("--provinces", default=None,
                    help="comma-separated, e.g. 山东,蒙西 (trading scans "
                         "only; investment scans are nation-wide)")
    args = ap.parse_args(argv)
    provinces = ([p.strip() for p in args.provinces.split(",") if p.strip()]
                 if args.provinces else None)

    from services.option_engine.scout import scout
    from shared.anthropic_client import is_llm_available, make_client

    client = None
    if is_llm_available():
        client = make_client()
    else:
        print("LLM unavailable (no ANTHROPIC_API_KEY / BEDROCK_REGION) — "
              "cards save without theses (llm_unavailable)", flush=True)

    eng = _engine()
    sweeps = list(scout.SWEEPS) if args.sweep == "all" else [args.sweep]
    scans_ran = 0
    for sw in sweeps:
        out = scout.run_sweep(eng, sw, client=client, provinces=provinces)
        scans_ran += out["scans_ran"]
        print(f"[{sw}] done: saved={out['saved']} carried={out['carried']} "
              f"skipped={out['skipped']} scans_ran={out['scans_ran']}",
              flush=True)
        for err in out["errors"]:
            print(f"[{sw}] ERROR {err}", flush=True)

    # A run where every scan raised produced nothing — fail loudly (nodal
    # daily_job pattern) rather than silently "succeed" with an empty queue.
    if scans_ran == 0:
        print("FATAL: zero scans ran", flush=True)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
