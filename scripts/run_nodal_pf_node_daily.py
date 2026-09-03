"""CLI wrapper for services.bess_map.nodal_pf_daily (kept for local runs).

    python scripts/run_nodal_pf_node_daily.py [--date D] [--start S --end E] [--province P] [--dry-run]
"""
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from services.bess_map.nodal_pf_daily import main

if __name__ == "__main__":
    main()
