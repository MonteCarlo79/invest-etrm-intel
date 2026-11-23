import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from services.ph_knowledge import run_jobs


def _run(results, argv):
    with patch.dict(os.environ, {"PGURL": "postgresql://x"}), \
         patch.dict(sys.modules, {"psycopg2": MagicMock()}), \
         patch("services.ph_knowledge.wesm_scraper.run_wesm_price_scrape", return_value=results) as m:
        return run_jobs.main(argv), m


def test_prices_ok_passes_window_and_overwrite():
    rc, m = _run({"2026-11-22": 72, "2026-11-21": 0}, ["--only", "prices", "--days-back", "120", "--overwrite"])
    assert rc == 0
    assert m.call_args.kwargs == {"days_back": 120, "overwrite": True}


def test_store_failure_exits_nonzero():
    rc, _ = _run({"2026-11-22": -1}, ["--only", "prices"])
    assert rc == 1


def test_step_exception_exits_nonzero():
    with patch.object(run_jobs, "_kb_ingest", side_effect=RuntimeError("boom")):
        assert run_jobs.main(["--only", "kb"]) == 1
