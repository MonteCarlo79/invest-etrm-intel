import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from services.po_knowledge import run_jobs


def test_only_selected_steps_run_and_window_passes_through():
    with patch.object(run_jobs, "_prices") as p, patch.object(run_jobs, "_digest") as d:
        assert run_jobs.main(["--only", "prices", "--days-back", "30"]) == 0
    p.assert_called_once_with(30)
    d.assert_not_called()


def test_failed_step_exits_nonzero_but_later_steps_still_run():
    with patch.object(run_jobs, "_prices", side_effect=RuntimeError("boom")), \
         patch.object(run_jobs, "_digest") as d:
        assert run_jobs.main(["--only", "prices", "--only", "digest"]) == 1
    d.assert_called_once()


def test_price_store_failure_raises():
    with patch.object(run_jobs, "_connect", return_value=MagicMock()), \
         patch("services.po_knowledge.entso_scraper.run_entso_price_scrape", return_value={"2026-11-22": -1}):
        try:
            run_jobs._prices(7)
        except RuntimeError:
            return
    raise AssertionError("expected RuntimeError")
