"""scheduler_service --once: run every daily job once, in order, then return
(one-shot ECS task; the default loop never exits and piled up 40 tasks)."""
import logging
import pathlib
import sys

from services.intl_market_common import scheduler_service as ss


def test_run_once_runs_all_jobs_in_order_and_returns(monkeypatch):
    calls = []
    names = ["_daily_market_job", "_daily_knowledge_job", "_kb_digest_job", "_modo_ai_job", "_daily_report_job"]

    def mk(n):
        def f():
            calls.append(n)
        f.__name__ = n
        return f

    monkeypatch.setattr(ss, "_make_jobs", lambda *a: tuple(mk(n) for n in names))
    ss.run_once("au", "", pathlib.Path("apps/au-market"), logging.getLogger("t"), max_seconds=30)
    assert calls == names


def test_main_once_does_not_start_the_scheduler_loop(monkeypatch):
    ran = []
    monkeypatch.setattr(ss, "run_once", lambda *a, **k: ran.append(a[0]))
    monkeypatch.setattr(ss, "start_scheduler", lambda *a, **k: (_ for _ in ()).throw(AssertionError("loop started")))
    monkeypatch.setattr(sys, "argv", ["x", "--code", "pjm", "--app-dir", "apps/pjm-market", "--once"])
    ss.main()
    assert ran == ["pjm"]
